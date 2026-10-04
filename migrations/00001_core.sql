-- The ledger's core: partners, accounts, balances, transactions and postings.
-- Run as the owner role (migrations); the app connects as a login role that is a member of ledger_writer.

-- +goose Up

-- ledger_writer holds the app's grants. It can't log in: deployments (and tests) create the actual login role
-- "IN ROLE ledger_writer" with its own secret, so no password ever lives in a migration.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ledger_writer') THEN
        CREATE ROLE ledger_writer NOLOGIN;
    END IF;
END
$$;
-- +goose StatementEnd

CREATE TABLE partners (
    id                  uuid PRIMARY KEY,
    name                text        NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    api_key_hash        bytea       NOT NULL UNIQUE,                 -- SHA-256 of the key; the key itself is never stored
    rate_limit_per_min  integer     NOT NULL DEFAULT 600 CHECK (rate_limit_per_min > 0),
    funding_limit_minor bigint      NOT NULL CHECK (funding_limit_minor >= 0),
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
    id                uuid PRIMARY KEY,
    partner_id        uuid        NOT NULL REFERENCES partners (id),
    kind              text        NOT NULL CHECK (kind IN ('customer', 'settlement')),
    customer_ref      text        CHECK (length(customer_ref) <= 255),
    currency          char(3)     NOT NULL,
    status            text        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'closed')),
    -- The floor for "available": 0 for customers, minus the funding limit for a settlement account, which lets the platform pre-fund payouts up to an agreed limit.
    min_balance_minor bigint      NOT NULL DEFAULT 0 CHECK (min_balance_minor <= 0),
    created_at        timestamptz NOT NULL DEFAULT now(),
    CHECK (kind = 'settlement' OR min_balance_minor = 0)
);
CREATE INDEX accounts_partner ON accounts (partner_id);
CREATE UNIQUE INDEX accounts_one_settlement_per_currency ON accounts (partner_id, currency) WHERE kind = 'settlement';

-- Materialised and derived: posted = the sum of the account's postings (reconciliation proves it).
-- Mutable on purpose; every change bumps version, which orders the account's events (account_seq).
CREATE TABLE balances (
    account_id   uuid   PRIMARY KEY REFERENCES accounts (id),
    posted_minor bigint NOT NULL DEFAULT 0,
    held_minor   bigint NOT NULL DEFAULT 0 CHECK (held_minor >= 0),
    version      bigint NOT NULL DEFAULT 0
);

-- Invariant 3 in the database too: a balance write may not leave available (posted − held) below the account's floor
-- AND lower than it was. The store checks first and refuses with insufficient_funds; this catches a bug in that
-- check, failing the transaction instead of committing an overdraft. A write that improves available is always
-- allowed: if a floor is raised above an account's current position (a lowered funding limit), the transfers that
-- bring it back must still go through, or the account would be stuck. min_balance_minor is a primary-key lookup.
-- +goose StatementBegin
CREATE FUNCTION check_balance_floor() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    old_available bigint := 0;  -- an inserted balance starts from nothing
BEGIN
    IF TG_OP = 'UPDATE' THEN
        old_available := OLD.posted_minor - OLD.held_minor;
    END IF;
    IF NEW.posted_minor - NEW.held_minor < (SELECT min_balance_minor FROM accounts WHERE id = NEW.account_id)
       AND NEW.posted_minor - NEW.held_minor < old_available THEN
        RAISE EXCEPTION 'account % would go below its floor', NEW.account_id USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
CREATE TRIGGER balances_floor BEFORE INSERT OR UPDATE ON balances FOR EACH ROW EXECUTE FUNCTION check_balance_floor();

CREATE TABLE transactions (
    id         uuid PRIMARY KEY,
    partner_id uuid        NOT NULL REFERENCES partners (id),
    kind       text        NOT NULL CHECK (kind IN ('transfer', 'hold_capture')),
    reference  text        CHECK (length(reference) <= 255),
    status     text        NOT NULL DEFAULT 'posted' CHECK (status = 'posted'),  -- inserted as posted, never changed
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE postings (
    id             uuid PRIMARY KEY,
    transaction_id uuid        NOT NULL REFERENCES transactions (id),
    account_id     uuid        NOT NULL REFERENCES accounts (id),
    amount_minor   bigint      NOT NULL CHECK (amount_minor <> 0),  -- signed: credit +, debit −
    currency       char(3)     NOT NULL,
    -- The account's balance version this entry produced: the account's own sequence, taken under its row lock,
    -- and the same number its event carries. Statements are ordered by it. Unique per account, so two writers
    -- that both think they produced version n (a lost update) fail instead of corrupting the order.
    account_seq    bigint      NOT NULL CHECK (account_seq > 0),
    created_at     timestamptz NOT NULL DEFAULT now(),  -- for display; never used for ordering
    UNIQUE (account_id, account_seq)                    -- also the statement's keyset index
);
CREATE INDEX postings_transaction ON postings (transaction_id);

-- Invariant 1 in the database itself: at COMMIT, each transaction's postings sum to zero per currency.
-- A deferred constraint trigger runs at commit, after all of the transaction's postings are inserted.
-- +goose StatementBegin
CREATE FUNCTION check_postings_balance() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM postings
        WHERE transaction_id = NEW.transaction_id
        GROUP BY currency
        HAVING sum(amount_minor) <> 0
    ) THEN
        RAISE EXCEPTION 'transaction % does not balance: postings must sum to zero per currency', NEW.transaction_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER postings_balance AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_postings_balance();

-- Append-only (invariant 8), enforced twice: the app role has no UPDATE/DELETE grant on these tables, and these
-- triggers stop even the owner. TRUNCATE needs its own statement-level trigger (row triggers don't fire on it).
-- A correction is a new, reversing transaction.
-- +goose StatementBegin
CREATE FUNCTION forbid_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % is not allowed (write a reversing transaction instead)', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'restrict_violation';
END
$$;
-- +goose StatementEnd
CREATE TRIGGER transactions_append_only BEFORE UPDATE OR DELETE ON transactions FOR EACH ROW EXECUTE FUNCTION forbid_change();
CREATE TRIGGER transactions_no_truncate BEFORE TRUNCATE ON transactions FOR EACH STATEMENT EXECUTE FUNCTION forbid_change();
CREATE TRIGGER postings_append_only BEFORE UPDATE OR DELETE ON postings FOR EACH ROW EXECUTE FUNCTION forbid_change();
CREATE TRIGGER postings_no_truncate BEFORE TRUNCATE ON postings FOR EACH STATEMENT EXECUTE FUNCTION forbid_change();

GRANT USAGE ON SCHEMA public TO ledger_writer;
GRANT SELECT ON partners TO ledger_writer;                              -- partners are created by ledgerctl as owner
GRANT SELECT, INSERT ON accounts TO ledger_writer;
GRANT UPDATE (status) ON accounts TO ledger_writer;                     -- closing; nothing else about an account changes
GRANT SELECT, INSERT, UPDATE ON balances TO ledger_writer;
GRANT SELECT, INSERT ON transactions, postings TO ledger_writer;        -- append-only: no UPDATE, no DELETE

-- +goose Down
DROP TABLE postings;
DROP TABLE transactions;
DROP TABLE balances;
DROP TABLE accounts;
DROP TABLE partners;
DROP FUNCTION forbid_change;
DROP FUNCTION check_postings_balance;
DROP FUNCTION check_balance_floor;
