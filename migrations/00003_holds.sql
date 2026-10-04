-- Holds: money reserved on an account (like a card authorisation) for a destination fixed when it's placed.
-- A hold is active, then ends exactly once: captured, released or expired (invariant 4).

-- +goose Up
CREATE TABLE holds (
    id             uuid PRIMARY KEY,
    partner_id     uuid        NOT NULL REFERENCES partners (id),
    account_id     uuid        NOT NULL REFERENCES accounts (id),
    to_account_id  uuid        NOT NULL REFERENCES accounts (id),
    amount_minor   bigint      NOT NULL CHECK (amount_minor > 0),
    currency       char(3)     NOT NULL,
    status         text        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'captured', 'released', 'expired')),
    expires_at     timestamptz NOT NULL,
    captured_minor bigint      NOT NULL DEFAULT 0,
    transaction_id uuid        REFERENCES transactions (id),   -- the capture's transaction
    created_at     timestamptz NOT NULL DEFAULT now(),
    ended_at       timestamptz,
    CHECK (account_id <> to_account_id),
    CHECK (captured_minor BETWEEN 0 AND amount_minor),
    CHECK ((status = 'captured') = (transaction_id IS NOT NULL)),
    CHECK ((status = 'captured') = (captured_minor > 0)),
    CHECK ((status = 'active') = (ended_at IS NULL))
);
CREATE INDEX holds_partner ON holds (partner_id);
CREATE INDEX holds_expiry ON holds (expires_at) WHERE status = 'active';  -- the expiry job's batches

-- A hold may end only once, and only from active: the database refuses a second ending even if the app tried.
-- +goose StatementBegin
CREATE FUNCTION hold_ends_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status <> 'active' THEN
        RAISE EXCEPTION 'hold % already ended (%)', OLD.id, OLD.status USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
CREATE TRIGGER holds_end_once BEFORE UPDATE ON holds FOR EACH ROW EXECUTE FUNCTION hold_ends_once();

GRANT SELECT, INSERT ON holds TO ledger_writer;
GRANT UPDATE (status, captured_minor, transaction_id, ended_at) ON holds TO ledger_writer;  -- ending a hold, nothing else

-- +goose Down
DROP TABLE holds;
DROP FUNCTION hold_ends_once;
