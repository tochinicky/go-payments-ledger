-- The transactional outbox: one event per account entry, written in the same transaction as the entry itself, so
-- an event exists if and only if its change committed (invariant 6). The relay publishes them to Kafka.

-- +goose Up
CREATE TABLE outbox (
    id           uuid PRIMARY KEY,                     -- the event_id consumers deduplicate on
    -- Publish order. Drawn when the row is inserted, which is always after the account's balance update in the
    -- same transaction, so while its row lock is held: per account, seq order is commit order.
    seq          bigint      GENERATED ALWAYS AS IDENTITY UNIQUE,
    partner_id   uuid        NOT NULL REFERENCES partners (id),
    account_id   uuid        NOT NULL REFERENCES accounts (id),
    account_seq  bigint      NOT NULL,                 -- the account's balance version after this entry
    event_type   text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    UNIQUE (account_id, account_seq)                   -- one event per account entry, never two
);
CREATE INDEX outbox_unpublished ON outbox (seq) WHERE published_at IS NULL;  -- the relay's queue

GRANT SELECT, INSERT ON outbox TO ledger_writer;
GRANT UPDATE (published_at) ON outbox TO ledger_writer;   -- the relay marks rows published, nothing else

-- +goose Down
DROP TABLE outbox;
