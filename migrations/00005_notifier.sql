-- The notifier: a sample downstream service consuming ledger events. Its tables live in their own schema with their
-- own role, which has no access to the ledger's tables (in production it would be a separate database).

-- +goose Up
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'notifier_writer') THEN
        CREATE ROLE notifier_writer NOLOGIN;
    END IF;
END
$$;
-- +goose StatementEnd

CREATE SCHEMA notifier;

-- Events already processed, per consumer. Processing an event and inserting its inbox row are one transaction,
-- so a redelivered event finds its row and is skipped.
CREATE TABLE notifier.inbox (
    consumer     text        NOT NULL,
    event_id     uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- The highest account_seq seen per account, to detect gaps and regressions in the stream.
CREATE TABLE notifier.account_progress (
    account_id uuid   PRIMARY KEY,
    last_seq   bigint NOT NULL
);

-- What the notifier "sends": one row per event, never two.
CREATE TABLE notifier.notifications (
    id           uuid        PRIMARY KEY,
    event_id     uuid        NOT NULL UNIQUE,
    partner_id   uuid        NOT NULL,
    account_id   uuid        NOT NULL,
    kind         text        NOT NULL,
    amount_minor bigint      NOT NULL,
    account_seq  bigint      NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

GRANT USAGE ON SCHEMA notifier TO notifier_writer;
GRANT SELECT, INSERT ON notifier.inbox, notifier.notifications TO notifier_writer;
GRANT SELECT, INSERT, UPDATE ON notifier.account_progress TO notifier_writer;
REVOKE ALL ON SCHEMA public FROM notifier_writer;

-- +goose Down
DROP SCHEMA notifier CASCADE;
