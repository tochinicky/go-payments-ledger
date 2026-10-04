-- Idempotency keys: one row per (partner, key). A request claims its key (in_progress, with a lease), does the work,
-- and stores its response (completed) in the same transaction as the money movement, so "moved the money" and
-- "remembered the answer" commit together or not at all.

-- +goose Up
CREATE TABLE idempotency (
    partner_id    uuid        NOT NULL REFERENCES partners (id),
    key           text        NOT NULL CHECK (length(key) BETWEEN 1 AND 255),
    request_hash  bytea       NOT NULL,             -- SHA-256 of the canonical request: a reuse must match it
    status        text        NOT NULL CHECK (status IN ('in_progress', 'completed')),
    -- While in progress: whose attempt this is, and until when (database clock). A new attempt may take over only
    -- after locked_until, and the work fences on its own lease_token, so a lapsed attempt can't also post.
    lease_token   uuid,
    locked_until  timestamptz,
    response_code integer,
    response_body bytea,
    created_at    timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz,
    PRIMARY KEY (partner_id, key),
    CHECK (status <> 'in_progress' OR (lease_token IS NOT NULL AND locked_until IS NOT NULL)),
    CHECK (status <> 'completed' OR (response_code IS NOT NULL AND response_body IS NOT NULL AND completed_at IS NOT NULL))
);
-- For the cleanup loop: old completed keys, and abandoned in-progress ones.
CREATE INDEX idempotency_completed ON idempotency (completed_at) WHERE status = 'completed';
CREATE INDEX idempotency_abandoned ON idempotency (locked_until) WHERE status = 'in_progress';

-- DELETE: freeing a key after a failure (only one's own in-progress claim) and the cleanup loop.
GRANT SELECT, INSERT, UPDATE, DELETE ON idempotency TO ledger_writer;

-- +goose Down
DROP TABLE idempotency;
