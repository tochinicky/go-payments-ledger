-- name: InsertOutbox :exec
-- Called right after the account's balance update in the same transaction: seq is drawn under the account lock.
-- occurred_at comes from the database clock, like every other time the ledger records.
INSERT INTO outbox (id, partner_id, account_id, account_seq, event_type, payload)
VALUES ($1, $2, $3, $4, $5, sqlc.arg(payload)::jsonb || jsonb_build_object('occurred_at', clock_timestamp()));
