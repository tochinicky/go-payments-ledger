-- name: InsertInbox :execrows
-- 0 rows: this consumer already processed the event (a redelivery), so skip it.
INSERT INTO notifier.inbox (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING;

-- name: LastSeq :one
-- The highest account_seq recorded for the account, locked for this transaction (0 rows for a new account).
SELECT last_seq FROM notifier.account_progress WHERE account_id = $1 FOR UPDATE;

-- name: SetLastSeq :exec
INSERT INTO notifier.account_progress (account_id, last_seq) VALUES ($1, $2)
ON CONFLICT (account_id) DO UPDATE SET last_seq = greatest(notifier.account_progress.last_seq, excluded.last_seq);

-- name: InsertNotification :exec
INSERT INTO notifier.notifications (id, event_id, partner_id, account_id, kind, amount_minor, account_seq)
VALUES ($1, $2, $3, $4, $5, $6, $7);
