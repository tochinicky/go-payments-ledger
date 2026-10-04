-- name: InsertHold :one
-- expires_at is computed on the database clock, like the idempotency lease.
INSERT INTO holds (id, partner_id, account_id, to_account_id, amount_minor, currency, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, now() + make_interval(secs => sqlc.arg(expires_in_seconds)::float8))
RETURNING *;

-- name: Hold :one
-- Scoped to the partner: another partner's hold is not found.
SELECT * FROM holds WHERE id = $1 AND partner_id = $2;

-- name: LockHold :one
-- Locks the hold first, then (separately, in account-id order) its accounts' balances: every path that ends a hold
-- takes the hold row before any balance row, and balance rows always in id order, so no two paths can deadlock.
-- expired says whether its time is up on the database clock.
SELECT h.*, (h.expires_at <= now())::bool AS expired FROM holds h WHERE h.id = $1 AND h.partner_id = $2 FOR UPDATE;

-- name: EndHold :one
UPDATE holds SET status = $2, captured_minor = $3, transaction_id = $4, ended_at = now()
WHERE id = $1
RETURNING *;

-- name: DueHolds :many
-- A batch of holds whose time is up. No lock here: each is then expired in its own short transaction.
SELECT id FROM holds
WHERE status = 'active' AND expires_at <= now()
ORDER BY expires_at
LIMIT sqlc.arg(batch_size);

-- name: LockDueHold :one
-- Locks a hold for expiry, re-checking it is still active and due. SKIP LOCKED: a hold a request (or another
-- replica) is ending right now is left to it.
SELECT * FROM holds WHERE id = $1 AND status = 'active' AND expires_at <= now() FOR UPDATE SKIP LOCKED;

-- name: LockBalanceNoWait :one
-- The expiry job never waits for a busy account: if a transfer holds the balance row, the hold is expired on the
-- next run instead (no client can capture it meanwhile, since it is past its time).
SELECT account_id FROM balances WHERE account_id = $1 FOR UPDATE SKIP LOCKED;
