-- name: ClaimKey :execrows
-- The first request with a key claims it. ON CONFLICT DO NOTHING: if the key exists, 0 rows, and the caller reads it.
INSERT INTO idempotency (partner_id, key, request_hash, status, lease_token, locked_until)
VALUES ($1, $2, $3, 'in_progress', $4, now() + make_interval(secs => sqlc.arg(lease_seconds)::float8))
ON CONFLICT DO NOTHING;

-- name: KeyState :one
-- A plain read (no lock): it never waits behind the attempt that holds the row.
SELECT request_hash, status, response_code, response_body, coalesce(locked_until > now(), false)::bool AS live
FROM idempotency WHERE partner_id = $1 AND key = $2;

-- name: TakeOverKey :execrows
-- A new attempt takes over a claim whose lease has lapsed. If the lapsed attempt is still inside its ledger
-- transaction, this waits on the row lock it holds, then re-checks: if that attempt completed, 0 rows.
UPDATE idempotency SET lease_token = $3, locked_until = now() + make_interval(secs => sqlc.arg(lease_seconds)::float8)
WHERE partner_id = $1 AND key = $2 AND status = 'in_progress' AND locked_until <= now();

-- name: FenceKey :one
-- The first statement of the ledger transaction: lock our own claim. No row means another attempt took over, so
-- stop before touching money. Holding the lock until commit makes any takeover wait for us.
SELECT 1::int FROM idempotency
WHERE partner_id = $1 AND key = $2 AND lease_token = $3 AND status = 'in_progress'
FOR UPDATE;

-- name: CompleteKey :exec
UPDATE idempotency
SET status = 'completed', response_code = $4, response_body = $5, completed_at = now(), lease_token = NULL, locked_until = NULL
WHERE partner_id = $1 AND key = $2 AND lease_token = $3;

-- name: ReleaseKey :execrows
-- Frees a key after a failure, so a retry can run. Conditional: only our own claim, only while in progress. If the
-- failure was a lost COMMIT acknowledgement, the row is already completed and stays; the retry gets the stored answer.
DELETE FROM idempotency WHERE partner_id = $1 AND key = $2 AND lease_token = $3 AND status = 'in_progress';

-- name: CleanupKeys :execrows
-- One batch of expired keys: completed longer ago than the retention, or abandoned in progress that long.
-- SKIP LOCKED: several replicas can run this at once without waiting on each other or on live requests.
DELETE FROM idempotency
WHERE (partner_id, key) IN (
    SELECT i.partner_id, i.key FROM idempotency i
    WHERE (i.status = 'completed' AND i.completed_at < now() - make_interval(secs => sqlc.arg(retention_seconds)::float8))
       OR (i.status = 'in_progress' AND i.locked_until < now() - make_interval(secs => sqlc.arg(retention_seconds)::float8))
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE SKIP LOCKED
);
