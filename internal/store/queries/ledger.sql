-- name: InsertPartner :exec
INSERT INTO partners (id, name, api_key_hash, rate_limit_per_min, funding_limit_minor)
VALUES ($1, $2, $3, $4, $5);

-- name: PartnerByKeyHash :one
SELECT id, name, api_key_hash, rate_limit_per_min, funding_limit_minor FROM partners WHERE api_key_hash = $1;

-- name: InsertAccount :exec
INSERT INTO accounts (id, partner_id, kind, customer_ref, currency, min_balance_minor)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: InsertBalance :exec
INSERT INTO balances (account_id) VALUES ($1);

-- name: AccountWithBalance :one
-- Scoped to the partner: another partner's account is simply not found (invariant 7).
SELECT a.id, a.partner_id, a.kind, a.customer_ref, a.currency, a.status, a.min_balance_minor, a.created_at,
       b.posted_minor, b.held_minor, b.version
FROM accounts a JOIN balances b ON b.account_id = a.id
WHERE a.id = $1 AND a.partner_id = $2;

-- name: LockAccounts :many
-- Locks the balance rows of every account in a money movement, ALWAYS in account-id order. Two transfers between
-- the same pair of accounts in opposite directions then take their locks in the same order, so neither can hold
-- one lock while waiting for the other: no deadlock. (The sort happens before the locking step.)
-- Scoped to the partner, so a request can never lock (and so slow down) another partner's account.
SELECT a.id, a.partner_id, a.kind, a.currency, a.status, a.min_balance_minor,
       b.posted_minor, b.held_minor, b.version
FROM accounts a JOIN balances b ON b.account_id = a.id
WHERE a.id = ANY(sqlc.arg(ids)::uuid[]) AND a.partner_id = sqlc.arg(partner_id)
ORDER BY a.id
FOR UPDATE OF b;

-- name: AddToPosted :one
UPDATE balances SET posted_minor = posted_minor + sqlc.arg(delta), version = version + 1
WHERE account_id = $1
RETURNING posted_minor, held_minor, version;

-- name: InsertTransaction :one
INSERT INTO transactions (id, partner_id, kind, reference) VALUES ($1, $2, $3, $4)
RETURNING created_at;

-- name: InsertPosting :exec
-- created_at is clock_timestamp(), not now(): now() is when the transaction began, before it waited for the account
-- lock. Taken after the lock, an account's posting times follow its commit order, so a statement page already read
-- can never gain an earlier row later (which keyset pagination would skip).
INSERT INTO postings (id, transaction_id, account_id, amount_minor, currency, created_at)
VALUES ($1, $2, $3, $4, $5, clock_timestamp());

-- name: CloseAccount :exec
UPDATE accounts SET status = 'closed' WHERE id = $1;

-- name: Statement :many
-- Keyset pagination: "the next page after (created_at, id)". Unlike OFFSET, it costs the same on page 1000 as on
-- page 1, and rows added meanwhile can't shift a page.
SELECT p.id, p.transaction_id, p.amount_minor, p.currency, p.created_at, t.kind, t.reference
FROM postings p JOIN transactions t ON t.id = p.transaction_id
WHERE p.account_id = sqlc.arg(account_id)
  AND (p.created_at, p.id) > (sqlc.arg(after_created_at)::timestamptz, sqlc.arg(after_id)::uuid)
ORDER BY p.created_at, p.id
LIMIT sqlc.arg(page_size);
