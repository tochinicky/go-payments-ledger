-- name: Unpublished :many
-- The next batch in publish order. No high-water mark: a lower seq on another account can commit after a higher
-- one, and re-selecting from the start of the queue each cycle picks it up.
SELECT id, seq, account_id, payload FROM outbox WHERE published_at IS NULL ORDER BY seq LIMIT sqlc.arg(batch_size);

-- name: MarkPublished :exec
UPDATE outbox SET published_at = now() WHERE id = ANY(sqlc.arg(ids)::uuid[]) AND published_at IS NULL;
