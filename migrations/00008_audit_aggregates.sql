-- Abuse is audited in aggregate: a flood of bad keys or rate-limited requests becomes one row per key per minute
-- with a count and its time span, not one row per request (which would make the protection a write amplifier).

-- +goose Up
ALTER TABLE audit_log ADD COLUMN count integer NOT NULL DEFAULT 1 CHECK (count > 0);
ALTER TABLE audit_log ADD COLUMN first_at timestamptz;   -- set on aggregated rows: the span is first_at…at

-- +goose Down
ALTER TABLE audit_log DROP COLUMN first_at;
ALTER TABLE audit_log DROP COLUMN count;
