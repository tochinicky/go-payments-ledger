-- Readiness: the app reports ready only once the schema has reached the version its code expects, so it needs to
-- read goose's version table (and nothing else of it).

-- +goose Up
GRANT SELECT ON goose_db_version TO ledger_writer;

-- +goose Down
REVOKE SELECT ON goose_db_version FROM ledger_writer;
