-- The audit log: one row per write attempt and per authentication failure, append-only like the ledger itself.
-- No secrets: an unknown API key is recorded only as a short fingerprint of its hash.

-- +goose Up
CREATE TABLE audit_log (
    id          uuid PRIMARY KEY,
    partner_id  uuid        REFERENCES partners (id),   -- null when authentication failed
    actor       text        NOT NULL,                    -- "partner:<id>", or "key:<hash fingerprint>" / "anonymous"
    action      text        NOT NULL,                    -- the route, e.g. "POST /v1/transfers"
    resource    text        NOT NULL,                    -- the request path
    status      integer     NOT NULL,                    -- the HTTP status answered
    request_id  text        NOT NULL,
    at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX audit_log_partner ON audit_log (partner_id, at);

CREATE TRIGGER audit_log_append_only BEFORE UPDATE OR DELETE ON audit_log FOR EACH ROW EXECUTE FUNCTION forbid_change();
CREATE TRIGGER audit_log_no_truncate BEFORE TRUNCATE ON audit_log FOR EACH STATEMENT EXECUTE FUNCTION forbid_change();

GRANT SELECT, INSERT ON audit_log TO ledger_writer;   -- append-only: no UPDATE, no DELETE

-- +goose Down
DROP TABLE audit_log;
