-- name: InsertAudit :exec
INSERT INTO audit_log (id, partner_id, actor, action, resource, status, request_id) VALUES ($1, $2, $3, $4, $5, $6, $7);
