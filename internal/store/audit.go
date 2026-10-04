package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/store/db"
)

// AuditEntry is one row of the audit log.
type AuditEntry struct {
	PartnerID *uuid.UUID // nil when authentication failed
	Actor     string
	Action    string
	Resource  string
	Status    int
	RequestID string
}

func (e AuditEntry) params(id uuid.UUID) db.InsertAuditParams {
	return db.InsertAuditParams{
		ID: id, PartnerID: e.PartnerID, Actor: e.Actor, Action: e.Action, Resource: e.Resource,
		Status: int32(e.Status), RequestID: e.RequestID, //nolint:gosec // an HTTP status fits
	}
}

// Audit records an entry in the same transaction as the write it describes, so the two commit together.
func (tx Tx) Audit(ctx context.Context, e AuditEntry) error {
	if err := tx.q.InsertAudit(ctx, e.params(tx.s.newID())); err != nil {
		return fmt.Errorf("audit: %w", err)
	}
	return nil
}

// Audit records an entry on its own, for outcomes with no ledger transaction (an auth failure, a refused or
// replayed request).
func (s *Store) Audit(ctx context.Context, e AuditEntry) error {
	if err := db.New(s.pool).InsertAudit(ctx, e.params(s.newID())); err != nil {
		return fmt.Errorf("audit: %w", err)
	}
	return nil
}
