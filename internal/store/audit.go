package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

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

// insertAudit is db.InsertAudit's statement, queued rather than run (see Tx.deferExec).
const insertAudit = `INSERT INTO audit_log (id, partner_id, actor, action, resource, status, request_id) VALUES ($1, $2, $3, $4, $5, $6, $7)`

// Audit records an entry in the same transaction as the write it describes, so the two commit together. It is
// sent with the transaction's other deferred statements, just before COMMIT.
func (tx Tx) Audit(_ context.Context, e AuditEntry) error {
	p := e.params(tx.s.newID())
	tx.deferExec(insertAudit, p.ID, p.PartnerID, p.Actor, p.Action, p.Resource, p.Status, p.RequestID)
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

// AuditAggregate is many identical outcomes (failed authentications with one key, rate-limited requests of one
// partner) recorded as one row: how many, and between when.
type AuditAggregate struct {
	AuditEntry
	Count   int
	FirstAt time.Time
	LastAt  time.Time
}

// AuditAggregates records aggregated entries in one transaction.
func (s *Store) AuditAggregates(ctx context.Context, entries []AuditAggregate) error {
	return s.inTx(ctx, func(tx Tx) error {
		for _, e := range entries {
			if err := tx.q.InsertAuditAggregate(ctx, db.InsertAuditAggregateParams{
				ID: tx.s.newID(), PartnerID: e.PartnerID, Actor: e.Actor, Action: e.Action, Resource: e.Resource,
				Status: int32(e.Status), RequestID: e.RequestID, Count: int32(e.Count), //nolint:gosec // small values
				FirstAt: pgtype.Timestamptz{Time: e.FirstAt, Valid: true}, At: pgtype.Timestamptz{Time: e.LastAt, Valid: true},
			}); err != nil {
				return fmt.Errorf("audit aggregate: %w", err)
			}
		}
		return nil
	})
}
