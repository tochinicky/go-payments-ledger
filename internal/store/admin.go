package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/store/db"
)

// NewAPIKey returns a new random API key (256 bits, URL-safe) and the hash the ledger stores instead of it.
func NewAPIKey() (key string, hash []byte, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", nil, fmt.Errorf("api key: %w", err)
	}
	key = "lk_" + base64.RawURLEncoding.EncodeToString(b)
	sum := sha256.Sum256([]byte(key))
	return key, sum[:], nil
}

// RotatePartnerKey replaces a partner's API key hash. The old key stops working at once.
func (s *Store) RotatePartnerKey(ctx context.Context, partnerID uuid.UUID, hash []byte) error {
	n, err := db.New(s.pool).RotatePartnerKey(ctx, db.RotatePartnerKeyParams{ID: partnerID, ApiKeyHash: hash})
	if err != nil {
		return fmt.Errorf("rotate key: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("partner %s not found", partnerID)
	}
	return nil
}

// OutboxStatus is the relay's backlog.
type OutboxStatus struct {
	Total, Unpublished       int64
	OldestUnpublishedSeconds float64
}

// Outbox returns the outbox's backlog.
func (s *Store) Outbox(ctx context.Context) (OutboxStatus, error) {
	r, err := db.New(s.pool).OutboxStatus(ctx)
	if err != nil {
		return OutboxStatus{}, fmt.Errorf("outbox status: %w", err)
	}
	return OutboxStatus{Total: r.Total, Unpublished: r.Unpublished, OldestUnpublishedSeconds: r.OldestUnpublishedSeconds}, nil
}
