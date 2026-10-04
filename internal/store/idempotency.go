package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/tochinicky/go-payments-ledger/internal/store/db"
)

// LeaseDuration is how long a claimed key belongs to one attempt before another may take it over. It is well above
// the request timeout, so a live request never loses its lease; a crashed one frees the key within this time.
const LeaseDuration = 30 * time.Second

// ErrLeaseLost means another attempt took over this request's key (its lease had lapsed) before the work started.
// Nothing was done; the other attempt owns the outcome.
var ErrLeaseLost = errors.New("idempotency lease lost to another attempt")

// ClaimResult says what to do with a request, given its idempotency key.
type ClaimResult int

// The outcomes of claiming a key.
const (
	// Claimed: this attempt owns the key; do the work under Lease.
	Claimed ClaimResult = iota + 1
	// Replay: the key already completed; answer with the stored response, unchanged.
	Replay
	// InProgress: another attempt holds a live lease on the key.
	InProgress
	// KeyReused: the key was first used for a different request.
	KeyReused
)

// Lease is one attempt's claim on a key. Token fences the work: only the attempt holding it can complete the key.
type Lease struct {
	PartnerID uuid.UUID
	Key       string
	Token     uuid.UUID
}

// Response is a final outcome, stored with its key and replayed byte for byte.
type Response struct {
	Status int
	Body   []byte
}

// Claim is the result of ClaimKey: a lease for Claimed, the stored response for Replay.
type Claim struct {
	Result   ClaimResult
	Lease    Lease
	Response Response
}

// ClaimKey claims (partner, key) for a request whose canonical hash is hash. A new key is claimed at once. An
// existing key is answered from its row: a different hash is KeyReused, whatever its state; a completed key is
// replayed; a live lease is InProgress; a lapsed lease is taken over. The loop covers the races in between (a key
// freed or completed while we looked).
func (s *Store) ClaimKey(ctx context.Context, partnerID uuid.UUID, key string, hash []byte) (Claim, error) {
	q := db.New(s.pool)
	lease := Lease{PartnerID: partnerID, Key: key, Token: uuid.New()}
	for range 3 {
		n, err := q.ClaimKey(ctx, db.ClaimKeyParams{
			PartnerID: partnerID, Key: key, RequestHash: hash, LeaseToken: &lease.Token, LeaseSeconds: LeaseDuration.Seconds(),
		})
		if err != nil {
			return Claim{}, fmt.Errorf("claim key: %w", err)
		}
		if n == 1 {
			return Claim{Result: Claimed, Lease: lease}, nil
		}
		row, err := q.KeyState(ctx, db.KeyStateParams{PartnerID: partnerID, Key: key})
		if errors.Is(err, pgx.ErrNoRows) {
			continue // freed after a failure since our insert: claim it again
		}
		if err != nil {
			return Claim{}, fmt.Errorf("key state: %w", err)
		}
		switch {
		case !bytes.Equal(row.RequestHash, hash):
			return Claim{Result: KeyReused}, nil
		case row.Status == "completed":
			return Claim{Result: Replay, Response: Response{Status: int(*row.ResponseCode), Body: row.ResponseBody}}, nil
		case row.Live:
			return Claim{Result: InProgress}, nil
		}
		n, err = q.TakeOverKey(ctx, db.TakeOverKeyParams{
			PartnerID: partnerID, Key: key, LeaseToken: &lease.Token, LeaseSeconds: LeaseDuration.Seconds(),
		})
		if err != nil {
			return Claim{}, fmt.Errorf("take over key: %w", err)
		}
		if n == 1 {
			return Claim{Result: Claimed, Lease: lease}, nil
		}
		// Someone else completed or took over the key meanwhile: look again.
	}
	return Claim{Result: InProgress}, nil
}

// RunWithLease does a request's work and stores its response in one transaction. The transaction first locks the
// key's row, fenced on the lease token (ErrLeaseLost if another attempt took it over), then runs work, then
// completes the key with work's response and commits. work returns an error only for a failure that must not be
// stored (the caller then frees the key with ReleaseKey); a business refusal is a Response like any other.
func (s *Store) RunWithLease(ctx context.Context, lease Lease, work func(Tx) (Response, error)) (Response, error) {
	var resp Response
	err := s.inTx(ctx, func(tx Tx) error {
		_, err := tx.q.FenceKey(ctx, db.FenceKeyParams{PartnerID: lease.PartnerID, Key: lease.Key, LeaseToken: &lease.Token})
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return fmt.Errorf("fence key: %w", err)
		}
		resp, err = work(tx)
		if err != nil {
			return err
		}
		code := int32(resp.Status) //nolint:gosec // an HTTP status fits
		return tx.q.CompleteKey(ctx, db.CompleteKeyParams{
			PartnerID: lease.PartnerID, Key: lease.Key, LeaseToken: &lease.Token, ResponseCode: &code, ResponseBody: resp.Body,
		})
	})
	if err != nil {
		return Response{}, err
	}
	return resp, nil
}

// ReleaseKey frees a key after a failure whose outcome must not be stored, so a retry runs the request again. It
// deletes only this attempt's own in-progress claim: if the failure was a lost COMMIT acknowledgement, the key is
// already completed and stays, and the retry gets the stored response. If the database is unreachable, nothing
// happens and the lease simply expires.
func (s *Store) ReleaseKey(ctx context.Context, lease Lease) error {
	_, err := db.New(s.pool).ReleaseKey(ctx, db.ReleaseKeyParams{PartnerID: lease.PartnerID, Key: lease.Key, LeaseToken: &lease.Token})
	if err != nil {
		return fmt.Errorf("release key: %w", err)
	}
	return nil
}

// CleanupKeys deletes keys completed more than retention ago, and in-progress claims abandoned that long, in
// batches of batchSize until none are left. It returns how many it deleted. Safe to run on several replicas.
func (s *Store) CleanupKeys(ctx context.Context, retention time.Duration, batchSize int32) (int64, error) {
	q := db.New(s.pool)
	var total int64
	for {
		n, err := q.CleanupKeys(ctx, db.CleanupKeysParams{RetentionSeconds: retention.Seconds(), BatchSize: batchSize})
		if err != nil {
			return total, fmt.Errorf("cleanup keys: %w", err)
		}
		total += n
		if n < int64(batchSize) {
			return total, nil
		}
	}
}
