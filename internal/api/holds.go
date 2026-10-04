package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store"
)

// maxHoldDuration caps expires_in: a reservation that outlives a month is a forgotten one.
const maxHoldDuration = 30 * 24 * time.Hour

type holdJSON struct {
	ID            uuid.UUID  `json:"id"`
	Account       uuid.UUID  `json:"account"`
	ToAccount     uuid.UUID  `json:"to_account"`
	AmountMinor   int64      `json:"amount_minor"`
	Currency      string     `json:"currency"`
	Status        string     `json:"status"`
	CapturedMinor int64      `json:"captured_minor"`
	TransactionID *uuid.UUID `json:"transaction_id"`
	ExpiresAt     time.Time  `json:"expires_at"`
	CreatedAt     time.Time  `json:"created_at"`
}

func toHoldJSON(h store.Hold) holdJSON {
	return holdJSON{
		ID: h.ID, Account: h.AccountID, ToAccount: h.ToAccountID, AmountMinor: h.Amount.Amount, Currency: string(h.Amount.Currency),
		Status: string(h.Status), CapturedMinor: h.Captured, TransactionID: h.TransactionID,
		ExpiresAt: h.ExpiresAt.UTC(), CreatedAt: h.CreatedAt.UTC(),
	}
}

// POST /v1/holds reserves money on an account for a destination fixed now. expires_in is in seconds.
func (s *Server) placeHold(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Account     string `json:"account"`
		ToAccount   string `json:"to_account"`
		AmountMinor *int64 `json:"amount_minor"`
		Currency    string `json:"currency"`
		ExpiresIn   *int64 `json:"expires_in"`
	}
	if !decode(w, r, &req) {
		return
	}
	var bad []fieldError
	from, err := uuid.Parse(req.Account)
	if err != nil {
		bad = append(bad, fieldError{"account", "must be an account id"})
	}
	to, err := uuid.Parse(req.ToAccount)
	if err != nil {
		bad = append(bad, fieldError{"to_account", "must be an account id"})
	}
	if req.AmountMinor == nil || *req.AmountMinor <= 0 {
		bad = append(bad, fieldError{"amount_minor", "must be a positive integer number of minor units"})
	}
	currency, err := ledger.ParseCurrency(req.Currency)
	if err != nil {
		bad = append(bad, fieldError{"currency", "must be a supported ISO 4217 code (EUR, USD, GBP, CHF, JPY)"})
	}
	if req.ExpiresIn == nil || *req.ExpiresIn < 1 || *req.ExpiresIn > int64(maxHoldDuration.Seconds()) {
		bad = append(bad, fieldError{"expires_in", fmt.Sprintf("seconds, from 1 to %d", int64(maxHoldDuration.Seconds()))})
	}
	if len(bad) > 0 {
		writeProblem(w, "validation_failed", "the request has invalid fields", bad...)
		return
	}
	amount := ledger.Money{Amount: *req.AmountMinor, Currency: currency}
	expiresIn := time.Duration(*req.ExpiresIn) * time.Second
	canonical := struct {
		Account     uuid.UUID       `json:"account"`
		ToAccount   uuid.UUID       `json:"to_account"`
		AmountMinor int64           `json:"amount_minor"`
		Currency    ledger.Currency `json:"currency"`
		ExpiresIn   int64           `json:"expires_in"`
	}{from, to, amount.Amount, currency, *req.ExpiresIn}
	s.idempotent(w, r, canonical, func(ctx context.Context, tx store.Tx) (int, any, error) {
		h, err := tx.PlaceHold(ctx, partnerOf(r).ID, from, to, amount, expiresIn)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusCreated, toHoldJSON(h), nil
	})
}

// GET /v1/holds/{id}
func (s *Server) getHold(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	h, err := s.store.Hold(r.Context(), partnerOf(r).ID, id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeOK(w, toHoldJSON(h))
}

// POST /v1/holds/{id}/capture {amount_minor} moves up to the held amount to the hold's destination and releases
// the rest. 200: the hold changed state; the capture's transaction is its transaction_id.
func (s *Server) captureHold(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req struct {
		AmountMinor *int64 `json:"amount_minor"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.AmountMinor == nil || *req.AmountMinor <= 0 {
		writeProblem(w, "validation_failed", "the request has invalid fields",
			fieldError{"amount_minor", "must be a positive integer number of minor units"})
		return
	}
	canonical := struct {
		AmountMinor int64 `json:"amount_minor"`
	}{*req.AmountMinor}
	s.idempotent(w, r, canonical, func(ctx context.Context, tx store.Tx) (int, any, error) {
		h, err := tx.CaptureHold(ctx, partnerOf(r).ID, id, *req.AmountMinor)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, toHoldJSON(h), nil
	})
}

// POST /v1/holds/{id}/release frees the reservation. The body may be empty or {}.
func (s *Server) releaseHold(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req struct{}
	if !decodeOptional(w, r, &req) {
		return
	}
	s.idempotent(w, r, req, func(ctx context.Context, tx store.Tx) (int, any, error) {
		h, err := tx.ReleaseHold(ctx, partnerOf(r).ID, id)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, toHoldJSON(h), nil
	})
}
