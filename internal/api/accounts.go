package api

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store"
)

type accountJSON struct {
	ID              uuid.UUID `json:"id"`
	Kind            string    `json:"kind"`
	CustomerRef     *string   `json:"customer_ref"`
	Currency        string    `json:"currency"`
	Status          string    `json:"status"`
	MinBalanceMinor int64     `json:"min_balance_minor"`
	CreatedAt       time.Time `json:"created_at"`
}

func toAccountJSON(a store.AccountView) accountJSON {
	return accountJSON{
		ID: a.ID, Kind: string(a.Kind), CustomerRef: a.CustomerRef, Currency: string(a.Currency),
		Status: string(a.Status), MinBalanceMinor: a.MinBalance, CreatedAt: a.CreatedAt.UTC(),
	}
}

// POST /v1/accounts opens a customer account. A partner's settlement accounts are created with the partner.
func (s *Server) openAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Currency    string  `json:"currency"`
		CustomerRef *string `json:"customer_ref"`
	}
	if !decode(w, r, &req) {
		return
	}
	var bad []fieldError
	currency, err := ledger.ParseCurrency(req.Currency)
	if err != nil {
		bad = append(bad, fieldError{"currency", "must be a supported ISO 4217 code (EUR, USD, GBP, CHF, JPY)"})
	}
	if req.CustomerRef != nil && utf8.RuneCountInString(*req.CustomerRef) > 255 {
		bad = append(bad, fieldError{"customer_ref", "at most 255 characters"})
	}
	if len(bad) > 0 {
		writeProblem(w, "validation_failed", "the request has invalid fields", bad...)
		return
	}
	canonical := struct {
		Currency    ledger.Currency `json:"currency"`
		CustomerRef *string         `json:"customer_ref"`
	}{currency, req.CustomerRef}
	s.idempotent(w, r, canonical, func(ctx context.Context, tx store.Tx) (int, any, error) {
		a, err := tx.OpenCustomerAccount(ctx, partnerOf(r).ID, currency, req.CustomerRef)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusCreated, toAccountJSON(a), nil
	})
}

// GET /v1/accounts/{id}
func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	a, err := s.store.Account(r.Context(), partnerOf(r).ID, id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeOK(w, toAccountJSON(a))
}

// GET /v1/accounts/{id}/balance
func (s *Server) getBalance(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	a, err := s.store.Account(r.Context(), partnerOf(r).ID, id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	available, ok := a.Balance.Available()
	if !ok {
		s.writeError(w, r, fmt.Errorf("account %s: available overflows", a.ID))
		return
	}
	writeOK(w, struct {
		AccountID      uuid.UUID `json:"account_id"`
		Currency       string    `json:"currency"`
		PostedMinor    int64     `json:"posted_minor"`
		HeldMinor      int64     `json:"held_minor"`
		AvailableMinor int64     `json:"available_minor"`
	}{a.ID, string(a.Currency), a.Balance.Posted, a.Balance.Held, available})
}

const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// GET /v1/accounts/{id}/statement?cursor=&limit= returns the account's entries oldest first, a page at a time.
// next_cursor is opaque to the client; it is absent on the last page.
func (s *Server) getStatement(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var bad []fieldError
	after, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		bad = append(bad, fieldError{"cursor", "not a cursor from a previous page"})
	}
	limit := defaultPageSize
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err != nil || n < 1 || n > maxPageSize {
			bad = append(bad, fieldError{"limit", fmt.Sprintf("an integer from 1 to %d", maxPageSize)})
		} else {
			limit = n
		}
	}
	if len(bad) > 0 {
		writeProblem(w, "validation_failed", "the request has invalid parameters", bad...)
		return
	}
	// Ask for one more than a page: if it comes back, there is a next page.
	entries, err := s.store.Statement(r.Context(), partnerOf(r).ID, id, after, int32(limit+1)) //nolint:gosec // limit ≤ maxPageSize
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	type entryJSON struct {
		PostingID     uuid.UUID `json:"posting_id"`
		TransactionID uuid.UUID `json:"transaction_id"`
		AccountSeq    int64     `json:"account_seq"`
		Kind          string    `json:"kind"`
		Reference     *string   `json:"reference"`
		AmountMinor   int64     `json:"amount_minor"`
		Currency      string    `json:"currency"`
		CreatedAt     time.Time `json:"created_at"`
	}
	body := struct {
		Entries    []entryJSON `json:"entries"`
		NextCursor string      `json:"next_cursor,omitempty"`
	}{Entries: []entryJSON{}}
	for i, e := range entries {
		if i == limit {
			body.NextCursor = encodeCursor(entries[limit-1].AccountSeq)
			break
		}
		body.Entries = append(body.Entries, entryJSON{
			e.PostingID, e.TransactionID, e.AccountSeq, string(e.Kind), e.Reference, e.Amount.Amount, string(e.Amount.Currency), e.CreatedAt.UTC(),
		})
	}
	writeOK(w, body)
}

// A cursor is the account_seq of the last entry on the previous page, base64url-encoded so clients treat it as
// opaque (it can change shape without breaking them).
func encodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString(strconv.AppendInt(nil, seq, 10))
}

func decodeCursor(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return 0, err
	}
	seq, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || seq < 0 {
		return 0, fmt.Errorf("cursor %q is not a position in a statement", s)
	}
	return seq, nil
}
