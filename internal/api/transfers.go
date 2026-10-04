package api

import (
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
)

// POST /v1/transfers moves money between two of the caller's accounts.
func (s *Server) createTransfer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From        string  `json:"from"`
		To          string  `json:"to"`
		AmountMinor *int64  `json:"amount_minor"`
		Currency    string  `json:"currency"`
		Reference   *string `json:"reference"`
	}
	if !decode(w, r, &req) {
		return
	}
	var bad []fieldError
	from, err := uuid.Parse(req.From)
	if err != nil {
		bad = append(bad, fieldError{"from", "must be an account id"})
	}
	to, err := uuid.Parse(req.To)
	if err != nil {
		bad = append(bad, fieldError{"to", "must be an account id"})
	}
	if req.AmountMinor == nil || *req.AmountMinor <= 0 {
		bad = append(bad, fieldError{"amount_minor", "must be a positive integer number of minor units"})
	}
	currency, err := ledger.ParseCurrency(req.Currency)
	if err != nil {
		bad = append(bad, fieldError{"currency", "must be a supported ISO 4217 code (EUR, USD, GBP, CHF, JPY)"})
	}
	if req.Reference != nil && utf8.RuneCountInString(*req.Reference) > 255 {
		bad = append(bad, fieldError{"reference", "at most 255 characters"})
	}
	if len(bad) > 0 {
		writeProblem(w, "validation_failed", "the request has invalid fields", bad...)
		return
	}
	amount := ledger.Money{Amount: *req.AmountMinor, Currency: currency}
	t, err := s.store.Transfer(r.Context(), partnerOf(r).ID, from, to, amount, req.Reference)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, struct {
		ID          uuid.UUID `json:"id"`
		From        uuid.UUID `json:"from"`
		To          uuid.UUID `json:"to"`
		AmountMinor int64     `json:"amount_minor"`
		Currency    string    `json:"currency"`
		Reference   *string   `json:"reference"`
		CreatedAt   time.Time `json:"created_at"`
	}{t.ID, t.From, t.To, t.Amount.Amount, string(t.Amount.Currency), t.Reference, t.CreatedAt.UTC()})
}
