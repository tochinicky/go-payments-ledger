package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
)

// problem is an RFC 9457 problem+json body, with the stable code the error catalogue promises.
type problem struct {
	Type   string       `json:"type"`
	Title  string       `json:"title"`
	Status int          `json:"status"`
	Code   string       `json:"code"`
	Detail string       `json:"detail,omitempty"`
	Errors []fieldError `json:"errors,omitempty"`
}

// fieldError says which request field failed validation, and why.
type fieldError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

// statusOf maps the ledger's error codes to HTTP statuses (the error catalogue). A missing code is a bug: 500.
var statusOf = map[string]int{
	"validation_failed":    http.StatusBadRequest,
	"malformed_json":       http.StatusBadRequest,
	"unauthenticated":      http.StatusUnauthorized,
	"not_found":            http.StatusNotFound,
	"body_too_large":       http.StatusRequestEntityTooLarge,
	"account_not_active":   http.StatusConflict,
	"insufficient_funds":   http.StatusUnprocessableEntity,
	"currency_mismatch":    http.StatusUnprocessableEntity,
	"same_account":         http.StatusUnprocessableEntity,
	"account_not_empty":    http.StatusConflict,
	"account_not_closable": http.StatusConflict,
}

func writeProblem(w http.ResponseWriter, code, detail string, fields ...fieldError) {
	status, ok := statusOf[code]
	if !ok {
		status, code, detail = http.StatusInternalServerError, "internal", ""
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Type:   "https://errors.ledger.example/" + code,
		Title:  http.StatusText(status),
		Status: status,
		Code:   code,
		Detail: detail,
		Errors: fields,
	})
}

// writeError answers with a business error's code and message, or a bare 500 for anything else. The cause of a 500
// is logged, never sent: it can name tables, constraints or hosts.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var lerr *ledger.Error
	if errors.As(err, &lerr) && lerr.Code != "internal" {
		writeProblem(w, lerr.Code, lerr.Message)
		return
	}
	s.log.ErrorContext(r.Context(), "request failed", slog.String("method", r.Method), slog.String("path", r.URL.Path), slog.Any("error", err))
	writeProblem(w, "internal", "")
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
