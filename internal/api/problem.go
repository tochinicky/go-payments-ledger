package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

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

// statusOf maps error codes to HTTP statuses (the error catalogue). A code missing here is a bug: 500.
var statusOf = map[string]int{
	"validation_failed":        http.StatusBadRequest,
	"malformed_json":           http.StatusBadRequest,
	"idempotency_key_required": http.StatusBadRequest,
	"unauthenticated":          http.StatusUnauthorized,
	"not_found":                http.StatusNotFound,
	"body_too_large":           http.StatusRequestEntityTooLarge,
	"idempotency_in_progress":  http.StatusConflict,
	"account_not_active":       http.StatusConflict,
	"account_not_empty":        http.StatusConflict,
	"account_not_closable":     http.StatusConflict,
	"hold_not_active":          http.StatusConflict,
	"hold_not_capturable":      http.StatusConflict,
	"capture_exceeds_hold":     http.StatusUnprocessableEntity,
	"idempotency_key_reused":   http.StatusUnprocessableEntity,
	"insufficient_funds":       http.StatusUnprocessableEntity,
	"currency_mismatch":        http.StatusUnprocessableEntity,
	"same_account":             http.StatusUnprocessableEntity,
	"rate_limited":             http.StatusTooManyRequests,
	"unavailable":              http.StatusServiceUnavailable,
}

// problemResponse renders a problem as a status and body, ready to send or to store with an idempotency key.
func problemResponse(code, detail string, fields ...fieldError) (int, []byte) {
	status, ok := statusOf[code]
	if !ok {
		status, code, detail = http.StatusInternalServerError, "internal", ""
	}
	return status, mustJSON(problem{
		Type:   "https://errors.ledger.example/" + code,
		Title:  http.StatusText(status),
		Status: status,
		Code:   code,
		Detail: detail,
		Errors: fields,
	})
}

func writeProblem(w http.ResponseWriter, code, detail string, fields ...fieldError) {
	status, body := problemResponse(code, detail, fields...)
	if code == "idempotency_in_progress" || code == "unavailable" { // both are worth retrying shortly
		w.Header().Set("Retry-After", "1")
	}
	writeBody(w, status, body)
}

// businessError reports whether err is a business refusal (a deterministic 4xx), and renders it.
func businessError(err error) (status int, body []byte, ok bool) {
	var lerr *ledger.Error
	if !errors.As(err, &lerr) || lerr.Code == "internal" {
		return 0, nil, false
	}
	status, body = problemResponse(lerr.Code, lerr.Message)
	return status, body, true
}

// writeError answers with a business error's code and message; with 503 when the database couldn't be reached
// (worth retrying); or with a bare 500. The cause of a 5xx is logged, never sent: it can name tables or hosts.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	if status, body, ok := businessError(err); ok {
		writeBody(w, status, body)
		return
	}
	if clientGone(r, err) {
		// Nobody is listening: no body, a debug line, and status 499 ("client closed request", as nginx records it)
		// so the metrics show it as its own outcome, outside the error-rate SLO. Never stored, like any 5xx.
		s.log.DebugContext(r.Context(), "client closed the request", slog.String("method", r.Method), slog.String("path", r.URL.Path))
		w.WriteHeader(statusClientClosed)
		return
	}
	s.log.ErrorContext(r.Context(), "request failed", slog.String("method", r.Method), slog.String("path", r.URL.Path), slog.Any("error", err))
	if unavailable(err) {
		writeProblem(w, "unavailable", "")
		return
	}
	writeProblem(w, "internal", "")
}

// statusClientClosed is nginx's 499: the client disconnected before the answer. It appears only in metrics.
const statusClientClosed = 499

// clientGone reports whether the request failed because the client disconnected (its context was cancelled, as
// opposed to our own deadline, which is DeadlineExceeded and answers 503).
func clientGone(r *http.Request, err error) bool {
	return errors.Is(err, context.Canceled) && errors.Is(r.Context().Err(), context.Canceled)
}

// unavailable reports whether err means the database couldn't be reached, dropped the connection, or didn't answer
// in time (the request's deadline, statement_timeout, lock_timeout), rather than a bug. All are worth retrying with
// the same idempotency key, and none is stored.
func unavailable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "08"): // connection exception
			return true
		case pgErr.Code == "55P03", pgErr.Code == "57014": // lock_not_available, query_canceled
			return true
		case pgErr.Code == "57P01", pgErr.Code == "57P02", pgErr.Code == "57P03": // the server is shutting down or not accepting yet
			return true
		}
	}
	var netErr net.Error
	var connectErr *pgconn.ConnectError
	return errors.As(err, &netErr) || errors.As(err, &connectErr) || pgconn.Timeout(err) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) // the connection dropped mid-conversation
}

// writeOK answers a read with 200 and a JSON body. (Writes answer through idempotent, which stores the response.)
func writeOK(w http.ResponseWriter, body any) {
	writeBody(w, http.StatusOK, mustJSON(body))
}

// writeBody sends a rendered JSON body: problem+json for errors.
func writeBody(w http.ResponseWriter, status int, body []byte) {
	if status >= 400 {
		w.Header().Set("Content-Type", "application/problem+json")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(body) //nolint:gosec // G705: always JSON we rendered, served as JSON with nosniff, never HTML
}

// mustJSON marshals the API's own response types, which always marshal.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}
