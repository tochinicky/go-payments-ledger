package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// Losing the database must answer 503 (retry with the same key), never 500. Each of these was seen in the
// connection-loss scenario or is a documented connection or timeout error.
func TestUnavailableClassifiesConnectionLoss(t *testing.T) {
	retryable := map[string]error{
		"dropped connection":   fmt.Errorf("lock accounts: %w", io.ErrUnexpectedEOF),
		"connection reset":     &net.OpError{Op: "read", Err: errors.New("connection reset by peer")},
		"connection exception": &pgconn.PgError{Code: "08006"},
		"admin shutdown":       &pgconn.PgError{Code: "57P01"},
		"lock timeout":         &pgconn.PgError{Code: "55P03"},
		"statement timeout":    &pgconn.PgError{Code: "57014"},
		"request deadline":     fmt.Errorf("claim key: %w", context.DeadlineExceeded),
	}
	for name, err := range retryable {
		if !unavailable(err) {
			t.Errorf("%s: %v is not classified as unavailable", name, err)
		}
	}
	for name, err := range map[string]error{
		"a bug":                errors.New("nil pointer"),
		"constraint violation": &pgconn.PgError{Code: "23505"},
	} {
		if unavailable(err) {
			t.Errorf("%s: %v is classified as unavailable", name, err)
		}
	}
}

// A client that disconnects is its own outcome (499 in metrics, no body), never a 500 against the error-rate SLO.
// Our own deadline is still a 503: the client is waiting for an answer.
func TestClientClosedIsNotAServerError(t *testing.T) {
	srv := &Server{log: slog.New(slog.DiscardHandler)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gone := httptest.NewRequestWithContext(ctx, "POST", "/v1/transfers", nil)
	rec := httptest.NewRecorder()
	srv.writeError(rec, gone, fmt.Errorf("lock accounts: %w", context.Canceled))
	if rec.Code != statusClientClosed || rec.Body.Len() != 0 {
		t.Errorf("client gone: %d %q, want 499 and no body", rec.Code, rec.Body.String())
	}

	deadline, cancel2 := context.WithTimeout(context.Background(), 0)
	defer cancel2()
	<-deadline.Done()
	late := httptest.NewRequestWithContext(deadline, "POST", "/v1/transfers", nil)
	rec = httptest.NewRecorder()
	srv.writeError(rec, late, fmt.Errorf("lock accounts: %w", context.DeadlineExceeded))
	if rec.Code != 503 {
		t.Errorf("our deadline: %d, want 503", rec.Code)
	}
}
