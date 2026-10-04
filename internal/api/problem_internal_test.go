package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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
