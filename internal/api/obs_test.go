package api_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tochinicky/go-payments-ledger/internal/obs"
	"github.com/tochinicky/go-payments-ledger/internal/store"
	"github.com/tochinicky/go-payments-ledger/migrations"
)

// The SLO is stated on POST /v1/transfers latency: the histogram is labelled by route (never by path) and has a
// bucket edge at exactly 0.1 s, so "under 100 ms" is a count.
func TestLatencyHistogramHasThe100msBucketPerRoute(t *testing.T) {
	c := newClient(t, 1_000)
	alice := c.openAccount()
	if status, body := c.transfer(c.settlement.String(), alice, 1); status != http.StatusCreated {
		t.Fatalf("transfer: %d %v", status, body)
	}
	rec := httptest.NewRecorder()
	telemetry.AdminHandler(&obs.Health{}).ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), "GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	var bucket string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "http_server_request_duration_seconds_bucket{") &&
			strings.Contains(line, `http_route="POST /v1/transfers"`) && strings.Contains(line, `le="0.1"`) {
			bucket = line
		}
		if strings.Contains(line, alice) {
			t.Errorf("a metric carries an account id (a path, not a route): %s", line)
		}
	}
	if bucket == "" {
		t.Fatalf("no 0.1 s bucket for POST /v1/transfers in:\n%s", body)
	}
}

// Readiness is gated on the schema: ready at this build's migration version, not ready if a newer one is needed.
func TestReadinessFollowsTheSchemaVersion(t *testing.T) {
	st := store.New(tdb.App, store.NewID)
	if err := st.Ready(context.Background(), migrations.Version); err != nil {
		t.Errorf("at version %d: %v", migrations.Version, err)
	}
	if err := st.Ready(context.Background(), migrations.Version+1); err == nil {
		t.Error("ready although the schema is behind the build")
	}
}
