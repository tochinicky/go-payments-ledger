package obs_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tochinicky/go-payments-ledger/internal/obs"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), "GET", path, nil))
	body, _ := io.ReadAll(rec.Body)
	return rec.Code, string(body)
}

func TestHealthAndReadiness(t *testing.T) {
	tel, err := obs.Setup(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	var notReady error
	health := &obs.Health{Ready: func(context.Context) error { return notReady }}
	h := tel.AdminHandler(health)

	if code, _ := get(t, h, "/healthz"); code != 200 {
		t.Errorf("healthz %d", code)
	}
	if code, _ := get(t, h, "/readyz"); code != 200 {
		t.Errorf("readyz %d, want 200", code)
	}
	notReady = errors.New("schema at migration 3")
	if code, body := get(t, h, "/readyz"); code != 503 {
		t.Errorf("readyz %d %q, want 503 while the dependency isn't ready", code, body)
	}
	notReady = nil
	health.Drain()
	if code, _ := get(t, h, "/readyz"); code != 503 {
		t.Errorf("readyz %d, want 503 while draining", code)
	}
	if code, _ := get(t, h, "/healthz"); code != 200 {
		t.Errorf("healthz %d while draining: liveness must not follow readiness", code)
	}
	if code, _ := get(t, h, "/metrics"); code != 200 {
		t.Errorf("metrics %d", code)
	}
}
