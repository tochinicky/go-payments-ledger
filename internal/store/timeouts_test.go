package store_test

import (
	"testing"
	"time"

	"github.com/tochinicky/go-payments-ledger/internal/store"
)

func TestTimeoutsMustNestInsideTheLease(t *testing.T) {
	s := time.Second
	tests := []struct {
		name string
		t    store.Timeouts
		ok   bool
	}{
		{"defaults", store.DefaultTimeouts, true},
		{"request as long as the lease", store.Timeouts{Request: store.LeaseDuration, Statement: 8 * s, Lock: 5 * s}, false},
		{"statement as long as the request", store.Timeouts{Request: 10 * s, Statement: 10 * s, Lock: 5 * s}, false},
		{"lock longer than a statement", store.Timeouts{Request: 10 * s, Statement: 8 * s, Lock: 9 * s}, false},
		{"no lock timeout", store.Timeouts{Request: 10 * s, Statement: 8 * s}, false},
	}
	for _, tt := range tests {
		if err := tt.t.Validate(); (err == nil) != tt.ok {
			t.Errorf("%s: Validate() = %v, want ok=%v", tt.name, err, tt.ok)
		}
	}
}
