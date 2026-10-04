package api_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

func (c client) transactions(t *testing.T) int {
	t.Helper()
	var n int
	if err := tdb.Owner.QueryRow(context.Background(),
		"SELECT count(*) FROM transactions t JOIN postings p ON p.transaction_id = t.id WHERE p.account_id = $1", c.settlement).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestWritesRequireAnIdempotencyKey(t *testing.T) {
	c := newClient(t, 1_000)
	alice := c.openAccount()
	for _, tt := range []struct{ path, body string }{
		{"/v1/accounts", `{"currency":"EUR"}`},
		{"/v1/transfers", transferBody(c.settlement.String(), alice, 1)},
	} {
		if resp := c.send("POST", tt.path, tt.body, ""); resp.status != http.StatusBadRequest || !bytes.Contains(resp.body, []byte(`"idempotency_key_required"`)) {
			t.Errorf("%s without a key: %d %s", tt.path, resp.status, resp.body)
		}
	}
}

// A completed key replays its response byte for byte, even when the retry's JSON is laid out differently.
func TestSameKeyReplaysTheStoredResponse(t *testing.T) {
	c := newClient(t, 1_000)
	alice := c.openAccount()
	key := uuid.NewString()
	first := c.send("POST", "/v1/transfers", `{"from":"`+c.settlement.String()+`","to":"`+alice+`","amount_minor":7,"currency":"EUR"}`, key)
	if first.status != http.StatusCreated {
		t.Fatalf("first: %d %s", first.status, first.body)
	}
	retry := c.send("POST", "/v1/transfers", `{ "currency": "EUR", "amount_minor": 7,
		"to": "`+alice+`", "from": "`+c.settlement.String()+`" }`, key)
	if retry.status != first.status || !bytes.Equal(retry.body, first.body) {
		t.Fatalf("retry: %d %s, want %d %s", retry.status, retry.body, first.status, first.body)
	}
	if retry.header.Get("Idempotent-Replayed") != "true" {
		t.Errorf("retry: Idempotent-Replayed = %q", retry.header.Get("Idempotent-Replayed"))
	}
	if n := c.transactions(t); n != 1 {
		t.Errorf("%d transactions, want 1", n)
	}
}

func TestSameKeyDifferentRequestIsRefused(t *testing.T) {
	c := newClient(t, 1_000)
	alice := c.openAccount()
	key := uuid.NewString()
	if resp := c.send("POST", "/v1/transfers", transferBody(c.settlement.String(), alice, 5), key); resp.status != http.StatusCreated {
		t.Fatalf("first: %d %s", resp.status, resp.body)
	}
	resp := c.send("POST", "/v1/transfers", transferBody(c.settlement.String(), alice, 6), key)
	if resp.status != http.StatusUnprocessableEntity || !bytes.Contains(resp.body, []byte(`"idempotency_key_reused"`)) {
		t.Fatalf("different amount, same key: %d %s", resp.status, resp.body)
	}
	// The same key on another route is a different request too.
	resp = c.send("POST", "/v1/accounts", `{"currency":"EUR"}`, key)
	if resp.status != http.StatusUnprocessableEntity {
		t.Fatalf("same key on another route: %d %s", resp.status, resp.body)
	}
}

// A business refusal is final for its key (a new key is how you try again); a validation error stores nothing,
// so the corrected request can use the same key.
func TestWhichOutcomesAreStored(t *testing.T) {
	c := newClient(t, 1_000)
	alice, bob := c.openAccount(), c.openAccount()

	poor := uuid.NewString()
	if resp := c.send("POST", "/v1/transfers", transferBody(alice, bob, 50), poor); resp.status != http.StatusUnprocessableEntity {
		t.Fatalf("unfunded: %d %s", resp.status, resp.body)
	}
	if status, body := c.transfer(c.settlement.String(), alice, 100); status != http.StatusCreated {
		t.Fatalf("funding: %d %v", status, body)
	}
	resp := c.send("POST", "/v1/transfers", transferBody(alice, bob, 50), poor)
	if resp.status != http.StatusUnprocessableEntity || !bytes.Contains(resp.body, []byte(`"insufficient_funds"`)) || resp.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("same key after funding: %d %s, want the stored insufficient_funds", resp.status, resp.body)
	}
	if resp := c.send("POST", "/v1/transfers", transferBody(alice, bob, 50), uuid.NewString()); resp.status != http.StatusCreated {
		t.Fatalf("new key after funding: %d %s", resp.status, resp.body)
	}

	fixed := uuid.NewString()
	if resp := c.send("POST", "/v1/transfers", transferBody(alice, bob, 0), fixed); resp.status != http.StatusBadRequest {
		t.Fatalf("invalid: %d %s", resp.status, resp.body)
	}
	if resp := c.send("POST", "/v1/transfers", transferBody(alice, bob, 1), fixed); resp.status != http.StatusCreated {
		t.Fatalf("corrected request, same key: %d %s", resp.status, resp.body)
	}
}

// Scenario 1, the retry storm: 50 identical requests with the same key at once. Exactly one transaction; every
// answer is the stored 201 or a 409 while the first is in flight, and a final retry gets the same 201.
//
// Left alone, the first request finishes in a few milliseconds and the storm mostly sees the stored answer, so the
// test makes "in flight" certain: it holds the settlement account's balance row locked, which parks whichever
// request claims the key inside its ledger transaction until the other 49 have been answered.
func TestRetryStorm(t *testing.T) {
	ctx := context.Background()
	c := newClient(t, 1_000)
	alice := c.openAccount()
	key, body := uuid.NewString(), transferBody(c.settlement.String(), alice, 10)

	blocker, err := tdb.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, "SELECT 1 FROM balances WHERE account_id = $1 FOR UPDATE", c.settlement); err != nil {
		t.Fatal(err)
	}

	const n = 50
	answers := make(chan response, n)
	start := make(chan struct{})
	for range n {
		go func() {
			<-start
			answers <- c.send("POST", "/v1/transfers", body, key)
		}()
	}
	close(start)
	var responses []response
	for range n - 1 { // everyone but the request parked behind the lock
		responses = append(responses, <-answers)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	responses = append(responses, <-answers)

	final := c.send("POST", "/v1/transfers", body, key)
	if final.status != http.StatusCreated {
		t.Fatalf("final retry: %d %s", final.status, final.body)
	}
	created, inFlight := 0, 0
	for i, resp := range responses {
		switch resp.status {
		case http.StatusCreated:
			created++
			if !bytes.Equal(resp.body, final.body) {
				t.Errorf("response %d differs from the stored one:\n%s\n%s", i, resp.body, final.body)
			}
		case http.StatusConflict:
			inFlight++
			if !bytes.Contains(resp.body, []byte(`"idempotency_in_progress"`)) || resp.header.Get("Retry-After") != "1" {
				t.Errorf("response %d: 409 %s, Retry-After %q", i, resp.body, resp.header.Get("Retry-After"))
			}
		default:
			t.Errorf("response %d: %d %s", i, resp.status, resp.body)
		}
	}
	if created != 1 || inFlight != n-1 {
		t.Errorf("%d × 201 and %d × 409, want 1 and %d", created, inFlight, n-1)
	}
	if got := c.transactions(t); got != 1 {
		t.Errorf("%d transactions, want exactly 1", got)
	}
}
