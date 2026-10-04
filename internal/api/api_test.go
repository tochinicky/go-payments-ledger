package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/api"
	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/obs"
	"github.com/tochinicky/go-payments-ledger/internal/store"
	"github.com/tochinicky/go-payments-ledger/internal/testdb"
)

var (
	tdb       *testdb.DB
	srv       *httptest.Server
	apiServer *api.Server
	telemetry *obs.Telemetry
)

func TestMain(m *testing.M) {
	os.Exit(testdb.Run(m, &tdb, func() {
		var err error
		if telemetry, err = obs.Setup(context.Background(), "ledger-api-test"); err != nil {
			panic(err)
		}
		apiServer = api.New(api.Config{Store: store.New(tdb.App, store.NewID), Log: slog.New(slog.DiscardHandler), RequestTimeout: store.DefaultTimeouts.Request})
		srv = httptest.NewServer(apiServer.Handler())
	}))
}

// client is one partner talking to the API.
type client struct {
	t          *testing.T
	key        string
	partner    uuid.UUID
	settlement uuid.UUID
}

func newClient(t *testing.T, fundingLimit int64) client {
	t.Helper()
	key := uuid.NewString() + uuid.NewString() // tests only: real keys come from crypto/rand
	hash := sha256.Sum256([]byte(key))
	ctx := context.Background()
	p, err := store.New(tdb.Owner, store.NewID).CreatePartner(ctx, "API partner", hash[:], fundingLimit, []ledger.Currency{"EUR"})
	if err != nil {
		t.Fatal(err)
	}
	var settlement uuid.UUID
	if err := tdb.Owner.QueryRow(ctx, "SELECT id FROM accounts WHERE partner_id = $1 AND kind = 'settlement'", p.ID).Scan(&settlement); err != nil {
		t.Fatal(err)
	}
	return client{t: t, key: key, partner: p.ID, settlement: settlement}
}

// do sends a request and decodes the JSON answer into a map. Writes get a fresh Idempotency-Key.
func (c client) do(method, path, body string) (int, map[string]any) {
	c.t.Helper()
	key := ""
	if method == http.MethodPost {
		key = uuid.NewString()
	}
	resp := c.send(method, path, body, key)
	var out map[string]any
	if err := json.Unmarshal(resp.body, &out); err != nil {
		c.t.Fatalf("%s %s: %d, body %q is not JSON", method, path, resp.status, resp.body)
	}
	return resp.status, out
}

// response is an HTTP answer, with the body as raw bytes so replays can be compared byte for byte.
type response struct {
	status int
	header http.Header
	body   []byte
}

func (c client) send(method, path, body, idempotencyKey string) response {
	c.t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	if c.key != "" {
		req.Header.Set("Authorization", "Bearer "+c.key)
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatal(err)
	}
	if resp.StatusCode >= 400 && resp.Header.Get("Content-Type") != "application/problem+json" {
		c.t.Errorf("%s %s: error with content type %q", method, path, resp.Header.Get("Content-Type"))
	}
	return response{status: resp.StatusCode, header: resp.Header, body: raw}
}

func (c client) openAccount() string {
	c.t.Helper()
	status, body := c.do("POST", "/v1/accounts", `{"currency":"EUR","customer_ref":"cust-1"}`)
	if status != http.StatusCreated {
		c.t.Fatalf("open account: %d %v", status, body)
	}
	return body["id"].(string)
}

func (c client) transfer(from, to string, amount int) (int, map[string]any) {
	c.t.Helper()
	body, _ := json.Marshal(map[string]any{"from": from, "to": to, "amount_minor": amount, "currency": "EUR", "reference": "test"})
	return c.do("POST", "/v1/transfers", string(body))
}

func TestOpenAccountTransferAndReadBack(t *testing.T) {
	c := newClient(t, 10_000)
	alice := c.openAccount()

	status, tr := c.transfer(c.settlement.String(), alice, 1_250)
	if status != http.StatusCreated || tr["amount_minor"] != 1250.0 || tr["to"] != alice {
		t.Fatalf("transfer: %d %v", status, tr)
	}

	status, acct := c.do("GET", "/v1/accounts/"+alice, "")
	if status != http.StatusOK || acct["kind"] != "customer" || acct["status"] != "active" || acct["customer_ref"] != "cust-1" {
		t.Fatalf("account: %d %v", status, acct)
	}
	status, bal := c.do("GET", "/v1/accounts/"+alice+"/balance", "")
	if status != http.StatusOK || bal["posted_minor"] != 1250.0 || bal["available_minor"] != 1250.0 {
		t.Fatalf("balance: %d %v", status, bal)
	}
	status, st := c.do("GET", "/v1/accounts/"+alice+"/statement", "")
	entries, _ := st["entries"].([]any)
	if status != http.StatusOK || len(entries) != 1 || st["next_cursor"] != nil {
		t.Fatalf("statement: %d %v", status, st)
	}
	if e := entries[0].(map[string]any); e["transaction_id"] != tr["id"] || e["amount_minor"] != 1250.0 || e["kind"] != "transfer" {
		t.Fatalf("statement entry: %v", e)
	}
}

func TestStatementPagination(t *testing.T) {
	c := newClient(t, 10_000)
	alice := c.openAccount()
	for range 5 {
		if status, body := c.transfer(c.settlement.String(), alice, 1); status != http.StatusCreated {
			t.Fatalf("transfer: %d %v", status, body)
		}
	}
	seen, pages, cursor := 0, 0, ""
	for {
		_, st := c.do("GET", "/v1/accounts/"+alice+"/statement?limit=2&cursor="+cursor, "")
		seen += len(st["entries"].([]any))
		pages++
		next, ok := st["next_cursor"].(string)
		if !ok {
			break
		}
		cursor = next
	}
	if seen != 5 || pages != 3 {
		t.Fatalf("%d entries over %d pages, want 5 over 3", seen, pages)
	}
}

func TestErrorCatalogue(t *testing.T) {
	c := newClient(t, 100)
	other := newClient(t, 100)
	alice, bob := c.openAccount(), c.openAccount()
	theirs := other.openAccount()

	tests := []struct {
		name               string
		client             client
		method, path, body string
		status             int
		code               string
	}{
		{"no API key", client{t: t}, "GET", "/v1/accounts/" + alice, "", 401, "unauthenticated"},
		{"unknown API key", client{t: t, key: "nope"}, "GET", "/v1/accounts/" + alice, "", 401, "unauthenticated"},
		{"another partner's account", c, "GET", "/v1/accounts/" + theirs, "", 404, "not_found"},
		{"another partner's statement", c, "GET", "/v1/accounts/" + theirs + "/statement", "", 404, "not_found"},
		{"malformed id", c, "GET", "/v1/accounts/xyz/balance", "", 404, "not_found"},
		{"malformed JSON", c, "POST", "/v1/transfers", `{"from":`, 400, "malformed_json"},
		{"unknown field", c, "POST", "/v1/accounts", `{"currency":"EUR","colour":"red"}`, 400, "malformed_json"},
		{"two JSON objects", c, "POST", "/v1/accounts", `{"currency":"EUR"}{}`, 400, "malformed_json"},
		{"body too large", c, "POST", "/v1/accounts", `{"customer_ref":"` + strings.Repeat("x", 70_000) + `"}`, 413, "body_too_large"},
		{"unsupported currency", c, "POST", "/v1/accounts", `{"currency":"XYZ"}`, 400, "validation_failed"},
		{"bad cursor", c, "GET", "/v1/accounts/" + alice + "/statement?cursor=!!", "", 400, "validation_failed"},
		{"limit too big", c, "GET", "/v1/accounts/" + alice + "/statement?limit=1000", "", 400, "validation_failed"},
		{"amount zero", c, "POST", "/v1/transfers", transferBody(alice, bob, 0), 400, "validation_failed"},
		{"insufficient funds", c, "POST", "/v1/transfers", transferBody(alice, bob, 1), 422, "insufficient_funds"},
		{"same account", c, "POST", "/v1/transfers", transferBody(alice, alice, 1), 422, "same_account"},
		{"transfer to another partner", c, "POST", "/v1/transfers", transferBody(alice, theirs, 1), 404, "not_found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.client.t = t
			status, body := tt.client.do(tt.method, tt.path, tt.body)
			if status != tt.status || body["code"] != tt.code || body["status"] != float64(tt.status) {
				t.Fatalf("%d %v, want %d %s", status, body, tt.status, tt.code)
			}
		})
	}

	// validation_failed names every bad field at once.
	_, body := c.do("POST", "/v1/transfers", `{"from":"x","to":"y","currency":"??"}`)
	fields := map[string]bool{}
	for _, e := range body["errors"].([]any) {
		fields[e.(map[string]any)["field"].(string)] = true
	}
	for _, f := range []string{"from", "to", "amount_minor", "currency"} {
		if !fields[f] {
			t.Errorf("errors %v: missing field %s", body["errors"], f)
		}
	}
}

func transferBody(from, to string, amount int) string {
	var b bytes.Buffer
	_ = json.NewEncoder(&b).Encode(map[string]any{"from": from, "to": to, "amount_minor": amount, "currency": "EUR"})
	return b.String()
}
