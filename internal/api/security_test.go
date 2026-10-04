package api_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/obs"
)

type auditRow struct {
	partner         *uuid.UUID
	actor, action   string
	resource, reqID string
	status          int
}

func auditRows(t *testing.T, where string, arg any) []auditRow {
	t.Helper()
	rows, err := tdb.Owner.Query(context.Background(),
		"SELECT partner_id, actor, action, resource, status, request_id FROM audit_log WHERE "+where+" ORDER BY at, id", arg)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []auditRow
	for rows.Next() {
		var a auditRow
		if err := rows.Scan(&a.partner, &a.actor, &a.action, &a.resource, &a.status, &a.reqID); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Every write attempt is audited with its outcome; a write that ran is audited in its own transaction. Reads are not.
func TestWritesAreAudited(t *testing.T) {
	c := newClient(t, 1_000)
	alice := c.openAccount()
	ok := c.send("POST", "/v1/transfers", transferBody(c.settlement.String(), alice, 5), uuid.NewString())
	refused := c.send("POST", "/v1/transfers", transferBody(alice, c.settlement.String(), 999), uuid.NewString())
	invalid := c.send("POST", "/v1/transfers", transferBody(alice, alice, 0), uuid.NewString())
	c.do("GET", "/v1/accounts/"+alice, "")

	got := auditRows(t, "partner_id = $1", c.partner)
	want := []struct {
		action string
		status int
		reqID  string
	}{
		{"POST /v1/accounts", 201, ""},
		{"POST /v1/transfers", 201, ok.header.Get("X-Request-Id")},
		{"POST /v1/transfers", 422, refused.header.Get("X-Request-Id")},
		{"POST /v1/transfers", 400, invalid.header.Get("X-Request-Id")},
	}
	if len(got) != len(want) {
		t.Fatalf("%d audit rows, want %d (writes only): %+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.action != w.action || g.status != w.status || (w.reqID != "" && g.reqID != w.reqID) || g.actor != "partner:"+c.partner.String() {
			t.Errorf("row %d = %+v, want %s %d request %q", i, g, w.action, w.status, w.reqID)
		}
	}
}

// metricValue reads one counter from the Prometheus exposition (0 if it isn't there yet).
func metricValue(t *testing.T, name string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	telemetry.AdminHandler(&obs.Health{}).ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), "GET", "/metrics", nil))
	var total float64
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			f := strings.Fields(line)
			v, err := strconv.ParseFloat(f[len(f)-1], 64)
			if err != nil {
				t.Fatal(err)
			}
			total += v
		}
	}
	return total
}

// We audit abuse in aggregate: a flood of 1,000 bad-key requests makes a handful of audit rows (one per key and
// outcome per flush), never one per request, and only the first few even reach the database: after that, the
// address is throttled before any lookup. The key itself is never recorded: only a short fingerprint of its hash.
func TestBadKeyFloodIsAuditedInAggregate(t *testing.T) {
	ctx := context.Background()
	if err := apiServer.FlushAudit(ctx); err != nil { // start from an empty aggregate
		t.Fatal(err)
	}
	var before int
	if err := tdb.Owner.QueryRow(ctx, "SELECT count(*) FROM audit_log").Scan(&before); err != nil {
		t.Fatal(err)
	}
	failuresBefore, throttledBefore := metricValue(t, "ledger_auth_failures_total"), metricValue(t, "ledger_auth_throttled_total")

	secret := "sk_flood-" + uuid.NewString()
	statuses := map[int]int{}
	for range 1000 {
		statuses[client{t: t, key: secret}.send("GET", "/v1/accounts/"+uuid.NewString(), "", "").status]++
	}
	if statuses[401]+statuses[429] != 1000 || statuses[429] == 0 {
		t.Fatalf("answers %v: want only 401s, then 429s once the address is throttled", statuses)
	}
	failures := metricValue(t, "ledger_auth_failures_total") - failuresBefore
	throttled := metricValue(t, "ledger_auth_throttled_total") - throttledBefore
	if failures != float64(statuses[401]) || throttled != float64(statuses[429]) {
		t.Errorf("metrics count %v failures and %v throttled, answers were %v", failures, throttled, statuses)
	}
	if failures > 100 {
		t.Errorf("%v bad-key lookups reached the database; the per-address throttle should stop a flood after ~20", failures)
	}

	if err := apiServer.FlushAudit(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := tdb.Owner.Query(ctx, "SELECT actor, action, status, count FROM audit_log ORDER BY at DESC LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	counted := map[string]int{}
	for rows.Next() {
		var actor, action string
		var status, count int
		if err := rows.Scan(&actor, &action, &status, &count); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(actor, secret) {
			t.Errorf("audit actor %q carries the key", actor)
		}
		counted[action] += count
	}
	var after int
	if err := tdb.Owner.QueryRow(ctx, "SELECT count(*) FROM audit_log").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after-before > 3 {
		t.Errorf("%d audit rows for the flood, want at most a handful", after-before)
	}
	if counted["auth_failure"] != statuses[401] || counted["auth_throttled"] != statuses[429] {
		t.Errorf("aggregated counts %v, answers %v", counted, statuses)
	}
	time.Sleep(2 * time.Second) // let this address's failure bucket refill before other tests use it
}

// The audit log is append-only, like postings: the app can't change it, and the trigger stops even the owner.
func TestAuditLogIsAppendOnly(t *testing.T) {
	ctx := context.Background()
	c := newClient(t, 0)
	c.openAccount()
	for _, stmt := range []string{"UPDATE audit_log SET status = 200 WHERE partner_id = $1", "DELETE FROM audit_log WHERE partner_id = $1"} {
		if _, err := tdb.App.Exec(ctx, stmt, c.partner); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("app %q: err = %v, want permission denied", stmt, err)
		}
		if _, err := tdb.Owner.Exec(ctx, stmt, c.partner); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Errorf("owner %q: err = %v, want the append-only trigger", stmt, err)
		}
	}
}

func TestRequestIDIsEchoedOrMade(t *testing.T) {
	c := newClient(t, 0)
	req, _ := http.NewRequestWithContext(context.Background(), "GET", srv.URL+"/v1/accounts/"+uuid.NewString(), nil)
	req.Header.Set("Authorization", "Bearer "+c.key)
	req.Header.Set("X-Request-Id", "client-trace-42")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got := resp.Header.Get("X-Request-Id"); got != "client-trace-42" {
		t.Errorf("echoed %q", got)
	}
	req.Header.Set("X-Request-Id", `bad id with spaces and a "quote"`)
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if _, err := uuid.Parse(resp.Header.Get("X-Request-Id")); err != nil {
		t.Errorf("an unsafe client id wasn't replaced: %q", resp.Header.Get("X-Request-Id"))
	}
}

// Each partner has its own token bucket; once it's empty, 429 with Retry-After, and other partners are unaffected.
func TestRateLimitIsPerPartner(t *testing.T) {
	ctx := context.Background()
	slow, other := newClient(t, 0), newClient(t, 0)
	if _, err := tdb.Owner.Exec(ctx, "UPDATE partners SET rate_limit_per_min = 6 WHERE id = $1", slow.partner); err != nil {
		t.Fatal(err) // 6 per minute: a burst of 1, then one every 10 s
	}
	path := "/v1/accounts/" + slow.settlement.String()
	if status, _ := slow.do("GET", path, ""); status != http.StatusOK {
		t.Fatalf("first request: %d", status)
	}
	resp := slow.send("GET", path, "", "")
	if resp.status != http.StatusTooManyRequests || !bytes.Contains(resp.body, []byte(`"rate_limited"`)) || resp.header.Get("Retry-After") == "" {
		t.Fatalf("second request: %d %s, Retry-After %q", resp.status, resp.body, resp.header.Get("Retry-After"))
	}
	if status, _ := other.do("GET", "/v1/accounts/"+other.settlement.String(), ""); status != http.StatusOK {
		t.Errorf("another partner was limited too: %d", status)
	}
	// Rate-limited requests are audited in aggregate: one row for the partner with the count.
	for range 5 {
		slow.send("POST", "/v1/accounts", `{"currency":"EUR"}`, uuid.NewString())
	}
	if err := apiServer.FlushAudit(ctx); err != nil {
		t.Fatal(err)
	}
	var rows, count int
	if err := tdb.Owner.QueryRow(ctx, "SELECT count(*), coalesce(sum(count), 0) FROM audit_log WHERE partner_id = $1 AND action = 'rate_limited'",
		slow.partner).Scan(&rows, &count); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || count != 6 { // the GET and the 5 writes
		t.Errorf("%d rate_limited rows counting %d, want 1 row counting 6", rows, count)
	}
}

func TestAmountsAboveTheMaximumAreRefused(t *testing.T) {
	c := newClient(t, 0)
	alice := c.openAccount()
	status, body := c.do("POST", "/v1/transfers", transferBody(c.settlement.String(), alice, 10_000_000_001))
	if status != http.StatusBadRequest || body["code"] != "validation_failed" {
		t.Fatalf("%d %v", status, body)
	}
}

// Scenario 7, tenant isolation: partner B's key can neither read nor move anything of partner A's. Every route
// answers 404, exactly as for an id that doesn't exist, and nothing changes.
func TestTenantIsolation(t *testing.T) {
	a, b := newClient(t, 10_000), newClient(t, 10_000)
	aliceA, shopA := a.openAccount(), a.openAccount()
	a.transfer(a.settlement.String(), aliceA, 500)
	_, h := a.placeHold(aliceA, shopA, 100)
	holdA := h["id"].(string)
	bob := b.openAccount()

	routes := []struct{ method, path, body string }{
		{"GET", "/v1/accounts/" + aliceA, ""},
		{"GET", "/v1/accounts/" + aliceA + "/balance", ""},
		{"GET", "/v1/accounts/" + aliceA + "/statement", ""},
		{"GET", "/v1/holds/" + holdA, ""},
		{"POST", "/v1/transfers", transferBody(aliceA, bob, 1)},
		{"POST", "/v1/transfers", transferBody(bob, aliceA, 1)},
		{"POST", "/v1/holds", `{"account":"` + aliceA + `","to_account":"` + bob + `","amount_minor":1,"currency":"EUR","expires_in":60}`},
		{"POST", "/v1/holds/" + holdA + "/capture", `{"amount_minor":1}`},
		{"POST", "/v1/holds/" + holdA + "/release", ""},
	}
	for _, rt := range routes {
		status, body := b.do(rt.method, rt.path, rt.body)
		if status != http.StatusNotFound || body["code"] != "not_found" {
			t.Errorf("B: %s %s = %d %v, want 404 not_found", rt.method, rt.path, status, body)
		}
	}
	if _, bal := a.do("GET", "/v1/accounts/"+aliceA+"/balance", ""); bal["posted_minor"] != 500.0 || bal["held_minor"] != 100.0 {
		t.Errorf("A's balance changed: %v", bal)
	}
	if _, got := a.do("GET", "/v1/holds/"+holdA, ""); got["status"] != "active" {
		t.Errorf("A's hold changed: %v", got)
	}
}
