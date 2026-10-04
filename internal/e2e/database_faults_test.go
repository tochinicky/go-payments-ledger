package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/toxiproxy"

	"github.com/tochinicky/go-payments-ledger/internal/api"
	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store"
)

// apiPartner is a partner with an API key, for tests that go through HTTP.
type apiPartner struct {
	key        string
	id         uuid.UUID
	settlement uuid.UUID
	accounts   []uuid.UUID
}

func newAPIPartner(t *testing.T, customers int) apiPartner {
	t.Helper()
	ctx := context.Background()
	p := apiPartner{key: uuid.NewString()}
	hash := sha256.Sum256([]byte(p.key))
	created, err := store.New(tdb.Owner, store.NewID).CreatePartner(ctx, "faults", hash[:], 1_000_000_000, []ledger.Currency{"EUR"})
	if err != nil {
		t.Fatal(err)
	}
	p.id = created.ID
	if _, err := tdb.Owner.Exec(ctx, "UPDATE partners SET rate_limit_per_min = 1000000 WHERE id = $1", p.id); err != nil {
		t.Fatal(err)
	}
	if err := tdb.Owner.QueryRow(ctx, "SELECT id FROM accounts WHERE partner_id = $1", p.id).Scan(&p.settlement); err != nil {
		t.Fatal(err)
	}
	app := store.New(tdb.App, store.NewID)
	for range customers {
		a, err := app.OpenCustomerAccount(ctx, p.id, "EUR", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := app.Transfer(ctx, p.id, p.settlement, a.ID, ledger.Money{Amount: 1_000_000, Currency: "EUR"}, nil); err != nil {
			t.Fatal(err)
		}
		p.accounts = append(p.accounts, a.ID)
	}
	return p
}

type reply struct {
	status   int
	body     []byte
	replayed bool
}

func (p apiPartner) post(t *testing.T, base, path, body, key string) reply {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), "POST", base+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+p.key)
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply{}
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return reply{status: resp.StatusCode, body: b, replayed: resp.Header.Get("Idempotent-Replayed") == "true"}
}

// checkLedger runs the full reconciliation (invariants 1–4 and 6) over the whole database: after any fault, every
// one must hold. Nothing in this package corrupts rows on purpose, so the report must be completely clean.
func checkLedger(t *testing.T) {
	t.Helper()
	report, err := store.New(tdb.Owner, store.NewID).Reconcile(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range report.Checks {
		if c.Breaks > 0 {
			t.Errorf("reconciliation: %s: %d breaks, e.g. %v", c.Name, c.Breaks, c.Examples)
		}
	}
}

// Scenario 8, commit outcome unknown: the transfer and its idempotency completion really commit, but the
// acknowledgement is "lost" (a faultinject build reports a connection reset right after COMMIT). The client gets
// 503 (never a false failure that's final, never a silent retry by the app); its retry with the same key gets the
// stored 201, because freeing the key after a 5xx is conditional and left the completed key alone. One transaction.
func TestScenario8CommitOutcomeUnknown(t *testing.T) {
	ctx := context.Background()
	p := newAPIPartner(t, 1)
	addr, admin := freeAddr(t), freeAddr(t)
	proc := startFaultProcess(t, "ledger-api", "store.commit_ack_lost",
		"DATABASE_URL="+tdb.AppDSN, "LISTEN_ADDR="+addr, "ADMIN_ADDR="+admin, "SHUTDOWN_DRAIN=0s")
	eventually(t, 20*time.Second, "ledger-api never became ready", func() bool { return httpStatus("http://"+admin+"/readyz") == 200 })

	key := uuid.NewString()
	body := fmt.Sprintf(`{"from":"%s","to":"%s","amount_minor":42,"currency":"EUR"}`, p.settlement, p.accounts[0])
	first := p.post(t, "http://"+addr, "/v1/transfers", body, key)
	if first.status != http.StatusServiceUnavailable || !bytes.Contains(first.body, []byte(`"unavailable"`)) {
		t.Fatalf("first attempt: %d %s, want 503 unavailable", first.status, first.body)
	}
	retry := p.post(t, "http://"+addr, "/v1/transfers", body, key)
	if retry.status != http.StatusCreated || !retry.replayed {
		t.Fatalf("retry: %d %s (replayed %v), want the stored 201", retry.status, retry.body, retry.replayed)
	}
	var n int
	if err := tdb.Owner.QueryRow(ctx, "SELECT count(*) FROM postings WHERE account_id = $1 AND amount_minor = 42", p.accounts[0]).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("%d transfers of 42, want exactly 1", n)
	}
	checkLedger(t)
	_ = proc.Process.Signal(syscall.SIGTERM)
}

// Scenario 6, Postgres connection loss mid-transfer: Toxiproxy resets the app's database connections in bursts
// while transfers run. Each client retries with its key until it gets a final answer. Every answer along the way
// is a 503 or a 409 (never a 500, never a half-applied transfer), every key ends with exactly one transaction, and
// the ledger reconciles.
func TestScenario6PostgresConnectionLoss(t *testing.T) {
	ctx := context.Background()
	dsn, err := url.Parse(tdb.AppDSN)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := toxiproxy.Run(ctx, "ghcr.io/shopify/toxiproxy:2.12.0",
		toxiproxy.WithProxy("postgres", "host.docker.internal:"+dsn.Port()),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway") // Linux CI; built in on Docker Desktop
		}))
	if proxy != nil {
		t.Cleanup(func() { _ = proxy.Terminate(ctx) })
	}
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := proxy.ProxiedEndpoint(8666)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := proxy.URI(ctx)
	if err != nil {
		t.Fatal(err)
	}
	toxics, err := toxiclient.NewClient(uri).Proxy("postgres")
	if err != nil {
		t.Fatal(err)
	}

	viaProxy := *dsn
	viaProxy.Host = net.JoinHostPort(host, port)
	cfg, err := pgxpool.ParseConfig(viaProxy.String())
	if err != nil {
		t.Fatal(err)
	}
	store.DefaultTimeouts.Apply(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	srv := httptest.NewServer(api.New(api.Config{
		Store: store.New(pool, store.NewID), Log: slog.New(slog.DiscardHandler), RequestTimeout: store.DefaultTimeouts.Request,
	}).Handler())
	defer srv.Close()

	p := newAPIPartner(t, 4)
	const workers, perWorker = 8, 15
	var mu sync.Mutex
	interim := map[int]int{}
	var unfinished []string
	var wg sync.WaitGroup
	stop := make(chan struct{})
	go func() { // four bursts of connection resets while the transfers run, then the network heals
		for range 4 {
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Millisecond):
			}
			if _, err := toxics.AddToxic("reset", "reset_peer", "downstream", 1, toxiclient.Attributes{"timeout": 0}); err == nil {
				time.Sleep(300 * time.Millisecond)
				_ = toxics.RemoveToxic("reset")
			}
		}
	}()
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				from, to := p.accounts[(w+i)%len(p.accounts)], p.accounts[(w+i+1)%len(p.accounts)]
				body := fmt.Sprintf(`{"from":"%s","to":"%s","amount_minor":1,"currency":"EUR"}`, from, to)
				key := uuid.NewString()
				deadline := time.Now().Add(60 * time.Second) // a lost release leaves the key leased for up to 30 s
				for {
					r := p.post(t, srv.URL, "/v1/transfers", body, key)
					if r.status == http.StatusCreated {
						break
					}
					mu.Lock()
					interim[r.status]++
					mu.Unlock()
					if time.Now().After(deadline) {
						mu.Lock()
						unfinished = append(unfinished, key)
						mu.Unlock()
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		})
	}
	wg.Wait()
	close(stop)
	_ = toxics.RemoveToxic("reset")

	t.Logf("interim answers before the final 201s: %v", interim)
	for status := range interim {
		if status != http.StatusServiceUnavailable && status != http.StatusConflict && status != 0 {
			t.Errorf("%d answers of %d: only 503 (unavailable), 409 (in progress) or a dropped connection are acceptable",
				interim[status], status)
		}
	}
	if len(unfinished) > 0 {
		t.Errorf("%d transfers never completed", len(unfinished))
	}
	var keys, transfers int
	if err := tdb.Owner.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM idempotency WHERE partner_id = $1 AND status = 'completed'),
		(SELECT count(*) FROM transactions WHERE partner_id = $1 AND kind = 'transfer')`, p.id).Scan(&keys, &transfers); err != nil {
		t.Fatal(err)
	}
	// The funding transfers made directly through the store have no key.
	if want := workers * perWorker; keys != want || transfers != want+len(p.accounts) {
		t.Errorf("%d completed keys and %d transfers, want %d and %d (one transaction per key)", keys, transfers, want, want+len(p.accounts))
	}
	checkLedger(t)
}
