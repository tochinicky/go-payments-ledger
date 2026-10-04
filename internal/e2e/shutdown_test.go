package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// startProcess starts one of the production builds; its output goes to the test log on failure.
func startProcess(t *testing.T, name string, env ...string) *exec.Cmd {
	t.Helper()
	var out bytes.Buffer
	p := exec.CommandContext(context.Background(), filepath.Join(bin, name)) //nolint:gosec // our own test build
	p.Env = append(os.Environ(), env...)
	p.Stdout, p.Stderr = &out, &out
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if p.ProcessState == nil {
			_ = p.Process.Kill()
			_ = p.Wait()
		}
		if t.Failed() {
			t.Logf("%s output:\n%s", name, out.String())
		}
	})
	return p
}

// startFaultProcess starts a faultinject build with FAULT_POINT set.
func startFaultProcess(t *testing.T, name, point string, env ...string) *exec.Cmd {
	t.Helper()
	return startProcess(t, name, append(env, "FAULT_POINT="+point)...)
}

func waitExit(t *testing.T, p *exec.Cmd, within time.Duration) (code int, took time.Duration) {
	t.Helper()
	began := time.Now()
	done := make(chan struct{})
	go func() { _ = p.Wait(); close(done) }()
	select {
	case <-done:
		return p.ProcessState.ExitCode(), time.Since(began)
	case <-time.After(within):
		t.Fatalf("still running %s after the stop signal", within)
		return 0, 0
	}
}

func httpStatus(url string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// Graceful shutdown of ledger-api, with a real SIGTERM: readiness fails at once (traffic moves away), a request
// already in flight still completes, and then the process exits cleanly.
func TestLedgerAPIShutsDownGracefully(t *testing.T) {
	ctx := context.Background()
	key := uuid.NewString()
	hash := sha256.Sum256([]byte(key))
	p, err := store.New(tdb.Owner, store.NewID).CreatePartner(ctx, "shutdown", hash[:], 1_000, []ledger.Currency{"EUR"})
	if err != nil {
		t.Fatal(err)
	}
	var settlement uuid.UUID
	if err := tdb.Owner.QueryRow(ctx, "SELECT id FROM accounts WHERE partner_id = $1", p.ID).Scan(&settlement); err != nil {
		t.Fatal(err)
	}
	alice, err := store.New(tdb.App, store.NewID).OpenCustomerAccount(ctx, p.ID, "EUR", nil)
	if err != nil {
		t.Fatal(err)
	}

	api, admin := freeAddr(t), freeAddr(t)
	proc := startProcess(t, "ledger-api-prod", "DATABASE_URL="+tdb.AppDSN, "LISTEN_ADDR="+api, "ADMIN_ADDR="+admin, "SHUTDOWN_DRAIN=1s")
	eventually(t, 20*time.Second, "ledger-api never became ready", func() bool { return httpStatus("http://"+admin+"/readyz") == 200 })

	// Park a transfer inside its transaction: the test holds the settlement balance row.
	blocker, err := tdb.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, "SELECT 1 FROM balances WHERE account_id = $1 FOR UPDATE", settlement); err != nil {
		t.Fatal(err)
	}
	result := make(chan int, 1)
	go func() {
		body := fmt.Sprintf(`{"from":"%s","to":"%s","amount_minor":10,"currency":"EUR"}`, settlement, alice.ID)
		req, _ := http.NewRequestWithContext(ctx, "POST", "http://"+api+"/v1/transfers", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Idempotency-Key", uuid.NewString())
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			result <- 0
			return
		}
		_ = resp.Body.Close()
		result <- resp.StatusCode
	}()
	time.Sleep(300 * time.Millisecond) // the request is now waiting on the lock

	if err := proc.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, "readiness didn't fail on SIGTERM", func() bool { return httpStatus("http://"+admin+"/readyz") == 503 })
	if httpStatus("http://"+admin+"/healthz") != 200 {
		t.Error("liveness failed during the drain")
	}
	time.Sleep(1500 * time.Millisecond) // past the drain: the server has stopped accepting, the request is still in flight
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if status := <-result; status != http.StatusCreated {
		t.Errorf("the in-flight request got %d, want 201", status)
	}
	if code, _ := waitExit(t, proc, 20*time.Second); code != 0 {
		t.Errorf("exit code %d, want 0", code)
	}
	var n int
	if err := tdb.Owner.QueryRow(ctx, "SELECT count(*) FROM postings WHERE account_id = $1", alice.ID).Scan(&n); err != nil || n != 1 {
		t.Errorf("%d postings to alice (%v), want the one in-flight transfer", n, err)
	}
}

// The relay's stop is bounded even with Kafka down: a publish already sent can't be cancelled, so after
// SHUTDOWN_TIMEOUT it exits anyway; the unmarked rows are published by the next leader.
func TestRelayStopIsBoundedWhileKafkaIsDown(t *testing.T) {
	ctx := context.Background()
	p := newPartner(t, 2)
	proc := startProcess(t, "relay-prod", "DATABASE_URL="+tdb.AppDSN, "KAFKA_BROKERS="+kafka.Broker,
		"ADMIN_ADDR="+freeAddr(t), "SHUTDOWN_TIMEOUT=3s")
	p.waitPublished(t)

	if err := kafka.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := kafka.Restart(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	p.traffic(t, 10) // rows the relay is now stuck publishing
	time.Sleep(time.Second)
	if err := proc.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	code, took := waitExit(t, proc, 10*time.Second)
	t.Logf("relay stopped %s after SIGTERM with Kafka down (exit %d)", took.Round(100*time.Millisecond), code)
	if code != 0 {
		t.Errorf("exit code %d, want 0", code)
	}
}
