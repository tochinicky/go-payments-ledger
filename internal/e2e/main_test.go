// Package e2e tests the event pipeline end to end: the ledger writes its outbox, the relay publishes it to a real
// broker, and the notifier consumes it into its inbox, including the failure scenarios with real process crashes.
package e2e_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tochinicky/go-payments-ledger/internal/events"
	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/notifier"
	"github.com/tochinicky/go-payments-ledger/internal/relay"
	"github.com/tochinicky/go-payments-ledger/internal/store"
	"github.com/tochinicky/go-payments-ledger/internal/testdb"
	"github.com/tochinicky/go-payments-ledger/internal/testkafka"
)

var (
	tdb         *testdb.DB
	kafka       *testkafka.Kafka
	notifierDSN string
	notifierDB  *pgxpool.Pool
	bin         string // directory holding the faultinject builds of cmd/relay and cmd/notifier
)

const faultExit = 86 // faultinject.ExitCode (not importable without the build tag)

// sessionTimeout is the shortest the broker accepts, so the group notices a crashed notifier quickly.
const sessionTimeout = 6 * time.Second

func TestMain(m *testing.M) {
	ctx := context.Background()
	code := testdb.Run(m, &tdb, func() {
		var err error
		if kafka, err = testkafka.Start(ctx); err != nil {
			panic(err)
		}
		if notifierDSN, err = tdb.LoginRole(ctx, "notifier_app", "notifier", "notifier_writer"); err != nil {
			panic(err)
		}
		if notifierDB, err = pgxpool.New(ctx, notifierDSN); err != nil {
			panic(err)
		}
		if bin, err = buildFaultinjectBinaries(); err != nil {
			panic(err)
		}
	})
	if kafka != nil {
		_ = kafka.Terminate(ctx)
	}
	os.Exit(code)
}

// buildFaultinjectBinaries builds the relay and the notifier with their crash points compiled in, and ledger-api
// and relay as production builds (for the shutdown tests, as "<cmd>-prod").
func buildFaultinjectBinaries() (string, error) {
	dir, err := os.MkdirTemp("", "e2e-bin")
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	gomod, err := exec.CommandContext(ctx, "go", "env", "GOMOD").Output()
	if err != nil {
		return "", err
	}
	root := filepath.Dir(strings.TrimSpace(string(gomod)))
	builds := [][]string{
		{"-tags", "faultinject", "-o", filepath.Join(dir, "relay"), "./cmd/relay"},
		{"-tags", "faultinject", "-o", filepath.Join(dir, "notifier"), "./cmd/notifier"},
		{"-o", filepath.Join(dir, "ledger-api-prod"), "./cmd/ledger-api"},
		{"-o", filepath.Join(dir, "relay-prod"), "./cmd/relay"},
	}
	for _, args := range builds {
		build := exec.CommandContext(ctx, "go", append([]string{"build"}, args...)...) //nolint:gosec // fixed arguments

		build.Dir = root
		if out, err := build.CombinedOutput(); err != nil {
			return "", fmt.Errorf("build %v: %w\n%s", args, err, out)
		}
	}
	return dir, nil
}

var logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

// startRelay runs a relay in this process until the test ends.
func startRelay(t *testing.T) {
	t.Helper()
	cl, err := kafka.Client()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = relay.Run(ctx, relay.Config{DatabaseURL: tdb.AppDSN, Kafka: cl, Log: logger, Retry: 200 * time.Millisecond})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		cl.Close()
	})
}

// startNotifier runs a notifier in this process until the test ends.
func startNotifier(t *testing.T) *notifier.Notifier {
	t.Helper()
	n := notifier.New(notifier.Config{Pool: notifierDB, Brokers: []string{kafka.Broker}, SessionTimeout: sessionTimeout, Log: logger})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("notifier: %v", err)
		}
	})
	return n
}

// crash runs a faultinject build until it crashes at point, and fails the test if it exits any other way.
func crash(t *testing.T, cmd, point string, env ...string) {
	t.Helper()
	p := exec.CommandContext(context.Background(), filepath.Join(bin, cmd)) //nolint:gosec // our own test build
	p.Env = append(os.Environ(), append(env, "FAULT_POINT="+point, "KAFKA_BROKERS="+kafka.Broker)...)
	out, err := p.CombinedOutput()
	if p.ProcessState == nil || p.ProcessState.ExitCode() != faultExit {
		t.Fatalf("%s didn't crash at %s (err %v):\n%s", cmd, point, err, out)
	}
}

// partner is one partner's accounts, so each test looks only at its own events.
type partner struct {
	store      *store.Store
	id         uuid.UUID
	settlement uuid.UUID
	accounts   []uuid.UUID // customers, funded
}

func newPartner(t *testing.T, customers int) partner {
	t.Helper()
	ctx := context.Background()
	hash := make([]byte, 32)
	_, _ = rand.Read(hash)
	p, err := store.New(tdb.Owner, store.NewID).CreatePartner(ctx, "e2e", hash, 1_000_000_000, []ledger.Currency{"EUR"})
	if err != nil {
		t.Fatal(err)
	}
	pr := partner{store: store.New(tdb.App, store.NewID), id: p.ID}
	if err := tdb.Owner.QueryRow(ctx, "SELECT id FROM accounts WHERE partner_id = $1", p.ID).Scan(&pr.settlement); err != nil {
		t.Fatal(err)
	}
	for range customers {
		a, err := pr.store.OpenCustomerAccount(ctx, p.ID, "EUR", nil)
		if err != nil {
			t.Fatal(err)
		}
		pr.accounts = append(pr.accounts, a.ID)
		pr.transfer(t, pr.settlement, a.ID, 1_000_000)
	}
	return pr
}

func (p partner) transfer(t *testing.T, from, to uuid.UUID, amount int64) {
	t.Helper()
	if _, err := p.store.Transfer(context.Background(), p.id, from, to, ledger.Money{Amount: amount, Currency: "EUR"}, nil); err != nil {
		t.Fatal(err)
	}
}

// traffic makes n operations across the partner's customers: transfers both ways, and holds placed, captured and
// released, so every kind of event is in the stream.
func (p partner) traffic(t *testing.T, n int) {
	t.Helper()
	ctx := context.Background()
	eur := func(a int64) ledger.Money { return ledger.Money{Amount: a, Currency: "EUR"} }
	for i := range n {
		a, b := p.accounts[i%len(p.accounts)], p.accounts[(i+1)%len(p.accounts)]
		switch i % 4 {
		case 0, 1:
			p.transfer(t, a, b, int64(1+i%7))
		case 2:
			h, err := p.store.PlaceHold(ctx, p.id, a, b, eur(5), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.store.CaptureHold(ctx, p.id, h.ID, 3); err != nil {
				t.Fatal(err)
			}
		case 3:
			h, err := p.store.PlaceHold(ctx, p.id, a, b, eur(5), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.store.ReleaseHold(ctx, p.id, h.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func (p partner) count(t *testing.T, query string) int {
	t.Helper()
	var n int
	if err := tdb.Owner.QueryRow(context.Background(), query, p.id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// eventually polls cond until it holds or the time is up.
func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("after %s: %s", within, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitPublished waits until every one of the partner's events is published.
func (p partner) waitPublished(t *testing.T) {
	t.Helper()
	eventually(t, 60*time.Second, "not every event was published", func() bool {
		return p.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1 AND published_at IS NULL") == 0
	})
}

// waitDelivered waits until every one of the partner's events is published and notified.
func (p partner) waitDelivered(t *testing.T, within time.Duration) {
	t.Helper()
	eventually(t, within, "not every event was published", func() bool {
		return p.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1 AND published_at IS NULL") == 0
	})
	events := p.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1")
	eventually(t, within, "not every event was notified", func() bool {
		return p.count(t, "SELECT count(*) FROM notifier.notifications WHERE partner_id = $1") == events
	})
}

// checkDelivery asserts the end state: exactly one notification per event; per account, notifications carry
// account_seq 1…version with no gap or repeat; and in the topic itself, the first copy of each event arrives in
// account_seq order per account (duplicates may follow; the inbox absorbs them).
func (p partner) checkDelivery(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	var missing, extra int
	if err := tdb.Owner.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM outbox o WHERE o.partner_id = $1
		     AND NOT EXISTS (SELECT 1 FROM notifier.notifications n WHERE n.event_id = o.id)),
		  (SELECT count(*) FROM notifier.notifications n WHERE n.partner_id = $1
		     AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.id = n.event_id))`, p.id).Scan(&missing, &extra); err != nil {
		t.Fatal(err)
	}
	if missing != 0 || extra != 0 {
		t.Errorf("%d events never notified, %d notifications for unknown events", missing, extra)
	}
	var broken int
	if err := tdb.Owner.QueryRow(ctx, `
		SELECT count(*) FROM accounts a JOIN balances b ON b.account_id = a.id
		WHERE a.partner_id = $1 AND (
		  (SELECT count(*) FROM notifier.notifications n WHERE n.account_id = a.id) <> b.version OR
		  (SELECT coalesce(max(account_seq), 0) FROM notifier.notifications n WHERE n.account_id = a.id) <> b.version OR
		  (SELECT coalesce(last_seq, 0) FROM notifier.account_progress pr WHERE pr.account_id = a.id) <> b.version)`, p.id).Scan(&broken); err != nil {
		t.Fatal(err)
	}
	if broken != 0 {
		t.Errorf("%d accounts whose notifications aren't exactly account_seq 1…version", broken)
	}
	p.checkTopicOrder(t)
}

// checkTopicOrder reads the whole topic and checks each of the partner's accounts: the first copy of each event
// arrives in strictly increasing account_seq.
func (p partner) checkTopicOrder(t *testing.T) {
	t.Helper()
	cl, err := kafka.Client(kgo.ConsumeTopics(events.Topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	want := p.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1")
	seen := map[uuid.UUID]bool{}
	last := map[uuid.UUID]int64{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for len(seen) < want {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d events from the topic", len(seen), want)
		}
		for _, rec := range fetches.Records() {
			var e events.Event
			if err := json.Unmarshal(rec.Value, &e); err != nil {
				t.Fatal(err)
			}
			if e.PartnerID != p.id || seen[e.EventID] {
				continue
			}
			seen[e.EventID] = true
			if e.AccountSeq <= last[e.AccountID] {
				t.Fatalf("account %s: account_seq %d arrived after %d", e.AccountID, e.AccountSeq, last[e.AccountID])
			}
			last[e.AccountID] = e.AccountSeq
		}
	}
}

// topicCopies counts the records in the topic that belong to the partner, duplicates included.
func (p partner) topicCopies(t *testing.T) int {
	t.Helper()
	cl, err := kafka.Client(kgo.ConsumeTopics(events.Topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	n, idle := 0, 0
	for idle < 3 { // stop after three empty polls
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		fetches := cl.PollFetches(ctx)
		cancel()
		records := fetches.Records()
		if len(records) == 0 {
			idle++
			continue
		}
		idle = 0
		for _, rec := range records {
			var e events.Event
			if err := json.Unmarshal(rec.Value, &e); err == nil && e.PartnerID == p.id {
				n++
			}
		}
	}
	return n
}
