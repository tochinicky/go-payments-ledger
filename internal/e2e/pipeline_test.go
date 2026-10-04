package e2e_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

// The happy path: every event is published and notified exactly once, in per-account order, with no gaps.
func TestPipelineDeliversEveryEventOnceInOrder(t *testing.T) {
	startRelay(t)
	n := startNotifier(t)
	p := newPartner(t, 5)
	var wg sync.WaitGroup
	for range 4 { // concurrent traffic on the same accounts
		wg.Go(func() { p.traffic(t, 50) })
	}
	wg.Wait()
	p.waitDelivered(t, 60*time.Second)
	p.checkDelivery(t)
	if s := n.Stats(); s.Gaps != 0 || s.Regressions != 0 {
		t.Errorf("notifier stats %+v: the stream had gaps or regressions", s)
	}
}

// The notifier's role can't read the ledger's tables.
func TestNotifierRoleCannotReadTheLedger(t *testing.T) {
	_, err := notifierDB.Exec(context.Background(), "SELECT 1 FROM public.balances LIMIT 1")
	if err == nil {
		t.Fatal("the notifier role read the ledger's balances")
	}
}

// One active relay: two run, one leads. Kill the leader's lock connection (as if its host died): the standby takes
// over, and the stream stays complete and in order. Duplicates are allowed; the inbox absorbs them.
func TestRelayFailover(t *testing.T) {
	ctx := context.Background()
	startRelay(t)
	startRelay(t)
	n := startNotifier(t)
	p := newPartner(t, 4)
	leader := func() int {
		var pid int
		if err := tdb.Owner.QueryRow(ctx, "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted").Scan(&pid); err != nil {
			return 0
		}
		return pid
	}
	eventually(t, 10*time.Second, "no relay took the lead", func() bool { return leader() != 0 })
	first := leader()

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.traffic(t, 200)
	}()
	time.Sleep(200 * time.Millisecond)
	if _, err := tdb.Owner.Exec(ctx, "SELECT pg_terminate_backend($1)", first); err != nil {
		t.Fatal(err)
	}
	<-done
	eventually(t, 10*time.Second, "no standby took over", func() bool { l := leader(); return l != 0 && l != first })
	p.waitDelivered(t, 60*time.Second)
	p.checkDelivery(t)
	// A failover can re-send rows the old leader was still publishing, so an older account_seq may arrive after a
	// newer one. The inbox check comes first, so a re-sent event is a duplicate, never a regression or a gap.
	if s := n.Stats(); s.Regressions != 0 || s.Gaps != 0 {
		t.Errorf("notifier stats %+v: duplicates from the failover were counted as regressions or gaps", s)
	}
}

// Fairness: one hot account under sustained load mustn't delay everyone else. The relay publishes in seq order,
// so every account's entries wait only for what committed before them; p99 of publish delay for the other
// accounts stays under a second.
func TestHotAccountDoesNotStarveOthers(t *testing.T) {
	startRelay(t)
	startNotifier(t) // consume everything, so later tests start with the group caught up
	hot := newPartner(t, 2)
	others := newPartner(t, 6)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if (i+w)%2 == 0 {
					hot.transfer(t, hot.accounts[0], hot.accounts[1], 1)
				} else {
					hot.transfer(t, hot.accounts[1], hot.accounts[0], 1)
				}
			}
		})
	}
	others.traffic(t, 100)
	close(stop)
	wg.Wait()
	eventually(t, 60*time.Second, "not every event was published", func() bool {
		return others.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1 AND published_at IS NULL") == 0
	})
	rows, err := tdb.Owner.Query(context.Background(),
		"SELECT extract(epoch FROM published_at - created_at) FROM outbox WHERE partner_id = $1", others.id)
	if err != nil {
		t.Fatal(err)
	}
	var delays []float64
	for rows.Next() {
		var d float64
		if err := rows.Scan(&d); err != nil {
			t.Fatal(err)
		}
		delays = append(delays, d)
	}
	rows.Close()
	sort.Float64s(delays)
	p99 := delays[len(delays)*99/100]
	t.Logf("%d events from other accounts alongside %d hot ones; publish delay p50 %.3fs, p99 %.3fs",
		len(delays), hot.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1"), delays[len(delays)/2], p99)
	if p99 >= 1 {
		t.Errorf("p99 publish delay %.3fs for the other accounts, want under 1s", p99)
	}
	hot.waitDelivered(t, 120*time.Second)
	others.waitDelivered(t, 120*time.Second)
}
