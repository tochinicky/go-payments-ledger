package store_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/events"
	"github.com/tochinicky/go-payments-ledger/internal/ledger"
)

// outboxOf returns an account's events in publish (seq) order.
func outboxOf(t *testing.T, account uuid.UUID) []events.Event {
	t.Helper()
	rows, err := tdb.Owner.Query(context.Background(), "SELECT payload FROM outbox WHERE account_id = $1 ORDER BY seq", account)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []events.Event
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var e events.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Invariant 6: every account entry has exactly one event, written with it. Here every kind of entry happens once,
// and each account's events, in publish order, carry account_seq 1…version with the right types and balances.
func TestEveryEntryWritesOneEvent(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice, shop := f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 500) // alice 1: credited
	captured := f.placeHold(t, alice, shop, 100)
	if _, err := f.store.CaptureHold(ctx, f.partner.ID, captured.ID, 60); err != nil { // alice 2 placed, 3 captured; shop 1 credited
		t.Fatal(err)
	}
	released := f.placeHold(t, alice, shop, 50)                                    // alice 4
	if _, err := f.store.ReleaseHold(ctx, f.partner.ID, released.ID); err != nil { // alice 5
		t.Fatal(err)
	}
	expiring := f.placeHold(t, alice, shop, 70) // alice 6
	backdate(t, expiring.ID)
	if _, err := f.store.ExpireHolds(ctx, 10); err != nil { // alice 7: the job writes its event too
		t.Fatal(err)
	}

	want := []struct {
		eventType       string
		amount          int64
		posted, held    int64
		withTransaction bool
	}{
		{events.AccountCredited, 500, 500, 0, true},
		{events.HoldPlaced, -100, 500, 100, false},
		{events.HoldCaptured, -60, 440, 0, true},
		{events.HoldPlaced, -50, 440, 50, false},
		{events.HoldReleased, 50, 440, 0, false},
		{events.HoldPlaced, -70, 440, 70, false},
		{events.HoldExpired, 70, 440, 0, false},
	}
	got := outboxOf(t, alice)
	if len(got) != len(want) {
		t.Fatalf("%d events for alice, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		e := got[i]
		if e.EventType != w.eventType || e.AmountMinor != w.amount || e.PostedBalanceAfter != w.posted || e.HeldBalanceAfter != w.held ||
			e.AccountSeq != int64(i+1) || (e.TransactionID != nil) != w.withTransaction || e.OccurredAt.IsZero() ||
			e.PartnerID != f.partner.ID || e.SchemaVersion != events.SchemaVersion {
			t.Errorf("event %d = %+v, want %+v with account_seq %d", i, e, w, i+1)
		}
	}
	if v := f.balance(t, alice).Version; v != int64(len(want)) {
		t.Errorf("alice's version %d, but %d events", v, len(want))
	}
	if shopEvents := outboxOf(t, shop); len(shopEvents) != 1 || shopEvents[0].EventType != events.AccountCredited ||
		*shopEvents[0].TransactionID != *got[2].TransactionID {
		t.Errorf("shop events = %+v, want one credit in the capture's transaction", shopEvents)
	}
}

// A refused command writes no event: the outbox only ever describes committed changes.
func TestARefusalWritesNoEvent(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 0)
	alice, bob := f.open(t, eur), f.open(t, eur)
	if _, err := f.store.Transfer(ctx, f.partner.ID, alice, bob, ledger.Money{Amount: 1, Currency: eur}, nil); err == nil {
		t.Fatal("an unfunded transfer succeeded")
	}
	if n := len(outboxOf(t, alice)) + len(outboxOf(t, bob)); n != 0 {
		t.Errorf("%d events after a refusal", n)
	}
}

// The publish order is per-account commit order: many transfers racing on the same accounts, and each account's
// events in seq order have strictly increasing account_seq. (Ids are drawn before the lock; seq is drawn under it.)
func TestOutboxSeqFollowsEachAccountsCommitOrder(t *testing.T) {
	f := newFixture(t, 1_000_000)
	accounts := []uuid.UUID{f.settlement, f.open(t, eur), f.open(t, eur), f.open(t, eur)}
	for _, a := range accounts[1:] {
		f.transfer(t, f.settlement, a, 10_000)
	}
	var wg sync.WaitGroup
	for w := range 20 {
		wg.Go(func() {
			for i := range 25 {
				from, to := accounts[(w+i)%len(accounts)], accounts[(w+i+1)%len(accounts)]
				_, _ = f.store.Transfer(context.Background(), f.partner.ID, from, to, ledger.Money{Amount: 1, Currency: eur}, nil)
			}
		})
	}
	wg.Wait()
	for _, a := range accounts {
		evs := outboxOf(t, a)
		for i, e := range evs {
			if e.AccountSeq != int64(i+1) {
				t.Fatalf("account %s: event %d in seq order has account_seq %d", a, i, e.AccountSeq)
			}
		}
	}
}

// The expiry job never waits on a busy account: with one account locked by a long-running transfer, the other
// accounts' due holds still expire in the same run, and the busy one is expired once the lock is gone.
func TestExpiryIsNotHeldUpByABusyAccount(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 10_000)
	busy, idle, shop := f.open(t, eur), f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, busy, 100)
	f.transfer(t, f.settlement, idle, 100)
	hBusy, hIdle := f.placeHold(t, busy, shop, 10), f.placeHold(t, idle, shop, 10)
	backdate(t, hBusy.ID)
	backdate(t, hIdle.ID)

	blocker, err := tdb.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, "SELECT 1 FROM balances WHERE account_id = $1 FOR UPDATE", busy); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	if _, err := f.store.ExpireHolds(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(began); took > time.Second {
		t.Errorf("the run took %s: it waited on the busy account", took)
	}
	status := func(id uuid.UUID) ledger.HoldStatus {
		h, err := f.store.Hold(ctx, f.partner.ID, id)
		if err != nil {
			t.Fatal(err)
		}
		return h.Status
	}
	if status(hIdle.ID) != ledger.HoldExpired || status(hBusy.ID) != ledger.HoldActive {
		t.Fatalf("idle %s, busy %s: want expired and (for now) active", status(hIdle.ID), status(hBusy.ID))
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ExpireHolds(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if status(hBusy.ID) != ledger.HoldExpired {
		t.Errorf("busy hold still %s after the lock was released", status(hBusy.ID))
	}
}
