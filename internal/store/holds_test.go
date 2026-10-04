package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store"
)

func (f fixture) placeHold(t *testing.T, from, to uuid.UUID, amount int64) store.Hold {
	t.Helper()
	h, err := f.store.PlaceHold(context.Background(), f.partner.ID, from, to, ledger.Money{Amount: amount, Currency: eur}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// backdate makes a hold's time be up, as if it had been placed long ago.
func backdate(t *testing.T, holdID uuid.UUID) {
	t.Helper()
	if _, err := tdb.Owner.Exec(context.Background(), "UPDATE holds SET expires_at = now() - interval '1 second' WHERE id = $1", holdID); err != nil {
		t.Fatal(err)
	}
}

func TestPlaceHoldReservesWithoutPosting(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice, shop := f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 100)

	h := f.placeHold(t, alice, shop, 60)
	if h.Status != ledger.HoldActive || h.Amount.Amount != 60 || h.ToAccountID != shop {
		t.Fatalf("hold = %+v", h)
	}
	if got := f.balance(t, alice); got != (ledger.Balance{Posted: 100, Held: 60, Version: 2}) {
		t.Errorf("alice = %+v, want posted 100, held 60, version 2", got)
	}
	tests := []struct {
		name     string
		from, to uuid.UUID
		amount   ledger.Money
		want     error
	}{
		{"more than available", alice, shop, ledger.Money{Amount: 41, Currency: eur}, ledger.ErrInsufficientFunds},
		{"same account", alice, alice, ledger.Money{Amount: 1, Currency: eur}, ledger.ErrSameAccount},
		{"wrong currency", alice, shop, ledger.Money{Amount: 1, Currency: "USD"}, ledger.ErrCurrencyMismatch},
		{"another partner's destination", alice, newFixture(t, 0).settlement, ledger.Money{Amount: 1, Currency: eur}, ledger.ErrNotFound},
		{"zero", alice, shop, ledger.Money{Amount: 0, Currency: eur}, ledger.ErrInvalidAmount},
	}
	for _, tt := range tests {
		if _, err := f.store.PlaceHold(ctx, f.partner.ID, tt.from, tt.to, tt.amount, time.Hour); !errors.Is(err, tt.want) {
			t.Errorf("%s: err = %v, want %v", tt.name, err, tt.want)
		}
	}
}

// A partial capture posts the captured part and releases the rest. The statement and the version agree: placing
// the hold took version 2 (no posting), the capture's entry is version 3 and carries account_seq 3.
func TestCapturePostsThePartAndReleasesTheRest(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice, shop := f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 100)
	h := f.placeHold(t, alice, shop, 60)

	captured, err := f.store.CaptureHold(ctx, f.partner.ID, h.ID, 45)
	if err != nil {
		t.Fatal(err)
	}
	if captured.Status != ledger.HoldCaptured || captured.Captured != 45 || captured.TransactionID == nil {
		t.Fatalf("captured = %+v", captured)
	}
	if got := f.balance(t, alice); got != (ledger.Balance{Posted: 55, Held: 0, Version: 3}) {
		t.Errorf("alice = %+v, want posted 55, held 0, version 3", got)
	}
	if got := f.balance(t, shop); got != (ledger.Balance{Posted: 45, Version: 1}) {
		t.Errorf("shop = %+v, want posted 45, version 1", got)
	}
	page, err := f.store.Statement(ctx, f.partner.ID, alice, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || page[1].AccountSeq != 3 || page[1].Kind != ledger.KindHoldCapture || page[1].TransactionID != *captured.TransactionID {
		t.Fatalf("statement = %+v, want the capture as account_seq 3", page)
	}

	for name, err := range map[string]error{
		"capture twice":         second(f.store.CaptureHold(ctx, f.partner.ID, h.ID, 1)),
		"release after capture": second(f.store.ReleaseHold(ctx, f.partner.ID, h.ID)),
	} {
		if !errors.Is(err, ledger.ErrHoldNotActive) {
			t.Errorf("%s: err = %v, want hold_not_active", name, err)
		}
	}
}

func second[T any](_ T, err error) error { return err }

func TestCaptureRefusals(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice, shop := f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 100)
	h := f.placeHold(t, alice, shop, 60)

	if err := second(f.store.CaptureHold(ctx, f.partner.ID, h.ID, 61)); !errors.Is(err, ledger.ErrCaptureExceedsHold) {
		t.Errorf("more than the hold: err = %v", err)
	}
	if err := second(f.store.CaptureHold(ctx, f.partner.ID, h.ID, 0)); !errors.Is(err, ledger.ErrInvalidAmount) {
		t.Errorf("zero: err = %v", err)
	}
	if err := second(f.store.CaptureHold(ctx, newFixture(t, 0).partner.ID, h.ID, 1)); !errors.Is(err, ledger.ErrNotFound) {
		t.Errorf("another partner's hold: err = %v", err)
	}
	if _, err := tdb.Owner.Exec(ctx, "UPDATE accounts SET status = 'closed' WHERE id = $1", shop); err != nil {
		t.Fatal(err)
	}
	if err := second(f.store.CaptureHold(ctx, f.partner.ID, h.ID, 1)); !errors.Is(err, ledger.ErrHoldNotCapturable) {
		t.Errorf("destination closed: err = %v", err)
	}
	if got, err := f.store.Hold(ctx, f.partner.ID, h.ID); err != nil || got.Status != ledger.HoldActive {
		t.Errorf("after the refusals the hold is %+v (%v), want still active", got, err)
	}
	if got := f.balance(t, alice); got.Held != 60 || got.Posted != 100 {
		t.Errorf("alice = %+v, want the reservation intact", got)
	}
}

// A hold whose time is up can't be captured or released, even before the expiry job has ended it.
func TestAHoldPastItsExpiryIsNoLongerActive(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice, shop := f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 100)
	h := f.placeHold(t, alice, shop, 60)
	backdate(t, h.ID)
	if err := second(f.store.CaptureHold(ctx, f.partner.ID, h.ID, 1)); !errors.Is(err, ledger.ErrHoldNotActive) {
		t.Errorf("capture: err = %v, want hold_not_active", err)
	}
	if err := second(f.store.ReleaseHold(ctx, f.partner.ID, h.ID)); !errors.Is(err, ledger.ErrHoldNotActive) {
		t.Errorf("release: err = %v, want hold_not_active", err)
	}
}

func TestReleaseGivesTheReservationBack(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice, shop := f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 100)
	h := f.placeHold(t, alice, shop, 60)
	released, err := f.store.ReleaseHold(ctx, f.partner.ID, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if released.Status != ledger.HoldReleased || released.Captured != 0 {
		t.Errorf("released = %+v", released)
	}
	if got := f.balance(t, alice); got != (ledger.Balance{Posted: 100, Held: 0, Version: 3}) {
		t.Errorf("alice = %+v, want posted 100, held 0, version 3", got)
	}
}

// The expiry job ends every due hold, across batches, and leaves the others alone.
func TestExpireHoldsEndsDueHoldsOnly(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 10_000)
	alice, shop := f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 1_000)
	var due []store.Hold
	for range 5 {
		h := f.placeHold(t, alice, shop, 10)
		backdate(t, h.ID)
		due = append(due, h)
	}
	live := f.placeHold(t, alice, shop, 10)

	if _, err := f.store.ExpireHolds(ctx, 2); err != nil { // batches of 2: the loop must keep going
		t.Fatal(err)
	}
	for _, h := range due {
		if got, _ := f.store.Hold(ctx, f.partner.ID, h.ID); got.Status != ledger.HoldExpired {
			t.Errorf("due hold %s is %s, want expired", h.ID, got.Status)
		}
	}
	if got, _ := f.store.Hold(ctx, f.partner.ID, live.ID); got.Status != ledger.HoldActive {
		t.Errorf("live hold is %s, want active", got.Status)
	}
	if got := f.balance(t, alice); got.Held != 10 || got.Version != 1+6+5 {
		t.Errorf("alice = %+v, want held 10 (the live hold) and one version per operation", got)
	}
}

// A capture and the expiry job racing for the same hold: exactly one of them ends it.
func TestCaptureAndExpiryNeverBothEndAHold(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 100_000)
	alice, shop := f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 50_000)
	for range 20 {
		h := f.placeHold(t, alice, shop, 100)
		// Due in a moment: the capture may see it active, or the job may get there first.
		if _, err := tdb.Owner.Exec(ctx, "UPDATE holds SET expires_at = now() + interval '20 milliseconds' WHERE id = $1", h.ID); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var captureErr, expireErr error
		wg.Go(func() { _, captureErr = f.store.CaptureHold(ctx, f.partner.ID, h.ID, 100) })
		wg.Go(func() {
			time.Sleep(20 * time.Millisecond)
			_, expireErr = f.store.ExpireHolds(ctx, 10)
		})
		wg.Wait()
		if expireErr != nil || (captureErr != nil && !errors.Is(captureErr, ledger.ErrHoldNotActive)) {
			t.Fatalf("capture: %v, expiry: %v", captureErr, expireErr)
		}
		got, _ := f.store.Hold(ctx, f.partner.ID, h.ID)
		if (captureErr == nil) != (got.Status == ledger.HoldCaptured) {
			t.Fatalf("capture returned %v but the hold is %s", captureErr, got.Status)
		}
	}
	if got := f.balance(t, alice); got.Held != 0 || got.Posted+f.balance(t, shop).Posted != 50_000 {
		t.Errorf("alice %+v, shop %+v: money was created, lost or left reserved", got, f.balance(t, shop))
	}
}

// Invariant 4 in the database: a hold ends once. Even the owner can't end it a second time.
func TestAnEndedHoldCannotBeChanged(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice, shop := f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 100)
	h := f.placeHold(t, alice, shop, 60)
	if _, err := f.store.ReleaseHold(ctx, f.partner.ID, h.ID); err != nil {
		t.Fatal(err)
	}
	_, err := tdb.Owner.Exec(ctx, "UPDATE holds SET status = 'active', ended_at = NULL WHERE id = $1", h.ID)
	if code := pgCode(err); code != "23001" {
		t.Fatalf("reviving a released hold: err = %v, want the hold trigger", err)
	}
}
