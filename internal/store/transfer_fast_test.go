package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store"
)

// The fast path's SQL predicate and ledger.Account.CanSpend must agree exactly: spending precisely the available
// amount down to the floor succeeds on the fast path, and one minor unit more is refused (by the slow path, with the
// Book's error). With a hold, available is posted − held; for a settlement account, the floor is −funding limit.
func TestFastPathPredicateAgreesWithCanSpend(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice, bob, shop := f.open(t, eur), f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 500)
	if _, err := f.store.PlaceHold(ctx, f.partner.ID, alice, shop, ledger.Money{Amount: 120, Currency: eur}, time.Hour); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		from     uuid.UUID
		amount   int64
		accepted bool
	}{
		{"one over available (500 − 120 held)", alice, 381, false},
		{"exactly available", alice, 380, true},
		{"settlement one past its floor (−1,000)", f.settlement, 501, false}, // it is at −500 after funding alice
		{"settlement exactly to its floor", f.settlement, 500, true},
	}
	for _, c := range cases {
		from := c.from
		before := store.SlowTransfers()
		_, err := f.store.Transfer(ctx, f.partner.ID, from, bob, ledger.Money{Amount: c.amount, Currency: eur}, nil)
		slow := store.SlowTransfers() - before
		switch {
		case c.accepted && (err != nil || slow != 0):
			t.Errorf("%s: err %v, slow path %d times; want accepted by the fast path", c.name, err, slow)
		case !c.accepted && (!errors.Is(err, ledger.ErrInsufficientFunds) || slow != 1):
			t.Errorf("%s: err %v, slow path %d times; want insufficient_funds from the slow path", c.name, err, slow)
		}
	}
}
