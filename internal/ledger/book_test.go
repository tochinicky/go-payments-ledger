package ledger

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var noon = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

// sequentialIDs makes deterministic ids (…0001, …0002, …), so a failing test always fails the same way.
func sequentialIDs() func() uuid.UUID {
	var n uint64
	return func() uuid.UUID {
		n++
		var id uuid.UUID
		binary.BigEndian.PutUint64(id[8:], n)
		return id
	}
}

func eur(n int64) Money { return Money{Amount: n, Currency: "EUR"} }

// partnerBook is a book with one partner: a settlement account with a €1,000 funding limit and two customers.
type partnerBook struct {
	*Book
	partner, settlement, alice, bob uuid.UUID
}

func newPartnerBook(t *testing.T) partnerBook {
	t.Helper()
	b := NewBook(sequentialIDs())
	partner := uuid.New()
	open := func(kind AccountKind, floor int64) uuid.UUID {
		a, err := b.OpenAccount(partner, kind, "EUR", floor)
		if err != nil {
			t.Fatal(err)
		}
		return a.ID
	}
	return partnerBook{Book: b, partner: partner, settlement: open(KindSettlement, -100_000), alice: open(KindCustomer, 0), bob: open(KindCustomer, 0)}
}

func (p partnerBook) fund(t *testing.T, to uuid.UUID, amount int64) {
	t.Helper()
	if _, err := p.Transfer(p.partner, p.settlement, to, eur(amount)); err != nil {
		t.Fatal(err)
	}
}

func (p partnerBook) balance(t *testing.T, id uuid.UUID) Balance {
	t.Helper()
	_, b, err := p.Account(p.partner, id)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// must turns a (value, error) result into the value, failing the test on an error. Go only lets a call's
// multiple results be passed straight on when they are *all* of the next call's arguments, so must takes t
// first and returns a function that takes exactly (T, error): must[Hold](t)(book.PlaceHold(...)).
func must[T any](t *testing.T) func(T, error) T {
	return func(v T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func TestFundingComesFromTheSettlementAccountUpToTheFundingLimit(t *testing.T) {
	p := newPartnerBook(t)

	p.fund(t, p.alice, 60_000)
	p.fund(t, p.bob, 40_000) // exactly the €1,000 limit

	if got := p.balance(t, p.settlement).Posted; got != -100_000 {
		t.Errorf("settlement posted %d, want −100000: it mirrors what the customers hold", got)
	}
	_, err := p.Transfer(p.partner, p.settlement, p.alice, eur(1))
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("one cent past the funding limit: %v, want ErrInsufficientFunds (a partner can't mint money)", err)
	}
}

func TestACustomerCantGoBelowZero(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 500)

	if _, err := p.Transfer(p.partner, p.alice, p.bob, eur(501)); !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("501 from 500: %v, want ErrInsufficientFunds", err)
	}
	if _, err := p.Transfer(p.partner, p.alice, p.bob, eur(500)); err != nil {
		t.Errorf("all of it: %v", err)
	}
}

func TestTransferPostsTwoBalancedPostingsAndBumpsBothVersions(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 1_000)

	tx, err := p.Transfer(p.partner, p.alice, p.bob, eur(300))

	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Postings) != 2 || tx.Postings[0].Amount.Amount != -300 || tx.Postings[1].Amount.Amount != 300 {
		t.Errorf("postings %+v, want −300 / +300", tx.Postings)
	}
	if a, b := p.balance(t, p.alice), p.balance(t, p.bob); a.Posted != 700 || b.Posted != 300 || a.Version != 2 || b.Version != 1 {
		t.Errorf("alice %+v, bob %+v", a, b)
	}
}

func TestTransferRules(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 1_000)
	// Another partner's account in the same book (a separate Book would reuse the same sequential ids).
	foreign, err := p.OpenAccount(uuid.New(), KindCustomer, "EUR", 0)
	if err != nil {
		t.Fatal(err)
	}
	usd, err := p.OpenAccount(p.partner, KindCustomer, "USD", 0)
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]struct {
		from, to uuid.UUID
		amount   Money
		want     error
	}{
		"zero":                      {p.alice, p.bob, eur(0), ErrInvalidAmount},
		"negative":                  {p.alice, p.bob, eur(-5), ErrInvalidAmount},
		"to itself":                 {p.alice, p.alice, eur(5), ErrSameAccount},
		"wrong currency":            {p.alice, usd.ID, eur(5), ErrCurrencyMismatch},
		"unknown account":           {p.alice, uuid.New(), eur(5), ErrNotFound},
		"another partner's account": {p.alice, foreign.ID, eur(5), ErrNotFound},
	}
	for name, tt := range tests {
		if _, err := p.Transfer(p.partner, tt.from, tt.to, tt.amount); !errors.Is(err, tt.want) {
			t.Errorf("%s: %v, want %v", name, err, tt.want)
		}
	}
}

func TestAnotherPartnerSeesNotFoundExactlyLikeAMissingAccount(t *testing.T) {
	p := newPartnerBook(t)
	intruder := uuid.New()

	_, _, errOthers := p.Account(intruder, p.alice)
	_, _, errMissing := p.Account(intruder, uuid.New())

	if !errors.Is(errOthers, ErrNotFound) || errOthers.Error() != errMissing.Error() {
		t.Errorf("another partner's account: %v; a missing one: %v; they must be indistinguishable", errOthers, errMissing)
	}
}

func TestOnlyAnEmptyCustomerAccountCanBeClosed(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 1_000)

	if err := p.CloseAccount(p.partner, p.alice); !errors.Is(err, ErrAccountNotEmpty) {
		t.Errorf("funded: %v, want ErrAccountNotEmpty (the money would be stranded)", err)
	}
	if err := p.CloseAccount(p.partner, p.settlement); !errors.Is(err, ErrAccountNotClosable) {
		t.Errorf("settlement: %v, want ErrAccountNotClosable", err)
	}

	// Move everything out but keep a hold: still not empty.
	h := must[Hold](t)(p.PlaceHold(p.partner, p.alice, p.bob, eur(400), noon.Add(time.Hour)))
	if _, err := p.Transfer(p.partner, p.alice, p.bob, eur(600)); err != nil {
		t.Fatal(err)
	}
	if err := p.CloseAccount(p.partner, p.alice); !errors.Is(err, ErrAccountNotEmpty) {
		t.Errorf("an active hold: %v, want ErrAccountNotEmpty (the hold could still be captured)", err)
	}

	if _, _, err := p.CaptureHold(p.partner, h.ID, 400); err != nil {
		t.Fatal(err)
	}
	if err := p.CloseAccount(p.partner, p.alice); err != nil {
		t.Errorf("empty: %v", err)
	}
	if err := p.CloseAccount(p.partner, p.alice); err != nil {
		t.Errorf("closing again is a no-op: %v", err)
	}
}

func TestAClosedAccountCantSendOrReceive(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 1_000)
	if err := p.CloseAccount(p.partner, p.bob); err != nil {
		t.Fatal(err)
	}

	if _, err := p.Transfer(p.partner, p.alice, p.bob, eur(1)); !errors.Is(err, ErrAccountNotActive) {
		t.Errorf("%v, want ErrAccountNotActive", err)
	}
}

func TestAHoldReservesMoneyThatCanThenNotBeSpentTwice(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 1_000)

	must[Hold](t)(p.PlaceHold(p.partner, p.alice, p.bob, eur(800), noon.Add(time.Hour)))

	if _, err := p.Transfer(p.partner, p.alice, p.bob, eur(201)); !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("spending reserved money: %v, want ErrInsufficientFunds", err)
	}
	if _, err := p.PlaceHold(p.partner, p.alice, p.bob, eur(201), noon.Add(time.Hour)); !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("holding reserved money: %v, want ErrInsufficientFunds", err)
	}
	if b := p.balance(t, p.alice); b.Posted != 1_000 || b.Held != 800 {
		t.Errorf("alice %+v: a hold reserves, it doesn't post", b)
	}
}

func TestAPartialCaptureMovesThePartAndReleasesTheRest(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 1_000)
	h := must[Hold](t)(p.PlaceHold(p.partner, p.alice, p.bob, eur(800), noon.Add(time.Hour)))

	captured, tx, err := p.CaptureHold(p.partner, h.ID, 500)

	if err != nil {
		t.Fatal(err)
	}
	if captured.Status != HoldCaptured || tx.Kind != KindHoldCapture {
		t.Errorf("hold %+v, tx %+v", captured, tx)
	}
	a, b := p.balance(t, p.alice), p.balance(t, p.bob)
	if a.Posted != 500 || a.Held != 0 || b.Posted != 500 {
		t.Errorf("alice %+v, bob %+v: 500 moved, the other 300 is free again", a, b)
	}
}

func TestAHoldEndsExactlyOnce(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 1_000)
	h := must[Hold](t)(p.PlaceHold(p.partner, p.alice, p.bob, eur(500), noon.Add(time.Hour)))
	if _, _, err := p.CaptureHold(p.partner, h.ID, 500); err != nil {
		t.Fatal(err)
	}

	if _, _, err := p.CaptureHold(p.partner, h.ID, 500); !errors.Is(err, ErrHoldNotActive) {
		t.Errorf("second capture: %v, want ErrHoldNotActive (invariant 4)", err)
	}
	if _, err := p.ReleaseHold(p.partner, h.ID); !errors.Is(err, ErrHoldNotActive) {
		t.Errorf("release after capture: %v, want ErrHoldNotActive", err)
	}
	if p.balance(t, p.bob).Posted != 500 {
		t.Error("the money moved once")
	}
}

func TestCaptureRules(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 1_000)
	h := must[Hold](t)(p.PlaceHold(p.partner, p.alice, p.bob, eur(500), noon.Add(time.Hour)))

	if _, _, err := p.CaptureHold(p.partner, h.ID, 501); !errors.Is(err, ErrCaptureExceedsHold) {
		t.Errorf("more than the hold: %v", err)
	}
	if _, _, err := p.CaptureHold(uuid.New(), h.ID, 100); !errors.Is(err, ErrNotFound) {
		t.Errorf("another partner's hold: %v", err)
	}

	if err := p.CloseAccount(p.partner, p.bob); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.CaptureHold(p.partner, h.ID, 100); !errors.Is(err, ErrHoldNotCapturable) {
		t.Errorf("destination closed: %v, want ErrHoldNotCapturable", err)
	}
	if b := p.balance(t, p.alice); b.Held != 500 {
		t.Errorf("after a refused capture the hold is still active and still reserves 500, got held %d", b.Held)
	}
}

func TestReleaseAndExpiryFreeTheReservation(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 1_000)
	released := must[Hold](t)(p.PlaceHold(p.partner, p.alice, p.bob, eur(300), noon.Add(time.Hour)))
	must[Hold](t)(p.PlaceHold(p.partner, p.alice, p.bob, eur(200), noon.Add(time.Minute)))
	must[Hold](t)(p.PlaceHold(p.partner, p.alice, p.bob, eur(100), noon.Add(2*time.Hour)))

	if _, err := p.ReleaseHold(p.partner, released.ID); err != nil {
		t.Fatal(err)
	}
	expired := p.ExpireHolds(noon.Add(time.Hour))

	if expired != 1 {
		t.Errorf("expired %d holds at 13:00, want 1 (the 12:01 one; the 12:00+1h one was released, the 14:00 one runs on)", expired)
	}
	if b := p.balance(t, p.alice); b.Held != 100 || b.Posted != 1_000 {
		t.Errorf("alice %+v", b)
	}
	if err := p.CheckInvariants(); err != nil {
		t.Error(err)
	}
}

func TestCheckInvariantsCatchesACorruptedBalance(t *testing.T) {
	p := newPartnerBook(t)
	p.fund(t, p.alice, 1_000)

	// What a bug or a manual UPDATE in the database would do: change a balance without a posting.
	b := p.balances[p.alice]
	b.Posted += 1
	p.balances[p.alice] = b

	if err := p.CheckInvariants(); err == nil {
		t.Error("money appeared from nowhere and the check didn't notice")
	}
}
