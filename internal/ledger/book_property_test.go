package ledger

import (
	"errors"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"pgregory.net/rapid"
)

// TestMain raises rapid's default from 100 to 1,000 random sequences per property,
// unless the command line already chose a number (-rapid.checks=…).
func TestMain(m *testing.M) {
	flag.Parse()
	explicit := false
	flag.Visit(func(f *flag.Flag) { explicit = explicit || f.Name == "rapid.checks" })
	if !explicit {
		if err := flag.Set("rapid.checks", "1000"); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}

// expectedErrors are the business refusals random commands can legitimately get. Anything else is a bug.
var expectedErrors = []error{
	ErrInsufficientFunds, ErrSameAccount, ErrNotFound, ErrAccountNotActive, ErrAccountNotEmpty, ErrAccountNotClosable,
	ErrHoldNotActive, ErrHoldNotCapturable, ErrCaptureExceedsHold, ErrInvalidAmount,
}

func expected(err error) bool {
	for _, e := range expectedErrors {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// TestRandomCommandSequencesNeverBreakAnInvariant runs random sequences of transfers, holds, captures, releases,
// expiries and account closures across two partners, and checks after every single step that:
//   - invariants 1–4 hold (CheckInvariants);
//   - money is conserved: within each partner, the posted balances of all its accounts sum to zero;
//   - every refusal is a known business error, never a surprise;
//   - nothing moves between partners.
func TestRandomCommandSequencesNeverBreakAnInvariant(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		book := NewBook(sequentialIDs())
		now := noon
		type partner struct {
			id       uuid.UUID
			accounts []uuid.UUID
		}
		var partners []partner
		for range 2 {
			p := partner{id: uuid.New()}
			limit := rapid.Int64Range(0, 1_000_000).Draw(t, "funding limit")
			s, err := book.OpenAccount(p.id, KindSettlement, "EUR", -limit)
			if err != nil {
				t.Fatal(err)
			}
			p.accounts = append(p.accounts, s.ID)
			for range rapid.IntRange(1, 4).Draw(t, "customers") {
				c, err := book.OpenAccount(p.id, KindCustomer, "EUR", 0)
				if err != nil {
					t.Fatal(err)
				}
				p.accounts = append(p.accounts, c.ID)
			}
			partners = append(partners, p)
		}
		var holds []uuid.UUID

		for step := range rapid.IntRange(1, 60).Draw(t, "steps") {
			p := rapid.SampledFrom(partners).Draw(t, "partner")
			// Mostly the partner's own accounts; sometimes another partner's, which must be refused.
			pickAccount := func(label string) uuid.UUID {
				owner := p
				if rapid.IntRange(0, 9).Draw(t, label+" foreign?") == 0 {
					owner = rapid.SampledFrom(partners).Draw(t, label+" owner")
				}
				return rapid.SampledFrom(owner.accounts).Draw(t, label)
			}
			amount := eur(rapid.Int64Range(-10, 300_000).Draw(t, "amount"))

			var err error
			switch op := rapid.IntRange(0, 6).Draw(t, "op"); op {
			case 0, 1:
				_, err = book.Transfer(p.id, pickAccount("from"), pickAccount("to"), amount)
			case 2:
				var h Hold
				h, err = book.PlaceHold(p.id, pickAccount("from"), pickAccount("to"), amount, now.Add(time.Duration(rapid.IntRange(1, 120).Draw(t, "ttl min"))*time.Minute))
				if err == nil {
					holds = append(holds, h.ID)
				}
			case 3:
				if len(holds) > 0 {
					_, _, err = book.CaptureHold(p.id, rapid.SampledFrom(holds).Draw(t, "hold"), rapid.Int64Range(-5, 300_000).Draw(t, "capture"))
				}
			case 4:
				if len(holds) > 0 {
					_, err = book.ReleaseHold(p.id, rapid.SampledFrom(holds).Draw(t, "hold"))
				}
			case 5:
				now = now.Add(time.Duration(rapid.IntRange(1, 90).Draw(t, "minutes pass")) * time.Minute)
				book.ExpireHolds(now)
			case 6:
				if rapid.IntRange(0, 4).Draw(t, "close?") == 0 {
					id := pickAccount("close")
					err = book.CloseAccount(p.id, id)
					if err == nil {
						if a, balance, _ := book.Account(p.id, id); a.Status != StatusClosed || balance.Posted != 0 || balance.Held != 0 {
							t.Fatalf("step %d: closed account %s isn't empty: %+v", step, id, balance)
						}
					}
				}
			}
			if errors.Is(err, ErrOverflow) || (err != nil && !expected(err)) {
				t.Fatalf("step %d: unexpected error %v", step, err)
			}

			if err := book.CheckInvariants(); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			for _, q := range partners {
				var total int64
				for _, id := range q.accounts {
					total += book.balances[id].Posted
				}
				if total != 0 {
					t.Fatalf("step %d: partner %s's accounts sum to %d: money was created, lost or moved between partners", step, q.id, total)
				}
			}
		}
	})
}
