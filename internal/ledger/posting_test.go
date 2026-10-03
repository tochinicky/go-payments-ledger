package ledger

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestValidatePostings(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	eur := func(id uuid.UUID, n int64) Posting { return Posting{AccountID: id, Amount: Money{n, "EUR"}} }
	usd := func(id uuid.UUID, n int64) Posting { return Posting{AccountID: id, Amount: Money{n, "USD"}} }

	valid := map[string][]Posting{
		"a transfer":                    {eur(a, -500), eur(b, 500)},
		"a split":                       {eur(a, -500), eur(b, 300), eur(c, 200)},
		"two currencies, each balanced": {eur(a, -500), eur(b, 500), usd(a, 70), usd(c, -70)},
	}
	for name, postings := range valid {
		if err := ValidatePostings(postings); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}

	invalid := map[string][]Posting{
		"money from nowhere":             {eur(a, 500), eur(b, 500)},
		"one posting":                    {eur(a, 0)},
		"a zero posting":                 {eur(a, 0), eur(b, 0)},
		"balanced only across EUR + USD": {eur(a, -500), usd(b, 500)},
	}
	for name, postings := range invalid {
		if err := ValidatePostings(postings); !errors.Is(err, ErrUnbalanced) {
			t.Errorf("%s: %v, want ErrUnbalanced", name, err)
		}
	}
}
