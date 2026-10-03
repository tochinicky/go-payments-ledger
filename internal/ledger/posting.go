package ledger

import (
	"fmt"

	"github.com/google/uuid"
)

// Posting is one line of a transaction: an amount added to (positive) or taken from (negative) one account.
type Posting struct {
	AccountID uuid.UUID
	Amount    Money
}

// TransactionKind says what a transaction did.
type TransactionKind string

// The transaction kinds the ledger writes.
const (
	KindTransfer    TransactionKind = "transfer"
	KindHoldCapture TransactionKind = "hold_capture"
)

// Transaction is a set of postings that happen together, or not at all.
type Transaction struct {
	ID       uuid.UUID
	Kind     TransactionKind
	Postings []Posting
}

// ValidatePostings checks the double-entry rule (invariant 1): there are at least two postings, none is zero,
// and per currency they sum to exactly zero. Money only ever moves between accounts; it never appears or vanishes.
func ValidatePostings(postings []Posting) error {
	if len(postings) < 2 {
		return newError(ErrUnbalanced, "a transaction needs at least two postings")
	}
	sums := map[Currency]Money{}
	for _, p := range postings {
		if p.Amount.Amount == 0 {
			return newError(ErrUnbalanced, "a posting can't be zero")
		}
		sum, ok := sums[p.Amount.Currency]
		if !ok {
			sum = Money{Currency: p.Amount.Currency}
		}
		next, err := sum.Add(p.Amount)
		if err != nil {
			return err
		}
		sums[p.Amount.Currency] = next
	}
	for currency, sum := range sums {
		if sum.Amount != 0 {
			return newError(ErrUnbalanced, fmt.Sprintf("%s postings sum to %s, not zero", currency, sum))
		}
	}
	return nil
}

// TransferPostings returns the two postings that move amount from one account to another: a debit (−) and a credit (+).
func TransferPostings(from, to uuid.UUID, amount Money) ([]Posting, error) {
	debit, err := amount.Neg()
	if err != nil {
		return nil, err
	}
	return []Posting{{AccountID: from, Amount: debit}, {AccountID: to, Amount: amount}}, nil
}
