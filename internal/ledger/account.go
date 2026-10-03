package ledger

import (
	"fmt"

	"github.com/google/uuid"
)

// AccountKind distinguishes a partner's customers' accounts from its settlement account.
type AccountKind string

// The account kinds.
const (
	// KindCustomer is an account belonging to one of a partner's customers. Its floor is 0.
	KindCustomer AccountKind = "customer"
	// KindSettlement is the partner's position at the bank: money enters and leaves the partner's world through it.
	// Its floor is minus the partner's funding limit, so it can go negative, but never without bound.
	KindSettlement AccountKind = "settlement"
)

// AccountStatus says whether an account can move money.
type AccountStatus string

// The account statuses.
const (
	StatusActive AccountStatus = "active"
	StatusClosed AccountStatus = "closed"
)

// Account is the static part of an account. Its money lives in Balance.
type Account struct {
	ID        uuid.UUID
	PartnerID uuid.UUID
	Kind      AccountKind
	Currency  Currency
	Status    AccountStatus
	// MinBalance is the lowest "available" balance allowed, in minor units: 0 for customers,
	// −(funding limit) for a settlement account.
	MinBalance int64
}

// Balance is an account's money. Posted is the sum of all its postings; Held is reserved by active holds.
// Version goes up by one on every change: it orders an account's events (account_seq).
type Balance struct {
	Posted  int64
	Held    int64
	Version int64
}

// Available is what can still be spent or held: posted minus held.
func (b Balance) Available() (int64, bool) {
	return addInt64(b.Posted, -b.Held)
}

// CanSpend checks invariant 3 for taking amount out of an account: available − amount must stay at or above the floor.
func (a Account) CanSpend(b Balance, amount int64) error {
	available, ok := b.Available()
	if !ok {
		return ErrOverflow
	}
	after, ok := addInt64(available, -amount)
	if !ok || after < a.MinBalance {
		return newError(ErrInsufficientFunds,
			fmt.Sprintf("available %s, need %s", FormatAmount(available, a.Currency.MinorUnits()), FormatAmount(amount, a.Currency.MinorUnits())))
	}
	return nil
}
