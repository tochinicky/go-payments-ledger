package ledger

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Book is an in-memory ledger. Rule for Version (it becomes account_seq in events): exactly one bump per account
// per ledger operation that changes the account's money, posted or held: place, capture, release and expire
// of a hold, and each side of a transfer.
//
// Book is an in-memory ledger. It applies the same rules the database-backed ledger will, and it's the
// reference model for the property-based tests: random commands against the Book must never break an invariant,
// and (from slice 2) the database must always agree with it. Not safe for concurrent use.
type Book struct {
	accounts     map[uuid.UUID]Account
	balances     map[uuid.UUID]Balance
	holds        map[uuid.UUID]Hold
	transactions []Transaction
	newID        func() uuid.UUID
}

// NewBook returns an empty ledger. newID makes entity ids (uuid.NewV7 in production; deterministic in tests).
func NewBook(newID func() uuid.UUID) *Book {
	return &Book{
		accounts: map[uuid.UUID]Account{},
		balances: map[uuid.UUID]Balance{},
		holds:    map[uuid.UUID]Hold{},
		newID:    newID,
	}
}

// OpenAccount adds an account with a zero balance.
func (b *Book) OpenAccount(partnerID uuid.UUID, kind AccountKind, currency Currency, minBalance int64) (Account, error) {
	if _, err := New(0, currency); err != nil {
		return Account{}, err
	}
	if kind == KindCustomer && minBalance != 0 {
		return Account{}, newError(ErrInvalidAmount, "a customer account's floor is 0")
	}
	if minBalance > 0 {
		return Account{}, newError(ErrInvalidAmount, "a floor can't be above zero")
	}
	a := Account{ID: b.newID(), PartnerID: partnerID, Kind: kind, Currency: currency, Status: StatusActive, MinBalance: minBalance}
	b.accounts[a.ID] = a
	b.balances[a.ID] = Balance{}
	return a, nil
}

// CloseAccount stops an account from moving money (its history stays). Only an empty account can be closed:
// otherwise its money would be stranded, and an active hold on it could still be captured. A settlement account
// can't be closed at all, since its balance mirrors the customers' money. Closing a closed account is a no-op.
func (b *Book) CloseAccount(partnerID, accountID uuid.UUID) error {
	a, err := b.account(partnerID, accountID)
	if err != nil {
		return err
	}
	if a.Status == StatusClosed {
		return nil
	}
	if a.Kind == KindSettlement {
		return ErrAccountNotClosable
	}
	if balance := b.balances[a.ID]; balance.Posted != 0 || balance.Held != 0 { // held == 0 ⇒ no active holds
		return ErrAccountNotEmpty
	}
	a.Status = StatusClosed
	b.accounts[a.ID] = a
	return nil
}

// Account returns an account as seen by a partner: another partner's account is "not found" (invariant 7).
func (b *Book) Account(partnerID, accountID uuid.UUID) (Account, Balance, error) {
	a, err := b.account(partnerID, accountID)
	if err != nil {
		return Account{}, Balance{}, err
	}
	return a, b.balances[a.ID], nil
}

// Transfer moves amount from one of the partner's accounts to another.
func (b *Book) Transfer(partnerID, fromID, toID uuid.UUID, amount Money) (Transaction, error) {
	from, to, err := b.pair(partnerID, fromID, toID, amount)
	if err != nil {
		return Transaction{}, err
	}
	if err := from.CanSpend(b.balances[from.ID], amount.Amount); err != nil {
		return Transaction{}, err
	}
	return b.post(KindTransfer, from.ID, to.ID, amount)
}

// PlaceHold reserves amount on an account for a destination fixed now.
func (b *Book) PlaceHold(partnerID, accountID, toAccountID uuid.UUID, amount Money, expiresAt time.Time) (Hold, error) {
	from, to, err := b.pair(partnerID, accountID, toAccountID, amount)
	if err != nil {
		return Hold{}, err
	}
	balance := b.balances[from.ID]
	if err := from.CanSpend(balance, amount.Amount); err != nil {
		return Hold{}, err
	}
	balance.Held += amount.Amount // can't overflow: CanSpend showed posted − held − amount ≥ floor
	balance.Version++
	b.balances[from.ID] = balance
	h := Hold{ID: b.newID(), AccountID: from.ID, ToAccountID: to.ID, Amount: amount, Status: HoldActive, ExpiresAt: expiresAt}
	b.holds[h.ID] = h
	return h, nil
}

// CaptureHold moves amount (at most the hold's amount) to the hold's destination and releases the rest.
func (b *Book) CaptureHold(partnerID, holdID uuid.UUID, amount int64) (Hold, Transaction, error) {
	h, err := b.activeHold(partnerID, holdID)
	if err != nil {
		return Hold{}, Transaction{}, err
	}
	if amount <= 0 {
		return Hold{}, Transaction{}, ErrInvalidAmount
	}
	if amount > h.Amount.Amount {
		return Hold{}, Transaction{}, ErrCaptureExceedsHold
	}
	if b.accounts[h.ToAccountID].Status != StatusActive {
		return Hold{}, Transaction{}, ErrHoldNotCapturable // the hold stays active
	}
	// Unreachable while the closure rule holds (an account with an active hold isn't empty, so it can't be closed);
	// kept so a closed account can never change balance even if that rule is ever loosened.
	if b.accounts[h.AccountID].Status != StatusActive {
		return Hold{}, Transaction{}, ErrAccountNotActive
	}

	// Release the whole reservation, then post the captured part. Available goes up by (hold − captured), never down,
	// so no funds check is needed: the money was reserved when the hold was placed.
	before := b.balances[h.AccountID]
	released := before
	released.Held -= h.Amount.Amount
	b.balances[h.AccountID] = released
	tx, err := b.post(KindHoldCapture, h.AccountID, h.ToAccountID, Money{Amount: amount, Currency: h.Amount.Currency})
	if err != nil {
		b.balances[h.AccountID] = before // all or nothing: the hold is still active, so its reservation must be too
		return Hold{}, Transaction{}, err
	}
	h.Status, h.Captured = HoldCaptured, amount
	b.holds[h.ID] = h
	return h, tx, nil
}

// ReleaseHold frees a hold's reservation without moving money.
func (b *Book) ReleaseHold(partnerID, holdID uuid.UUID) (Hold, error) {
	h, err := b.activeHold(partnerID, holdID)
	if err != nil {
		return Hold{}, err
	}
	return b.endHold(h, HoldReleased), nil
}

// ExpireHolds ends every active hold whose time has come, as the background job does. It returns how many expired.
func (b *Book) ExpireHolds(now time.Time) int {
	n := 0
	for _, h := range b.holds {
		if h.Status == HoldActive && !now.Before(h.ExpiresAt) {
			b.endHold(h, HoldExpired)
			n++
		}
	}
	return n
}

// Transactions returns every transaction, in order.
func (b *Book) Transactions() []Transaction { return append([]Transaction(nil), b.transactions...) }

func (b *Book) endHold(h Hold, status HoldStatus) Hold {
	balance := b.balances[h.AccountID]
	balance.Held -= h.Amount.Amount
	balance.Version++
	b.balances[h.AccountID] = balance
	h.Status = status
	b.holds[h.ID] = h
	return h
}

// post records a balanced transaction and applies it to the balances.
func (b *Book) post(kind TransactionKind, fromID, toID uuid.UUID, amount Money) (Transaction, error) {
	postings, err := TransferPostings(fromID, toID, amount)
	if err != nil {
		return Transaction{}, err
	}
	if err := ValidatePostings(postings); err != nil {
		return Transaction{}, err
	}
	// Check every new balance before changing any: all postings apply, or none do.
	next := map[uuid.UUID]Balance{}
	for _, p := range postings {
		balance, ok := next[p.AccountID]
		if !ok {
			balance = b.balances[p.AccountID]
		}
		posted, ok := addInt64(balance.Posted, p.Amount.Amount)
		if !ok {
			return Transaction{}, ErrOverflow
		}
		balance.Posted = posted
		balance.Version++
		next[p.AccountID] = balance
	}
	for id, balance := range next {
		b.balances[id] = balance
	}
	tx := Transaction{ID: b.newID(), Kind: kind, Postings: postings}
	b.transactions = append(b.transactions, tx)
	return tx, nil
}

// pair loads two different accounts of the same partner, both active, in the amount's currency.
func (b *Book) pair(partnerID, fromID, toID uuid.UUID, amount Money) (Account, Account, error) {
	if !amount.IsPositive() {
		return Account{}, Account{}, ErrInvalidAmount
	}
	if fromID == toID {
		return Account{}, Account{}, ErrSameAccount
	}
	from, err := b.account(partnerID, fromID)
	if err != nil {
		return Account{}, Account{}, err
	}
	to, err := b.account(partnerID, toID)
	if err != nil {
		return Account{}, Account{}, err
	}
	if from.Currency != amount.Currency || to.Currency != amount.Currency {
		return Account{}, Account{}, ErrCurrencyMismatch
	}
	if from.Status != StatusActive || to.Status != StatusActive {
		return Account{}, Account{}, ErrAccountNotActive
	}
	return from, to, nil
}

// account finds an account of this partner. Another partner's account is reported exactly like a missing one,
// so a partner can't even learn that it exists (invariant 7).
func (b *Book) account(partnerID, accountID uuid.UUID) (Account, error) {
	a, ok := b.accounts[accountID]
	if !ok || a.PartnerID != partnerID {
		return Account{}, ErrNotFound
	}
	return a, nil
}

func (b *Book) activeHold(partnerID, holdID uuid.UUID) (Hold, error) {
	h, ok := b.holds[holdID]
	if !ok || b.accounts[h.AccountID].PartnerID != partnerID {
		return Hold{}, ErrNotFound
	}
	if h.Status != HoldActive {
		return Hold{}, ErrHoldNotActive
	}
	return h, nil
}

// CheckInvariants verifies invariants 1–4 over the whole book, the way the reconciliation job will over the database:
//  1. every transaction's postings sum to zero per currency;
//  2. every account's posted balance is the sum of its postings;
//  3. every account's available balance is at or above its floor;
//  4. every account's held balance is the sum of its active holds, and no capture exceeds its hold.
func (b *Book) CheckInvariants() error {
	var errs []error
	posted := map[uuid.UUID]int64{}
	for _, tx := range b.transactions {
		if err := ValidatePostings(tx.Postings); err != nil {
			errs = append(errs, fmt.Errorf("invariant 1, transaction %s: %w", tx.ID, err))
		}
		for _, p := range tx.Postings {
			sum, ok := addInt64(posted[p.AccountID], p.Amount.Amount)
			if !ok {
				errs = append(errs, fmt.Errorf("invariant 2, account %s: the sum of its postings overflows", p.AccountID))
			}
			posted[p.AccountID] = sum
		}
	}
	held := map[uuid.UUID]int64{}
	for _, h := range b.holds {
		if h.Status == HoldActive {
			sum, ok := addInt64(held[h.AccountID], h.Amount.Amount)
			if !ok {
				errs = append(errs, fmt.Errorf("invariant 4, account %s: the sum of its holds overflows", h.AccountID))
			}
			held[h.AccountID] = sum
		}
		if h.Captured > h.Amount.Amount || (h.Status != HoldCaptured && h.Captured != 0) {
			errs = append(errs, fmt.Errorf("invariant 4, hold %s: captured %d of %d (%s)", h.ID, h.Captured, h.Amount.Amount, h.Status))
		}
	}
	for id, a := range b.accounts {
		balance := b.balances[id]
		if balance.Posted != posted[id] {
			errs = append(errs, fmt.Errorf("invariant 2, account %s: posted %d, postings sum to %d", id, balance.Posted, posted[id]))
		}
		if balance.Held != held[id] {
			errs = append(errs, fmt.Errorf("invariant 4, account %s: held %d, active holds sum to %d", id, balance.Held, held[id]))
		}
		if available, ok := balance.Available(); !ok || available < a.MinBalance {
			errs = append(errs, fmt.Errorf("invariant 3, account %s: available %d below floor %d", id, available, a.MinBalance))
		}
	}
	return errors.Join(errs...)
}
