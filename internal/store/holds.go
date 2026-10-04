package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/tochinicky/go-payments-ledger/internal/events"
	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store/db"
)

// Hold is a hold as the API sees it.
type Hold struct {
	ID            uuid.UUID
	AccountID     uuid.UUID
	ToAccountID   uuid.UUID
	Amount        ledger.Money
	Status        ledger.HoldStatus
	ExpiresAt     time.Time
	Captured      int64
	TransactionID *uuid.UUID
	CreatedAt     time.Time
}

func toHold(h db.Hold) Hold {
	return Hold{
		ID: h.ID, AccountID: h.AccountID, ToAccountID: h.ToAccountID,
		Amount: ledger.Money{Amount: h.AmountMinor, Currency: ledger.Currency(h.Currency)},
		Status: ledger.HoldStatus(h.Status), ExpiresAt: h.ExpiresAt.Time, Captured: h.CapturedMinor,
		TransactionID: h.TransactionID, CreatedAt: h.CreatedAt.Time,
	}
}

// lockedHold is a locked hold row without its expiry check.
func lockedHold(r db.LockHoldRow) Hold {
	return toHold(db.Hold{
		ID: r.ID, PartnerID: r.PartnerID, AccountID: r.AccountID, ToAccountID: r.ToAccountID, AmountMinor: r.AmountMinor,
		Currency: r.Currency, Status: r.Status, ExpiresAt: r.ExpiresAt, CapturedMinor: r.CapturedMinor,
		TransactionID: r.TransactionID, CreatedAt: r.CreatedAt, EndedAt: r.EndedAt,
	})
}

// PlaceHold reserves amount on one of the partner's accounts for a destination fixed now, until expiresIn from now
// (on the database clock). The checks are the Book's: both accounts the partner's, active, in the amount's currency,
// and the source able to spend it.
func (tx Tx) PlaceHold(ctx context.Context, partnerID, accountID, toAccountID uuid.UUID, amount ledger.Money, expiresIn time.Duration) (Hold, error) {
	if !amount.IsPositive() {
		return Hold{}, ledger.ErrInvalidAmount
	}
	if accountID == toAccountID {
		return Hold{}, ledger.ErrSameAccount
	}
	locked, err := lockAccounts(ctx, tx.q, partnerID, accountID, toAccountID)
	if err != nil {
		return Hold{}, err
	}
	from, to := locked[accountID], locked[toAccountID]
	if from.Currency != amount.Currency || to.Currency != amount.Currency {
		return Hold{}, ledger.ErrCurrencyMismatch
	}
	if from.Status != ledger.StatusActive || to.Status != ledger.StatusActive {
		return Hold{}, ledger.ErrAccountNotActive
	}
	if err := from.CanSpend(from.balance, amount.Amount); err != nil {
		return Hold{}, err // CanSpend also shows posted − held − amount fits, so held + amount can't overflow
	}
	holdID := tx.s.newID()
	h, err := tx.q.InsertHold(ctx, db.InsertHoldParams{
		ID: holdID, PartnerID: partnerID, AccountID: accountID, ToAccountID: toAccountID,
		AmountMinor: amount.Amount, Currency: string(amount.Currency), ExpiresInSeconds: expiresIn.Seconds(),
	})
	if err != nil {
		return Hold{}, fmt.Errorf("insert hold: %w", err)
	}
	if _, err := tx.apply(ctx, entry{
		partnerID: partnerID, accountID: accountID, currency: amount.Currency, heldDelta: amount.Amount,
		eventType: events.HoldPlaced, amount: -amount.Amount, holdID: &holdID,
	}); err != nil {
		return Hold{}, err
	}
	return toHold(h), nil
}

// lockActiveHold locks one of the partner's holds and checks it can still be ended by a client. A hold whose time is
// up is no longer active even before the expiry job has ended it: an expired authorisation can't be captured or
// released.
func (tx Tx) lockActiveHold(ctx context.Context, partnerID, holdID uuid.UUID) (db.LockHoldRow, error) {
	h, err := tx.q.LockHold(ctx, db.LockHoldParams{ID: holdID, PartnerID: partnerID})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.LockHoldRow{}, ledger.ErrNotFound
	}
	if err != nil {
		return db.LockHoldRow{}, fmt.Errorf("lock hold: %w", err)
	}
	if h.Status != string(ledger.HoldActive) || h.Expired {
		return db.LockHoldRow{}, ledger.ErrHoldNotActive
	}
	return h, nil
}

// CaptureHold moves amount (at most the hold's amount) to the hold's destination and releases the rest, as one
// transaction. The source's entry both releases the whole reservation and posts the captured part, so its version
// goes up once; no funds check is needed, since the money was reserved when the hold was placed.
func (tx Tx) CaptureHold(ctx context.Context, partnerID, holdID uuid.UUID, amount int64) (Hold, error) {
	h, err := tx.lockActiveHold(ctx, partnerID, holdID)
	if err != nil {
		return Hold{}, err
	}
	if amount <= 0 {
		return Hold{}, ledger.ErrInvalidAmount
	}
	if amount > h.AmountMinor {
		return Hold{}, ledger.ErrCaptureExceedsHold
	}
	locked, err := lockAccounts(ctx, tx.q, partnerID, h.AccountID, h.ToAccountID)
	if err != nil {
		return Hold{}, err
	}
	if locked[h.ToAccountID].Status != ledger.StatusActive {
		return Hold{}, ledger.ErrHoldNotCapturable // the hold stays active
	}
	if locked[h.AccountID].Status != ledger.StatusActive { // unreachable while only empty accounts can be closed
		return Hold{}, ledger.ErrAccountNotActive
	}
	captured := ledger.Money{Amount: amount, Currency: ledger.Currency(h.Currency)}
	postings, err := ledger.TransferPostings(h.AccountID, h.ToAccountID, captured)
	if err != nil {
		return Hold{}, err
	}
	if err := checkPosted(postings, locked); err != nil {
		return Hold{}, err
	}
	txID := tx.s.newID()
	if _, err := tx.q.InsertTransaction(ctx, db.InsertTransactionParams{ID: txID, PartnerID: partnerID, Kind: string(ledger.KindHoldCapture)}); err != nil {
		return Hold{}, fmt.Errorf("insert transaction: %w", err)
	}
	kind := ledger.KindHoldCapture
	entries := transferEntries(partnerID, txID, kind, postings)
	// The source's entry releases the whole reservation and posts the captured part: one entry, one version.
	entries[0].heldDelta, entries[0].eventType, entries[0].holdID = -h.AmountMinor, events.HoldCaptured, &h.ID
	if err := tx.post(ctx, txID, postings, entries); err != nil {
		return Hold{}, err
	}
	ended, err := tx.q.EndHold(ctx, db.EndHoldParams{ID: h.ID, Status: string(ledger.HoldCaptured), CapturedMinor: amount, TransactionID: &txID})
	if err != nil {
		return Hold{}, fmt.Errorf("end hold: %w", err)
	}
	return toHold(ended), nil
}

// ReleaseHold frees a hold's reservation without moving money.
func (tx Tx) ReleaseHold(ctx context.Context, partnerID, holdID uuid.UUID) (Hold, error) {
	h, err := tx.lockActiveHold(ctx, partnerID, holdID)
	if err != nil {
		return Hold{}, err
	}
	if _, err := lockAccounts(ctx, tx.q, partnerID, h.AccountID); err != nil {
		return Hold{}, err
	}
	return tx.endHold(ctx, lockedHold(h), partnerID, ledger.HoldReleased)
}

// endHold gives back a hold's reservation (one entry on its account, with its event) and marks the hold ended.
func (tx Tx) endHold(ctx context.Context, h Hold, partnerID uuid.UUID, status ledger.HoldStatus) (Hold, error) {
	eventType := events.HoldReleased
	if status == ledger.HoldExpired {
		eventType = events.HoldExpired
	}
	if _, err := tx.apply(ctx, entry{
		partnerID: partnerID, accountID: h.AccountID, currency: h.Amount.Currency, heldDelta: -h.Amount.Amount,
		eventType: eventType, amount: h.Amount.Amount, holdID: &h.ID,
	}); err != nil {
		return Hold{}, err
	}
	ended, err := tx.q.EndHold(ctx, db.EndHoldParams{ID: h.ID, Status: string(status)})
	if err != nil {
		return Hold{}, fmt.Errorf("end hold: %w", err)
	}
	return toHold(ended), nil
}

// Hold returns one of the partner's holds.
func (s *Store) Hold(ctx context.Context, partnerID, holdID uuid.UUID) (Hold, error) {
	h, err := db.New(s.pool).Hold(ctx, db.HoldParams{ID: holdID, PartnerID: partnerID})
	if errors.Is(err, pgx.ErrNoRows) {
		return Hold{}, ledger.ErrNotFound
	}
	if err != nil {
		return Hold{}, fmt.Errorf("hold: %w", err)
	}
	return toHold(h), nil
}

// ExpireHolds ends active holds whose time is up, each in its own short transaction, and returns how many it
// expired. A hold that is locked (a request is ending it, or another replica is expiring it) or whose account is
// locked (a transfer is running) is skipped and picked up on a later run, so the job never waits on a lock and
// never holds more than one hold's locks. Safe on several replicas.
func (s *Store) ExpireHolds(ctx context.Context, batchSize int32) (int, error) {
	total := 0
	for {
		due, err := db.New(s.pool).DueHolds(ctx, batchSize)
		if err != nil {
			return total, fmt.Errorf("due holds: %w", err)
		}
		expired := 0
		for _, id := range due {
			ok, err := s.expireHold(ctx, id)
			if err != nil {
				return total, err
			}
			if ok {
				expired++
			}
		}
		total += expired
		if len(due) < int(batchSize) || expired < len(due) { // done, or only busy holds are left for now
			return total, nil
		}
	}
}

func (s *Store) expireHold(ctx context.Context, id uuid.UUID) (expired bool, err error) {
	err = s.inTx(ctx, func(tx Tx) error {
		h, err := tx.q.LockDueHold(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // already ended, no longer due, or someone else has it
		}
		if err != nil {
			return fmt.Errorf("lock hold: %w", err)
		}
		if _, err := tx.q.LockBalanceNoWait(ctx, h.AccountID); errors.Is(err, pgx.ErrNoRows) {
			return nil // the account is busy: next run
		} else if err != nil {
			return fmt.Errorf("lock balance: %w", err)
		}
		if _, err := tx.endHold(ctx, toHold(h), h.PartnerID, ledger.HoldExpired); err != nil {
			return err
		}
		expired = true
		return nil
	})
	return expired, err
}

// PlaceHold, CaptureHold and ReleaseHold in their own transactions, for callers without an idempotency key.

// PlaceHold reserves money; see Tx.PlaceHold.
func (s *Store) PlaceHold(ctx context.Context, partnerID, accountID, toAccountID uuid.UUID, amount ledger.Money, expiresIn time.Duration) (Hold, error) {
	var h Hold
	err := s.inTx(ctx, func(tx Tx) error {
		var err error
		h, err = tx.PlaceHold(ctx, partnerID, accountID, toAccountID, amount, expiresIn)
		return err
	})
	return h, err
}

// CaptureHold captures a hold; see Tx.CaptureHold.
func (s *Store) CaptureHold(ctx context.Context, partnerID, holdID uuid.UUID, amount int64) (Hold, error) {
	var h Hold
	err := s.inTx(ctx, func(tx Tx) error {
		var err error
		h, err = tx.CaptureHold(ctx, partnerID, holdID, amount)
		return err
	})
	return h, err
}

// ReleaseHold releases a hold; see Tx.ReleaseHold.
func (s *Store) ReleaseHold(ctx context.Context, partnerID, holdID uuid.UUID) (Hold, error) {
	var h Hold
	err := s.inTx(ctx, func(tx Tx) error {
		var err error
		h, err = tx.ReleaseHold(ctx, partnerID, holdID)
		return err
	})
	return h, err
}
