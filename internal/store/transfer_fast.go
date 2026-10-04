package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/tochinicky/go-payments-ledger/internal/events"
	"github.com/tochinicky/go-payments-ledger/internal/ledger"
)

// The transfer fast path sends a transfer's statements as one pipelined batch: one round trip while its row locks
// are held, instead of one per statement. sqlc can't put different queries in one pipeline, so these are written
// here, next to the generated ones they mirror.
//
// The debit is a conditional UPDATE that checks and applies in one step. Its predicate is ledger.Account.CanSpend
// in SQL: available − amount ≥ floor, where available = posted − held. Keep the two in step; a test checks they
// agree at the boundary. Anything the predicates refuse (or any error) rolls back to the savepoint, and the slow
// path (the ordinary checks, in the Book's order) decides which error it is.
const (
	fastSavepoint = `SAVEPOINT transfer`

	// debitFast and creditFast take each balance row's lock themselves; the caller sends them in account-id order,
	// like every other path, so two transfers can't deadlock.
	debitFast = `UPDATE balances b SET posted_minor = b.posted_minor - $3, version = b.version + 1
FROM accounts a
WHERE b.account_id = $1 AND a.id = b.account_id AND a.partner_id = $2 AND a.currency = $4 AND a.status = 'active'
  AND b.posted_minor - b.held_minor - $3 >= a.min_balance_minor
RETURNING b.version`
	creditFast = `UPDATE balances b SET posted_minor = b.posted_minor + $3, version = b.version + 1
FROM accounts a
WHERE b.account_id = $1 AND a.id = b.account_id AND a.partner_id = $2 AND a.currency = $4 AND a.status = 'active'
  AND b.posted_minor <= 9223372036854775807 - $3
RETURNING b.version`

	insertTransactionFast = `INSERT INTO transactions (id, partner_id, kind, reference) VALUES ($1, $2, 'transfer', $3) RETURNING created_at`

	// The posting and the event read the version the UPDATE above just produced, inside the same transaction, so
	// they need no round trip of their own. The event's balance fields are filled the same way.
	insertPostingFast = `INSERT INTO postings (id, transaction_id, account_id, amount_minor, currency, account_seq, created_at)
SELECT $1, $2, $3, $4, $5, b.version, clock_timestamp() FROM balances b WHERE b.account_id = $3`
	insertOutboxFast = `INSERT INTO outbox (id, partner_id, account_id, account_seq, event_type, payload)
SELECT $1, $2, $3, b.version, $4, $5::jsonb || jsonb_build_object(
    'account_seq', b.version, 'posted_balance_after', b.posted_minor, 'held_balance_after', b.held_minor,
    'occurred_at', clock_timestamp())
FROM balances b WHERE b.account_id = $3`

	rollbackFast = `ROLLBACK TO SAVEPOINT transfer`
)

// errFastRefused means the fast path's predicates refused the transfer; the slow path finds out why.
var errFastRefused = errors.New("fast path refused")

// transferFast tries the transfer in one round trip. errFastRefused (or any other error) means nothing was applied:
// the caller rolls back to the savepoint and takes the slow path.
func (tx Tx) transferFast(ctx context.Context, partnerID uuid.UUID, t *Transfer, postings []ledger.Posting) error {
	b := &pgx.Batch{}
	b.Queue(fastSavepoint)
	cur := string(t.Amount.Currency)
	// Lock order: account id, as in lockAccounts.
	debitFirst := uuidLess(t.From, t.To)
	if debitFirst {
		b.Queue(debitFast, t.From, partnerID, t.Amount.Amount, cur)
		b.Queue(creditFast, t.To, partnerID, t.Amount.Amount, cur)
	} else {
		b.Queue(creditFast, t.To, partnerID, t.Amount.Amount, cur)
		b.Queue(debitFast, t.From, partnerID, t.Amount.Amount, cur)
	}
	b.Queue(insertTransactionFast, t.ID, partnerID, t.Reference)
	kind := string(ledger.KindTransfer)
	for _, e := range transferEntries(partnerID, t.ID, ledger.KindTransfer, postings) {
		eventID := tx.s.newID()
		payload, err := json.Marshal(events.Event{
			EventID: eventID, EventType: e.eventType, SchemaVersion: events.SchemaVersion, PartnerID: partnerID,
			AccountID: e.accountID, TransactionID: &t.ID, TransactionKind: &kind, AmountMinor: e.amount, Currency: cur,
			PostedDeltaMinor: e.postedDelta,
		})
		if err != nil {
			return fmt.Errorf("event payload: %w", err)
		}
		b.Queue(insertPostingFast, tx.s.newID(), t.ID, e.accountID, e.postedDelta, cur)
		b.Queue(insertOutboxFast, eventID, partnerID, e.accountID, e.eventType, payload)
	}

	br := tx.tx.SendBatch(ctx, b)
	err := readFast(br, t)
	if closeErr := br.Close(); err == nil {
		err = closeErr
	}
	return err
}

// readFast reads the batch's results in order. A balance UPDATE that matched no row is a refusal.
func readFast(br pgx.BatchResults, t *Transfer) error {
	if _, err := br.Exec(); err != nil { // SAVEPOINT
		return err
	}
	for range 2 { // the two balance UPDATEs
		var version int64
		if err := br.QueryRow().Scan(&version); errors.Is(err, pgx.ErrNoRows) {
			return errFastRefused
		} else if err != nil {
			return err
		}
	}
	var created time.Time
	if err := br.QueryRow().Scan(&created); err != nil {
		return err
	}
	t.CreatedAt = created
	for range 4 { // two postings, two events
		tag, err := br.Exec()
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("fast path wrote %d rows, want 1", tag.RowsAffected())
		}
	}
	return nil
}

func uuidLess(a, b uuid.UUID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
