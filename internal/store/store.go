// Package store is the database-backed ledger: it applies the rules of package ledger to rows in Postgres.
// Every money movement is one database transaction that locks the balance rows it touches, in account-id order,
// checks the rules against the locked rows, then writes the transaction, its postings and the new balances.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store/db"
)

// Store is the ledger in Postgres. It is safe for concurrent use: the database does the coordinating.
type Store struct {
	pool  *pgxpool.Pool
	newID func() uuid.UUID
}

// New returns a store on pool. newID makes entity ids (NewID in production; deterministic in tests).
func New(pool *pgxpool.Pool, newID func() uuid.UUID) *Store {
	return &Store{pool: pool, newID: newID}
}

// NewID returns a UUIDv7: time-ordered, so new rows land at the end of the primary-key index instead of at random.
func NewID() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}

// Partner is a partner as the API sees it.
type Partner struct {
	ID           uuid.UUID
	Name         string
	FundingLimit int64
}

// AccountView is an account with its balance, read in one query.
type AccountView struct {
	ledger.Account
	CustomerRef *string
	CreatedAt   time.Time
	Balance     ledger.Balance
}

// Transfer is a posted transfer.
type Transfer struct {
	ID        uuid.UUID
	From      uuid.UUID
	To        uuid.UUID
	Amount    ledger.Money
	Reference *string
	CreatedAt time.Time
}

// Entry is one line of an account statement.
type Entry struct {
	PostingID     uuid.UUID
	TransactionID uuid.UUID
	Kind          ledger.TransactionKind
	Reference     *string
	Amount        ledger.Money
	CreatedAt     time.Time
}

// Cursor marks a position in a statement: the last entry of the previous page. The zero Cursor is the start.
type Cursor struct {
	CreatedAt time.Time
	PostingID uuid.UUID
}

// CreatePartner adds a partner and its settlement account in each currency, whose floor is minus the funding limit.
// Only the hash of the API key is stored. Run as the owner role (ledgerctl): the app role can't insert partners.
func (s *Store) CreatePartner(ctx context.Context, name string, apiKeyHash []byte, fundingLimit int64, currencies []ledger.Currency) (Partner, error) {
	if fundingLimit < 0 {
		return Partner{}, fmt.Errorf("funding limit %d: %w", fundingLimit, ledger.ErrInvalidAmount)
	}
	p := Partner{ID: s.newID(), Name: name, FundingLimit: fundingLimit}
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		if err := q.InsertPartner(ctx, db.InsertPartnerParams{
			ID: p.ID, Name: name, ApiKeyHash: apiKeyHash, RateLimitPerMin: 600, FundingLimitMinor: fundingLimit,
		}); err != nil {
			return fmt.Errorf("insert partner: %w", err)
		}
		for _, c := range currencies {
			if _, err := s.openAccount(ctx, q, p.ID, ledger.KindSettlement, c, -fundingLimit, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Partner{}, err
	}
	return p, nil
}

// PartnerByKeyHash finds the partner an API key belongs to, by the key's hash. ok is false if there is none.
func (s *Store) PartnerByKeyHash(ctx context.Context, hash []byte) (p Partner, ok bool, err error) {
	row, err := db.New(s.pool).PartnerByKeyHash(ctx, hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return Partner{}, false, nil
	}
	if err != nil {
		return Partner{}, false, fmt.Errorf("partner by key: %w", err)
	}
	return Partner{ID: row.ID, Name: row.Name, FundingLimit: row.FundingLimitMinor}, true, nil
}

// OpenCustomerAccount opens a customer account (floor 0) for a partner. Settlement accounts are created only with
// their partner.
func (s *Store) OpenCustomerAccount(ctx context.Context, partnerID uuid.UUID, currency ledger.Currency, customerRef *string) (AccountView, error) {
	var view AccountView
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		id, err := s.openAccount(ctx, q, partnerID, ledger.KindCustomer, currency, 0, customerRef)
		if err != nil {
			return err
		}
		view, err = accountView(ctx, q, partnerID, id)
		return err
	})
	return view, err
}

func (s *Store) openAccount(ctx context.Context, q *db.Queries, partnerID uuid.UUID, kind ledger.AccountKind, currency ledger.Currency, minBalance int64, customerRef *string) (uuid.UUID, error) {
	if _, err := ledger.New(0, currency); err != nil {
		return uuid.Nil, err
	}
	id := s.newID()
	if err := q.InsertAccount(ctx, db.InsertAccountParams{
		ID: id, PartnerID: partnerID, Kind: string(kind), CustomerRef: customerRef, Currency: string(currency), MinBalanceMinor: minBalance,
	}); err != nil {
		return uuid.Nil, fmt.Errorf("insert account: %w", err)
	}
	if err := q.InsertBalance(ctx, id); err != nil {
		return uuid.Nil, fmt.Errorf("insert balance: %w", err)
	}
	return id, nil
}

// Account returns one of the partner's accounts with its balance. Another partner's account is ErrNotFound,
// exactly like a missing one (invariant 7).
func (s *Store) Account(ctx context.Context, partnerID, accountID uuid.UUID) (AccountView, error) {
	return accountView(ctx, db.New(s.pool), partnerID, accountID)
}

func accountView(ctx context.Context, q *db.Queries, partnerID, accountID uuid.UUID) (AccountView, error) {
	row, err := q.AccountWithBalance(ctx, db.AccountWithBalanceParams{ID: accountID, PartnerID: partnerID})
	if errors.Is(err, pgx.ErrNoRows) {
		return AccountView{}, ledger.ErrNotFound
	}
	if err != nil {
		return AccountView{}, fmt.Errorf("account: %w", err)
	}
	return AccountView{
		Account: ledger.Account{
			ID: row.ID, PartnerID: row.PartnerID, Kind: ledger.AccountKind(row.Kind), Currency: ledger.Currency(row.Currency),
			Status: ledger.AccountStatus(row.Status), MinBalance: row.MinBalanceMinor,
		},
		CustomerRef: row.CustomerRef,
		CreatedAt:   row.CreatedAt.Time,
		Balance:     ledger.Balance{Posted: row.PostedMinor, Held: row.HeldMinor, Version: row.Version},
	}, nil
}

// Transfer moves amount between two of the partner's accounts. The checks are the in-memory Book's, in the same
// order, made against balance rows locked for the rest of the transaction, so no concurrent transfer can spend
// the same money between the check and the write.
func (s *Store) Transfer(ctx context.Context, partnerID, fromID, toID uuid.UUID, amount ledger.Money, reference *string) (Transfer, error) {
	if !amount.IsPositive() {
		return Transfer{}, ledger.ErrInvalidAmount
	}
	if fromID == toID {
		return Transfer{}, ledger.ErrSameAccount
	}
	postings, err := ledger.TransferPostings(fromID, toID, amount)
	if err != nil {
		return Transfer{}, err
	}
	if err := ledger.ValidatePostings(postings); err != nil {
		return Transfer{}, err
	}
	t := Transfer{ID: s.newID(), From: fromID, To: toID, Amount: amount, Reference: reference}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		locked, err := lockAccounts(ctx, q, partnerID, fromID, toID)
		if err != nil {
			return err
		}
		from, to := locked[fromID], locked[toID]
		if from.Currency != amount.Currency || to.Currency != amount.Currency {
			return ledger.ErrCurrencyMismatch
		}
		if from.Status != ledger.StatusActive || to.Status != ledger.StatusActive {
			return ledger.ErrAccountNotActive
		}
		if err := from.CanSpend(from.balance, amount.Amount); err != nil {
			return err
		}
		created, err := q.InsertTransaction(ctx, db.InsertTransactionParams{
			ID: t.ID, PartnerID: partnerID, Kind: string(ledger.KindTransfer), Reference: reference,
		})
		if err != nil {
			return fmt.Errorf("insert transaction: %w", err)
		}
		t.CreatedAt = created.Time
		return s.post(ctx, q, t.ID, postings, locked)
	})
	if err != nil {
		return Transfer{}, err
	}
	return t, nil
}

// lockedAccount is an account and its balance as read under the balance row's lock.
type lockedAccount struct {
	ledger.Account
	balance ledger.Balance
}

// lockAccounts locks the balance rows of the given accounts of one partner, in account-id order (the query sorts),
// and returns them by id. Any account that is missing or another partner's is ErrNotFound.
func lockAccounts(ctx context.Context, q *db.Queries, partnerID uuid.UUID, ids ...uuid.UUID) (map[uuid.UUID]lockedAccount, error) {
	rows, err := q.LockAccounts(ctx, db.LockAccountsParams{Ids: ids, PartnerID: partnerID})
	if err != nil {
		return nil, fmt.Errorf("lock accounts: %w", err)
	}
	locked := make(map[uuid.UUID]lockedAccount, len(rows))
	for _, r := range rows {
		locked[r.ID] = lockedAccount{
			Account: ledger.Account{
				ID: r.ID, PartnerID: r.PartnerID, Kind: ledger.AccountKind(r.Kind), Currency: ledger.Currency(r.Currency),
				Status: ledger.AccountStatus(r.Status), MinBalance: r.MinBalanceMinor,
			},
			balance: ledger.Balance{Posted: r.PostedMinor, Held: r.HeldMinor, Version: r.Version},
		}
	}
	for _, id := range ids {
		if _, ok := locked[id]; !ok {
			return nil, ledger.ErrNotFound
		}
	}
	return locked, nil
}

// post writes a transaction's postings and applies them to the locked balances. Every new balance is computed
// with checked arithmetic first, so an overflow writes nothing.
func (s *Store) post(ctx context.Context, q *db.Queries, txID uuid.UUID, postings []ledger.Posting, locked map[uuid.UUID]lockedAccount) error {
	for _, p := range postings {
		posted := ledger.Money{Amount: locked[p.AccountID].balance.Posted, Currency: p.Amount.Currency}
		if _, err := posted.Add(p.Amount); err != nil {
			return err
		}
	}
	for _, p := range postings {
		if err := q.InsertPosting(ctx, db.InsertPostingParams{
			ID: s.newID(), TransactionID: txID, AccountID: p.AccountID, AmountMinor: p.Amount.Amount, Currency: string(p.Amount.Currency),
		}); err != nil {
			return fmt.Errorf("insert posting: %w", err)
		}
		if _, err := q.AddToPosted(ctx, db.AddToPostedParams{AccountID: p.AccountID, Delta: p.Amount.Amount}); err != nil {
			return fmt.Errorf("update balance: %w", err)
		}
	}
	return nil
}

// Statement returns up to limit entries of one of the partner's accounts after cursor, oldest first.
func (s *Store) Statement(ctx context.Context, partnerID, accountID uuid.UUID, after Cursor, limit int32) ([]Entry, error) {
	q := db.New(s.pool)
	if _, err := accountView(ctx, q, partnerID, accountID); err != nil {
		return nil, err
	}
	// The zero cursor starts before every row: -infinity sorts first, and uuid.Nil is the smallest id.
	afterAt := pgtype.Timestamptz{Time: after.CreatedAt, Valid: true}
	if after.CreatedAt.IsZero() {
		afterAt = pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}
	}
	rows, err := q.Statement(ctx, db.StatementParams{
		AccountID: accountID, AfterCreatedAt: afterAt, AfterID: after.PostingID, PageSize: limit,
	})
	if err != nil {
		return nil, fmt.Errorf("statement: %w", err)
	}
	entries := make([]Entry, 0, len(rows))
	for _, r := range rows {
		entries = append(entries, Entry{
			PostingID: r.ID, TransactionID: r.TransactionID, Kind: ledger.TransactionKind(r.Kind), Reference: r.Reference,
			Amount: ledger.Money{Amount: r.AmountMinor, Currency: ledger.Currency(r.Currency)}, CreatedAt: r.CreatedAt.Time,
		})
	}
	return entries, nil
}
