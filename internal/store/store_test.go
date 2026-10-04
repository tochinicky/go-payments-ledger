package store_test

import (
	"context"
	"crypto/rand"
	"errors"
	"math"
	mathrand "math/rand/v2"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"pgregory.net/rapid"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store"
	"github.com/tochinicky/go-payments-ledger/internal/testdb"
)

var tdb *testdb.DB

func TestMain(m *testing.M) { os.Exit(testdb.Run(m, &tdb)) }

const eur = ledger.Currency("EUR")

// fixture is one partner with its settlement account, so tests never see each other's money.
type fixture struct {
	store      *store.Store
	owner      *store.Store
	partner    store.Partner
	settlement uuid.UUID
}

func newFixture(t testing.TB, fundingLimit int64) fixture {
	t.Helper()
	ctx := context.Background()
	owner := store.New(tdb.Owner, store.NewID)
	p, err := owner.CreatePartner(ctx, "Test partner", randomHash(t), fundingLimit, []ledger.Currency{eur, "USD"})
	if err != nil {
		t.Fatal(err)
	}
	var settlement uuid.UUID
	if err := tdb.Owner.QueryRow(ctx,
		"SELECT id FROM accounts WHERE partner_id = $1 AND kind = 'settlement' AND currency = 'EUR'", p.ID).Scan(&settlement); err != nil {
		t.Fatal(err)
	}
	return fixture{store: store.New(tdb.App, store.NewID), owner: owner, partner: p, settlement: settlement}
}

func randomHash(t testing.TB) []byte {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func (f fixture) open(t testing.TB, currency ledger.Currency) uuid.UUID {
	t.Helper()
	a, err := f.store.OpenCustomerAccount(context.Background(), f.partner.ID, currency, nil)
	if err != nil {
		t.Fatal(err)
	}
	return a.ID
}

func (f fixture) transfer(t testing.TB, from, to uuid.UUID, amount int64) {
	t.Helper()
	if _, err := f.store.Transfer(context.Background(), f.partner.ID, from, to, ledger.Money{Amount: amount, Currency: eur}, nil); err != nil {
		t.Fatal(err)
	}
}

func (f fixture) balance(t testing.TB, id uuid.UUID) ledger.Balance {
	t.Helper()
	a, err := f.store.Account(context.Background(), f.partner.ID, id)
	if err != nil {
		t.Fatal(err)
	}
	return a.Balance
}

func TestCreatePartnerOpensSettlementAccountsAtMinusTheFundingLimit(t *testing.T) {
	f := newFixture(t, 50_000)
	a, err := f.store.Account(context.Background(), f.partner.ID, f.settlement)
	if err != nil {
		t.Fatal(err)
	}
	if a.Kind != ledger.KindSettlement || a.MinBalance != -50_000 || a.Status != ledger.StatusActive {
		t.Fatalf("settlement account = %+v", a.Account)
	}
}

func TestTransferMovesMoneyAndBumpsBothVersions(t *testing.T) {
	f := newFixture(t, 100_000)
	alice := f.open(t, eur)
	f.transfer(t, f.settlement, alice, 2_500)

	if got := f.balance(t, alice); got != (ledger.Balance{Posted: 2_500, Version: 1}) {
		t.Errorf("alice = %+v", got)
	}
	if got := f.balance(t, f.settlement); got != (ledger.Balance{Posted: -2_500, Version: 1}) {
		t.Errorf("settlement = %+v", got)
	}
}

func TestTransferRefusals(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	other := newFixture(t, 1_000)
	alice, bob, dollars := f.open(t, eur), f.open(t, eur), f.open(t, "USD")
	f.transfer(t, f.settlement, alice, 100)

	tests := []struct {
		name     string
		from, to uuid.UUID
		amount   ledger.Money
		want     error
	}{
		{"more than available", alice, bob, ledger.Money{Amount: 101, Currency: eur}, ledger.ErrInsufficientFunds},
		{"settlement past its funding limit", f.settlement, bob, ledger.Money{Amount: 901, Currency: eur}, ledger.ErrInsufficientFunds},
		{"zero", alice, bob, ledger.Money{Amount: 0, Currency: eur}, ledger.ErrInvalidAmount},
		{"same account", alice, alice, ledger.Money{Amount: 1, Currency: eur}, ledger.ErrSameAccount},
		{"currency differs from the accounts", alice, bob, ledger.Money{Amount: 1, Currency: "USD"}, ledger.ErrCurrencyMismatch},
		{"accounts in different currencies", alice, dollars, ledger.Money{Amount: 1, Currency: eur}, ledger.ErrCurrencyMismatch},
		{"missing account", alice, uuid.New(), ledger.Money{Amount: 1, Currency: eur}, ledger.ErrNotFound},
		{"another partner's account", alice, other.settlement, ledger.Money{Amount: 1, Currency: eur}, ledger.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := f.store.Transfer(ctx, f.partner.ID, tt.from, tt.to, tt.amount, nil)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
	// Nothing above may have moved money.
	if got := f.balance(t, alice).Posted; got != 100 {
		t.Errorf("alice posted = %d, want 100", got)
	}
}

func TestTransferToAClosedAccountIsRefused(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice, closed := f.open(t, eur), f.open(t, eur)
	f.transfer(t, f.settlement, alice, 100)
	if _, err := tdb.Owner.Exec(ctx, "UPDATE accounts SET status = 'closed' WHERE id = $1", closed); err != nil {
		t.Fatal(err)
	}
	_, err := f.store.Transfer(ctx, f.partner.ID, alice, closed, ledger.Money{Amount: 1, Currency: eur}, nil)
	if !errors.Is(err, ledger.ErrAccountNotActive) {
		t.Fatalf("err = %v, want account_not_active", err)
	}
}

func TestPostedBalanceOverflowWritesNothing(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice := f.open(t, eur)
	// No real sequence of transfers gets a balance this close to the limit, so set it directly (balances are mutable).
	if _, err := tdb.Owner.Exec(ctx, "UPDATE balances SET posted_minor = $1 WHERE account_id = $2", int64(math.MaxInt64-5), alice); err != nil {
		t.Fatal(err)
	}
	_, err := f.store.Transfer(ctx, f.partner.ID, f.settlement, alice, ledger.Money{Amount: 10, Currency: eur}, nil)
	if !errors.Is(err, ledger.ErrOverflow) {
		t.Fatalf("err = %v, want overflow", err)
	}
	if got := f.balance(t, f.settlement).Posted; got != 0 {
		t.Errorf("settlement posted = %d, want 0 (nothing written)", got)
	}
}

// Invariant 8: postings and transactions are append-only, enforced by the database for the app and the owner alike.
func TestPostingsAreAppendOnly(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice := f.open(t, eur)
	f.transfer(t, f.settlement, alice, 100)

	for _, stmt := range []string{
		"UPDATE postings SET amount_minor = 1 WHERE account_id = $1",
		"DELETE FROM postings WHERE account_id = $1",
		"DELETE FROM transactions WHERE id IN (SELECT transaction_id FROM postings WHERE account_id = $1)",
	} {
		_, err := tdb.App.Exec(ctx, stmt, alice)
		if code := pgCode(err); code != "42501" { // insufficient_privilege: the app role has no grant
			t.Errorf("app %q: err = %v, want permission denied", stmt, err)
		}
		_, err = tdb.Owner.Exec(ctx, stmt, alice)
		if code := pgCode(err); code != "23001" { // restrict_violation: the trigger stops even the owner
			t.Errorf("owner %q: err = %v, want the append-only trigger", stmt, err)
		}
	}
	_, err := tdb.Owner.Exec(ctx, "TRUNCATE postings CASCADE")
	if code := pgCode(err); code != "23001" {
		t.Errorf("owner TRUNCATE: err = %v, want the append-only trigger", err)
	}
}

// Invariant 1 in the database: a transaction whose postings don't sum to zero can't commit, whatever wrote it.
func TestUnbalancedTransactionCannotCommit(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice := f.open(t, eur)
	tx, err := tdb.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	txID := uuid.New()
	if _, err := tx.Exec(ctx, "INSERT INTO transactions (id, partner_id, kind) VALUES ($1, $2, 'transfer')", txID, f.partner.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO postings (id, transaction_id, account_id, amount_minor, currency) VALUES ($1, $2, $3, 100, 'EUR')",
		uuid.New(), txID, alice); err != nil {
		t.Fatal(err)
	}
	if code := pgCode(tx.Commit(ctx)); code != "23514" { // check_violation, raised at COMMIT by the deferred trigger
		t.Fatalf("commit: code %q, want 23514", code)
	}
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// TestConcurrentTransfersConserveMoney is the slice's concurrency test: 1,000 random transfers among 10 accounts,
// 50 at a time, sent as pairs moving money in opposite directions (A→B racing B→A: the classic deadlock). Afterwards:
// no error other than insufficient_funds (in particular no deadlock, 40P01), total money unchanged, no account
// below its floor, and every posted balance equal to the sum of its postings (invariant 2).
func TestConcurrentTransfersConserveMoney(t *testing.T) {
	ctx := context.Background()
	const accounts, transfers, workers = 10, 1_000, 50
	f := newFixture(t, accounts*1_000)
	ids := make([]uuid.UUID, accounts)
	for i := range ids {
		ids[i] = f.open(t, eur)
		f.transfer(t, f.settlement, ids[i], 1_000)
	}

	jobs := make(chan [2]uuid.UUID)
	var (
		mu       sync.Mutex
		ok, poor int
		failures []error
		wg       sync.WaitGroup
	)
	for range workers {
		wg.Go(func() {
			for pair := range jobs {
				amount := ledger.Money{Amount: 1 + mathrand.Int64N(300), Currency: eur}
				_, err := f.store.Transfer(ctx, f.partner.ID, pair[0], pair[1], amount, nil)
				mu.Lock()
				switch {
				case err == nil:
					ok++
				case errors.Is(err, ledger.ErrInsufficientFunds):
					poor++
				default:
					failures = append(failures, err)
				}
				mu.Unlock()
			}
		})
	}
	for range transfers / 2 {
		a, b := mathrand.IntN(accounts), mathrand.IntN(accounts-1)
		if b >= a {
			b++
		}
		jobs <- [2]uuid.UUID{ids[a], ids[b]}
		jobs <- [2]uuid.UUID{ids[b], ids[a]}
	}
	close(jobs)
	wg.Wait()

	for _, err := range failures {
		t.Errorf("unexpected error (code %q): %v", pgCode(err), err)
	}
	if ok+poor != transfers {
		t.Errorf("%d transfers ran, want %d", ok+poor, transfers)
	}
	t.Logf("%d posted, %d refused for insufficient funds", ok, poor)

	var total int64
	for _, id := range ids {
		b := f.balance(t, id)
		if b.Posted < 0 {
			t.Errorf("account %s went negative: %d", id, b.Posted)
		}
		total += b.Posted
	}
	if total != accounts*1_000 {
		t.Errorf("customers hold %d in total, want %d: money was created or lost", total, accounts*1_000)
	}
	var drift int
	if err := tdb.Owner.QueryRow(ctx, `
		SELECT count(*) FROM balances b JOIN accounts a ON a.id = b.account_id
		WHERE a.partner_id = $1
		  AND b.posted_minor <> (SELECT coalesce(sum(amount_minor), 0) FROM postings p WHERE p.account_id = b.account_id)`,
		f.partner.ID).Scan(&drift); err != nil {
		t.Fatal(err)
	}
	if drift != 0 {
		t.Errorf("%d accounts' posted balance differs from the sum of their postings", drift)
	}
}

// TestStoreAgreesWithTheBook runs random transfers against both the database and the in-memory Book (slice 1's
// reference model): every command must get the same outcome from both, and every balance must match after each step.
func TestStoreAgreesWithTheBook(t *testing.T) {
	ctx := context.Background()
	rapid.Check(t, func(rt *rapid.T) {
		limit := rapid.Int64Range(0, 10_000).Draw(rt, "funding limit")
		f := newFixture(t, limit)
		book := ledger.NewBook(store.NewID)
		bookSettlement, err := book.OpenAccount(f.partner.ID, ledger.KindSettlement, eur, -limit)
		if err != nil {
			rt.Fatal(err)
		}
		toBook := map[uuid.UUID]uuid.UUID{f.settlement: bookSettlement.ID}
		dbIDs := []uuid.UUID{f.settlement}
		for range rapid.IntRange(1, 4).Draw(rt, "customers") {
			id := f.open(t, eur)
			a, err := book.OpenAccount(f.partner.ID, ledger.KindCustomer, eur, 0)
			if err != nil {
				rt.Fatal(err)
			}
			toBook[id] = a.ID
			dbIDs = append(dbIDs, id)
		}
		for i := range rapid.IntRange(1, 20).Draw(rt, "steps") {
			from := rapid.SampledFrom(dbIDs).Draw(rt, "from")
			to := rapid.SampledFrom(dbIDs).Draw(rt, "to")
			amount := ledger.Money{Amount: rapid.Int64Range(0, 5_000).Draw(rt, "amount"), Currency: eur}

			_, dbErr := f.store.Transfer(ctx, f.partner.ID, from, to, amount, nil)
			_, bookErr := book.Transfer(f.partner.ID, toBook[from], toBook[to], amount)
			if (dbErr == nil) != (bookErr == nil) || (dbErr != nil && dbErr.Error() != bookErr.Error()) {
				rt.Fatalf("step %d: store says %v, book says %v", i, dbErr, bookErr)
			}
			for _, id := range dbIDs {
				_, want, _ := book.Account(f.partner.ID, toBook[id])
				if got := f.balance(t, id); got != want {
					rt.Fatalf("step %d, account %s: store %+v, book %+v", i, id, got, want)
				}
			}
		}
	})
}

func TestStatementPagesWithAKeysetCursor(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice := f.open(t, eur)
	for i := range 5 {
		f.transfer(t, f.settlement, alice, int64(10+i))
	}
	var got []int64
	var cursor store.Cursor
	for {
		page, err := f.store.Statement(ctx, f.partner.ID, alice, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, e := range page {
			got = append(got, e.Amount.Amount)
		}
		last := page[len(page)-1]
		cursor = store.Cursor{CreatedAt: last.CreatedAt, PostingID: last.PostingID}
	}
	want := []int64{10, 11, 12, 13, 14}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entries = %v, want %v", got, want)
		}
	}

	other := newFixture(t, 0)
	if _, err := other.store.Statement(ctx, other.partner.ID, alice, store.Cursor{}, 10); !errors.Is(err, ledger.ErrNotFound) {
		t.Errorf("another partner's statement: err = %v, want not_found", err)
	}
}
