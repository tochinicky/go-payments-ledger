package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/ledger"
	"github.com/tochinicky/go-payments-ledger/internal/store"
)

var hashA, hashB = []byte("request A"), []byte("request B")

func (f fixture) claim(t *testing.T, key string, hash []byte) store.Claim {
	t.Helper()
	c, err := f.store.ClaimKey(context.Background(), f.partner.ID, key, hash)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// complete runs a transfer of 1 from settlement to account under the lease and stores the response.
func (f fixture) complete(t *testing.T, lease store.Lease, to uuid.UUID) error {
	t.Helper()
	_, err := f.store.RunWithLease(context.Background(), lease, func(tx store.Tx) (store.Response, error) {
		if _, err := tx.Transfer(context.Background(), f.partner.ID, f.settlement, to, ledger.Money{Amount: 1, Currency: eur}, nil); err != nil {
			return store.Response{}, err
		}
		return store.Response{Status: 201, Body: []byte(`{"ok":true}`)}, nil
	})
	return err
}

func (f fixture) transactions(t *testing.T) int {
	t.Helper()
	var n int
	if err := tdb.Owner.QueryRow(context.Background(), "SELECT count(*) FROM transactions WHERE partner_id = $1", f.partner.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// expireLease moves a claim's lease into the past, as if its attempt had crashed 31 seconds ago.
func (f fixture) expireLease(t *testing.T, key string) {
	t.Helper()
	if _, err := tdb.Owner.Exec(context.Background(),
		"UPDATE idempotency SET locked_until = now() - interval '1 second' WHERE partner_id = $1 AND key = $2", f.partner.ID, key); err != nil {
		t.Fatal(err)
	}
}

func TestClaimKeyLifecycle(t *testing.T) {
	f := newFixture(t, 1_000)
	alice := f.open(t, eur)

	first := f.claim(t, "k1", hashA)
	if first.Result != store.Claimed {
		t.Fatalf("new key: %v, want Claimed", first.Result)
	}
	if got := f.claim(t, "k1", hashA).Result; got != store.InProgress {
		t.Errorf("same key while running: %v, want InProgress", got)
	}
	if got := f.claim(t, "k1", hashB).Result; got != store.KeyReused {
		t.Errorf("different request while running: %v, want KeyReused", got)
	}
	if err := f.complete(t, first.Lease, alice); err != nil {
		t.Fatal(err)
	}
	replay := f.claim(t, "k1", hashA)
	if replay.Result != store.Replay || replay.Response.Status != 201 || string(replay.Response.Body) != `{"ok":true}` {
		t.Errorf("after completion: %+v, want the stored 201", replay)
	}
	if got := f.claim(t, "k1", hashB).Result; got != store.KeyReused {
		t.Errorf("different request after completion: %v, want KeyReused", got)
	}
	// Keys are per partner: another partner's "k1" is a different key.
	if got := newFixture(t, 0).claim(t, "k1", hashB).Result; got != store.Claimed {
		t.Errorf("another partner's same key: %v, want Claimed", got)
	}
	if n := f.transactions(t); n != 1 {
		t.Errorf("%d transactions, want 1", n)
	}
}

// A crashed attempt's lease lapses and a retry takes over. If the crashed attempt was in fact only slow and comes
// back, its fence fails: it can't post, so the money moves once.
func TestLapsedLeaseIsTakenOverAndTheOldAttemptIsFenced(t *testing.T) {
	f := newFixture(t, 1_000)
	alice := f.open(t, eur)
	slow := f.claim(t, "k", hashA)
	f.expireLease(t, "k")

	retry := f.claim(t, "k", hashA)
	if retry.Result != store.Claimed || retry.Lease.Token == slow.Lease.Token {
		t.Fatalf("after the lease lapsed: %+v, want a new claim", retry)
	}
	if err := f.complete(t, slow.Lease, alice); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("the slow attempt: err = %v, want ErrLeaseLost", err)
	}
	if err := f.complete(t, retry.Lease, alice); err != nil {
		t.Fatal(err)
	}
	if n := f.transactions(t); n != 1 {
		t.Errorf("%d transactions, want 1", n)
	}
}

// A takeover waits for an attempt that is inside its ledger transaction (it holds the key's row lock), then sees
// the completed key and replays it instead of running the work a second time.
func TestTakeoverWaitsForAnAttemptStillInItsTransaction(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice := f.open(t, eur)
	first := f.claim(t, "k", hashA)
	f.expireLease(t, "k") // the lease lapses, but the first attempt carries on: it's slow, not dead

	inside, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := f.store.RunWithLease(ctx, first.Lease, func(tx store.Tx) (store.Response, error) {
			if _, err := tx.Transfer(ctx, f.partner.ID, f.settlement, alice, ledger.Money{Amount: 1, Currency: eur}, nil); err != nil {
				return store.Response{}, err
			}
			close(inside)
			<-finish
			return store.Response{Status: 201, Body: []byte(`{}`)}, nil
		})
		done <- err
	}()
	<-inside
	time.AfterFunc(200*time.Millisecond, func() { close(finish) })

	retry := f.claim(t, "k", hashA) // blocks in TakeOverKey until the first attempt commits, then replays
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if retry.Result != store.Replay {
		t.Fatalf("retry: %v, want Replay", retry.Result)
	}
	if n := f.transactions(t); n != 1 {
		t.Errorf("%d transactions, want 1", n)
	}
}

// Freeing a key after a failure is conditional: it deletes only our own in-progress claim. A completed key (the
// "commit outcome unknown" case: the commit happened, only its acknowledgement was lost) stays, and so does a
// claim another attempt has taken over.
func TestReleaseKeyOnlyFreesOwnInProgressClaim(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice := f.open(t, eur)

	failed := f.claim(t, "free-me", hashA)
	if err := f.store.ReleaseKey(ctx, failed.Lease); err != nil {
		t.Fatal(err)
	}
	if got := f.claim(t, "free-me", hashA).Result; got != store.Claimed {
		t.Errorf("after release: %v, want Claimed (the retry runs)", got)
	}

	done := f.claim(t, "completed", hashA)
	if err := f.complete(t, done.Lease, alice); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ReleaseKey(ctx, done.Lease); err != nil {
		t.Fatal(err)
	}
	if got := f.claim(t, "completed", hashA).Result; got != store.Replay {
		t.Errorf("release after commit: %v, want Replay (the stored answer survives)", got)
	}

	old := f.claim(t, "taken", hashA)
	f.expireLease(t, "taken")
	f.claim(t, "taken", hashA) // a new attempt takes over
	if err := f.store.ReleaseKey(ctx, old.Lease); err != nil {
		t.Fatal(err)
	}
	if got := f.claim(t, "taken", hashA).Result; got != store.InProgress {
		t.Errorf("stale release: %v, want InProgress (the new attempt keeps its claim)", got)
	}
}

func TestCleanupDeletesOnlyExpiredKeys(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice := f.open(t, eur)
	for _, key := range []string{"old", "recent"} {
		c := f.claim(t, key, hashA)
		if err := f.complete(t, c.Lease, alice); err != nil {
			t.Fatal(err)
		}
	}
	f.claim(t, "abandoned", hashA)
	f.claim(t, "running", hashA)
	// Age the keys (the database clock decides, so move the rows rather than the clock).
	for _, stmt := range []string{
		"UPDATE idempotency SET completed_at = now() - interval '25 hours' WHERE partner_id = $1 AND key = 'old'",
		"UPDATE idempotency SET locked_until = now() - interval '25 hours' WHERE partner_id = $1 AND key = 'abandoned'",
	} {
		if _, err := tdb.Owner.Exec(ctx, stmt, f.partner.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.store.CleanupKeys(ctx, 24*time.Hour, 1); err != nil { // batch of 1: the loop must keep going
		t.Fatal(err)
	}
	rows, err := tdb.Owner.Query(ctx, "SELECT key FROM idempotency WHERE partner_id = $1 ORDER BY key", f.partner.ID)
	if err != nil {
		t.Fatal(err)
	}
	var left []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		left = append(left, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(left) != 2 || left[0] != "recent" || left[1] != "running" {
		t.Errorf("keys left = %v, want [recent running]", left)
	}
}
