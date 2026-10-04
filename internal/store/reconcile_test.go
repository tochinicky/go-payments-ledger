package store_test

import (
	"context"
	"strings"
	"testing"
)

// Reconciliation finds a balance that no longer matches its postings (here, edited behind the ledger's back) and
// names the account. Other tests in this package corrupt rows on purpose, so this one looks for its own account
// rather than a clean report; the end-to-end tests require a fully clean one.
func TestReconcileFindsABalanceThatDisagreesWithItsPostings(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1_000)
	alice := f.open(t, eur)
	f.transfer(t, f.settlement, alice, 100)

	found := func() bool {
		report, err := f.store.Reconcile(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range report.Checks {
			if strings.HasPrefix(c.Name, "2.") {
				for _, e := range c.Examples {
					if strings.HasPrefix(e, alice.String()) {
						return true
					}
				}
			}
		}
		return false
	}
	if found() {
		t.Fatal("a correct balance was reported")
	}
	if _, err := tdb.Owner.Exec(ctx, "UPDATE balances SET posted_minor = posted_minor + 1 WHERE account_id = $1", alice); err != nil {
		t.Fatal(err)
	}
	if !found() {
		t.Fatal("reconciliation missed a balance one cent off its postings")
	}
}
