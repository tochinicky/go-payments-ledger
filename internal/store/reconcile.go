package store

import (
	"context"
	"fmt"
)

// Check is one invariant the reconciliation verifies, with how many rows break it and a few examples.
type Check struct {
	Name     string
	Breaks   int
	Examples []string
}

// Report is the outcome of a reconciliation.
type Report struct {
	Checks []Check
}

// OK reports whether every invariant holds.
func (r Report) OK() bool {
	for _, c := range r.Checks {
		if c.Breaks > 0 {
			return false
		}
	}
	return true
}

// reconciliation is the set of invariant checks. Each query returns one text row per break (a description);
// a check passes when it returns none.
var reconciliation = []struct{ name, query string }{
	{"1. every transaction's postings sum to zero per currency", `
		SELECT transaction_id || ' ' || currency || ' sums to ' || sum(amount_minor)
		FROM postings GROUP BY transaction_id, currency HAVING sum(amount_minor) <> 0`},
	{"2. every posted balance equals the sum of the account's postings", `
		SELECT b.account_id || ': posted ' || b.posted_minor || ', postings sum to ' || coalesce(p.total, 0)
		FROM balances b LEFT JOIN (SELECT account_id, sum(amount_minor) AS total FROM postings GROUP BY account_id) p
		  ON p.account_id = b.account_id
		WHERE b.posted_minor <> coalesce(p.total, 0)`},
	{"3. every available balance is at or above the account's floor", `
		SELECT b.account_id || ': available ' || (b.posted_minor - b.held_minor) || ', floor ' || a.min_balance_minor
		FROM balances b JOIN accounts a ON a.id = b.account_id
		WHERE b.posted_minor - b.held_minor < a.min_balance_minor`},
	{"4a. every held balance equals the sum of the account's active holds", `
		SELECT b.account_id || ': held ' || b.held_minor || ', active holds sum to ' || coalesce(h.total, 0)
		FROM balances b LEFT JOIN (SELECT account_id, sum(amount_minor) AS total FROM holds WHERE status = 'active' GROUP BY account_id) h
		  ON h.account_id = b.account_id
		WHERE b.held_minor <> coalesce(h.total, 0)`},
	{"4b. every captured hold has its transaction, posting exactly the captured amount", `
		SELECT h.id || ': captured ' || h.captured_minor || ', its transaction moved ' || coalesce(p.moved, 0)
		FROM holds h LEFT JOIN (SELECT transaction_id, sum(amount_minor) FILTER (WHERE amount_minor > 0) AS moved
		                        FROM postings GROUP BY transaction_id) p ON p.transaction_id = h.transaction_id
		WHERE h.status = 'captured' AND (h.captured_minor > h.amount_minor OR coalesce(p.moved, 0) <> h.captured_minor)`},
	{"6a. every account has exactly one event per version (account_seq 1…version)", `
		SELECT b.account_id || ': version ' || b.version || ', ' || coalesce(o.n, 0) || ' events, highest seq ' || coalesce(o.top, 0)
		FROM balances b LEFT JOIN (SELECT account_id, count(*) AS n, max(account_seq) AS top FROM outbox GROUP BY account_id) o
		  ON o.account_id = b.account_id
		WHERE coalesce(o.n, 0) <> b.version OR coalesce(o.top, 0) <> b.version`},
	{"6b. every posting has its event, with the same account_seq", `
		SELECT p.id || ': account ' || p.account_id || ' seq ' || p.account_seq || ' has no event'
		FROM postings p LEFT JOIN outbox o ON o.account_id = p.account_id AND o.account_seq = p.account_seq
		WHERE o.id IS NULL`},
}

// unpublishedCheck is invariant 6's "eventually published", checked only when asked: right after traffic, a
// healthy relay may simply not have caught up yet.
const unpublishedCheck = `
	SELECT id || ': account ' || account_id || ' seq ' || account_seq || ' unpublished since ' || created_at
	FROM outbox WHERE published_at IS NULL AND created_at < now() - make_interval(secs => $1)`

// Reconcile recomputes the ledger from its source of truth (the postings, the holds and the outbox) and checks
// invariants 1–4 and 6 over the whole database. If publishedWithin > 0, every event older than that must also be
// published. It reads only; run it as any role that can SELECT the ledger's tables.
func (s *Store) Reconcile(ctx context.Context, publishedWithinSeconds float64) (Report, error) {
	var report Report
	run := func(name, query string, args ...any) error {
		rows, err := s.pool.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		defer rows.Close()
		c := Check{Name: name}
		for rows.Next() {
			var example string
			if err := rows.Scan(&example); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			c.Breaks++
			if len(c.Examples) < 5 {
				c.Examples = append(c.Examples, example)
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		report.Checks = append(report.Checks, c)
		return nil
	}
	for _, c := range reconciliation {
		if err := run(c.name, c.query); err != nil {
			return Report{}, err
		}
	}
	if publishedWithinSeconds > 0 {
		if err := run("6c. every event is published", unpublishedCheck, publishedWithinSeconds); err != nil {
			return Report{}, err
		}
	}
	return report, nil
}
