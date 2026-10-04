package store

import (
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Timeouts bound one request at every layer, each inside the one above it: the database gives up on a lock wait,
// then on a statement, before the request's deadline, and the request ends well before its idempotency lease. So a
// lease always outlives the attempt that holds it, by construction rather than by luck.
type Timeouts struct {
	Request   time.Duration // the HTTP request's context deadline
	Statement time.Duration // Postgres statement_timeout on the app's connections
	Lock      time.Duration // Postgres lock_timeout: how long one statement may wait for a row lock
	// IdleInTransaction is Postgres idle_in_transaction_session_timeout: a backstop that ends a session left idle
	// inside an open transaction (a leaked transaction would otherwise hold its balance row locks indefinitely).
	// Longer than Request, so it never cuts off a live request.
	IdleInTransaction time.Duration
}

// DefaultTimeouts are the production values.
var DefaultTimeouts = Timeouts{
	Request: 10 * time.Second, Statement: 8 * time.Second, Lock: 5 * time.Second, IdleInTransaction: 15 * time.Second,
}

// Validate refuses an order that would let a request outlive its lease: LeaseDuration > Request > Statement ≥ Lock > 0.
// statement_timeout and lock_timeout bound each statement, not a transaction of several; the request deadline is
// what bounds the whole transaction, so "lease > request" is the relation that keeps a lease alive. The idle
// backstop must be longer than a request.
func (t Timeouts) Validate() error {
	nested := LeaseDuration > t.Request && t.Request > t.Statement && t.Statement >= t.Lock && t.Lock > 0
	if !nested {
		return fmt.Errorf("timeouts must satisfy lease (%s) > request (%s) > statement (%s) ≥ lock (%s) > 0",
			LeaseDuration, t.Request, t.Statement, t.Lock)
	}
	if t.IdleInTransaction <= t.Request {
		return fmt.Errorf("idle-in-transaction timeout (%s) must be longer than the request timeout (%s)", t.IdleInTransaction, t.Request)
	}
	return nil
}

// Apply sets the database timeouts on every connection the pool opens. Everything sharing the pool inherits them:
// a later background job that needs a long statement or lock wait must use a pool or session of its own.
func (t Timeouts) Apply(cfg *pgxpool.Config) {
	params := cfg.ConnConfig.RuntimeParams
	params["statement_timeout"] = strconv.FormatInt(t.Statement.Milliseconds(), 10)
	params["lock_timeout"] = strconv.FormatInt(t.Lock.Milliseconds(), 10)
	params["idle_in_transaction_session_timeout"] = strconv.FormatInt(t.IdleInTransaction.Milliseconds(), 10)
}
