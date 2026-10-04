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
}

// DefaultTimeouts are the production values.
var DefaultTimeouts = Timeouts{Request: 10 * time.Second, Statement: 8 * time.Second, Lock: 5 * time.Second}

// Validate refuses an order that would let a request outlive its lease: LeaseDuration > Request > Statement ≥ Lock > 0.
func (t Timeouts) Validate() error {
	nested := LeaseDuration > t.Request && t.Request > t.Statement && t.Statement >= t.Lock && t.Lock > 0
	if !nested {
		return fmt.Errorf("timeouts must satisfy lease (%s) > request (%s) > statement (%s) ≥ lock (%s) > 0",
			LeaseDuration, t.Request, t.Statement, t.Lock)
	}
	return nil
}

// Apply sets statement_timeout and lock_timeout on every connection the pool opens.
func (t Timeouts) Apply(cfg *pgxpool.Config) {
	params := cfg.ConnConfig.RuntimeParams
	params["statement_timeout"] = strconv.FormatInt(t.Statement.Milliseconds(), 10)
	params["lock_timeout"] = strconv.FormatInt(t.Lock.Milliseconds(), 10)
}
