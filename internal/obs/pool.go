package obs

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// ObservePool exports a connection pool's statistics: how many connections it has and may have, and how often
// (and for how long, in total) requests waited for one. A growing wait is the first sign the pool is too small.
func ObservePool(pool *pgxpool.Pool, name string) error {
	m := otel.Meter("github.com/tochinicky/go-payments-ledger/internal/obs")
	acquires, err := m.Int64ObservableCounter("db.pool.acquires", metric.WithDescription("Connections taken from the pool."))
	if err != nil {
		return err
	}
	waits, err := m.Int64ObservableCounter("db.pool.waits", metric.WithDescription("Acquires that had to wait: no idle connection."))
	if err != nil {
		return err
	}
	waitTime, err := m.Float64ObservableCounter("db.pool.wait_time", metric.WithUnit("s"), metric.WithDescription("Total time spent acquiring connections."))
	if err != nil {
		return err
	}
	inUse, err := m.Int64ObservableGauge("db.pool.in_use", metric.WithDescription("Connections in use now."))
	if err != nil {
		return err
	}
	size, err := m.Int64ObservableGauge("db.pool.max", metric.WithDescription("The pool's maximum size."))
	if err != nil {
		return err
	}
	attrs := metric.WithAttributes(attrPool(name))
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := pool.Stat()
		o.ObserveInt64(acquires, s.AcquireCount(), attrs)
		o.ObserveInt64(waits, s.EmptyAcquireCount(), attrs)
		o.ObserveFloat64(waitTime, s.AcquireDuration().Seconds(), attrs)
		o.ObserveInt64(inUse, int64(s.AcquiredConns()), attrs)
		o.ObserveInt64(size, int64(s.MaxConns()), attrs)
		return nil
	}, acquires, waits, waitTime, inUse, size)
	return err
}
