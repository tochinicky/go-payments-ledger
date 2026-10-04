// Package relay publishes the outbox to Kafka. Exactly one relay is active at a time: each replica tries to take a
// Postgres advisory lock on a dedicated connection, and only the holder publishes. The lock lives exactly as long
// as that connection, so a crashed or partitioned leader loses it, and a standby takes over.
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tochinicky/go-payments-ledger/internal/events"
	"github.com/tochinicky/go-payments-ledger/internal/relay/relaydb"
)

// lockKey is the advisory lock that elects the active relay.
const lockKey int64 = 0x6c65646765725f72 // "ledger_r"

// Config configures a relay.
type Config struct {
	DatabaseURL    string // the app's login role; a connection of its own, outside any pool
	Kafka          *kgo.Client
	BatchSize      int32         // outbox rows per publish (default 500)
	PublishTimeout time.Duration // how long one batch may wait for Kafka (default 10 s)
	Idle           time.Duration // pause when the outbox is empty (default 100 ms); also the lock connection's health check
	Retry          time.Duration // pause after a failure, and between leadership attempts (default 1 s)
	Log            *slog.Logger
}

func (c *Config) defaults() {
	if c.BatchSize == 0 {
		c.BatchSize = 500
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = 10 * time.Second
	}
	if c.Idle == 0 {
		c.Idle = 100 * time.Millisecond
	}
	if c.Retry == 0 {
		c.Retry = time.Second
	}
}

// Run publishes until ctx ends: it connects, competes for leadership, publishes while it leads, and starts over
// whenever its connection fails.
func Run(ctx context.Context, cfg Config) error {
	cfg.defaults()
	for ctx.Err() == nil {
		if err := session(ctx, cfg); err != nil && ctx.Err() == nil {
			cfg.Log.WarnContext(ctx, "relay session ended; reconnecting", slog.Any("error", err))
			sleep(ctx, cfg.Retry)
		}
	}
	return nil
}

// session is one connection's life: standby until the lock is free, then leader until something fails. Closing
// the connection (on any return) releases the lock, so the next leader can start at once.
func session(ctx context.Context, cfg Config) error {
	conn, err := pgx.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	for {
		var leader bool
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockKey).Scan(&leader); err != nil {
			return fmt.Errorf("try lock: %w", err)
		}
		if leader {
			cfg.Log.InfoContext(ctx, "relay is the leader")
			return lead(ctx, cfg, relaydb.New(conn))
		}
		if !sleep(ctx, cfg.Retry) {
			return nil
		}
	}
}

// lead publishes batches in seq order. Every query runs on the lock's connection, so if that connection is lost
// the next query fails and the relay steps down at once, before it could publish without the lock.
func lead(ctx context.Context, cfg Config, q *relaydb.Queries) error {
	for ctx.Err() == nil {
		published, failed, err := publish(ctx, cfg, q)
		if err != nil {
			return err
		}
		switch {
		case failed > 0:
			cfg.Log.WarnContext(ctx, "kafka refused records; retrying", slog.Int("failed", failed))
			sleep(ctx, cfg.Retry)
		case published == 0:
			sleep(ctx, cfg.Idle)
		}
	}
	return nil
}

// publish sends one batch and marks the records Kafka acknowledged. A record that fails stays unpublished and is
// sent again next time; the idempotent producer fails every later record of the same partition with it, so an
// account's events never go out with a gap. A crash between the ack and the mark re-sends the batch: consumers
// deduplicate on event_id.
func publish(ctx context.Context, cfg Config, q *relaydb.Queries) (published, failed int, err error) {
	rows, err := q.Unpublished(ctx, cfg.BatchSize)
	if err != nil {
		return 0, 0, fmt.Errorf("read outbox: %w", err)
	}
	if len(rows) == 0 {
		return 0, 0, nil
	}
	recs := make([]*kgo.Record, len(rows))
	for i, r := range rows {
		recs[i] = &kgo.Record{Topic: events.Topic, Key: []byte(r.AccountID.String()), Value: r.Payload}
	}
	produceCtx, cancel := context.WithTimeout(ctx, cfg.PublishTimeout)
	results := cfg.Kafka.ProduceSync(produceCtx, recs...)
	cancel()
	fault("relay.after_produce")
	acked := make([]uuid.UUID, 0, len(rows))
	for i, res := range results {
		if res.Err != nil {
			failed++
			continue
		}
		acked = append(acked, rows[i].ID)
	}
	if len(acked) > 0 {
		if err := q.MarkPublished(ctx, acked); err != nil {
			return 0, failed, fmt.Errorf("mark published: %w", err)
		}
	}
	return len(acked), failed, nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
