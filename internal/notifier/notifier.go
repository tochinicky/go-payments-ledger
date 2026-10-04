// Package notifier is a sample downstream service: it consumes ledger events and records one notification per
// event, exactly once. Each event is processed in one database transaction that also inserts its inbox row, and
// the Kafka offset is committed only after that transaction commits. A crash in between redelivers the event, and
// the inbox row makes the second delivery a no-op.
package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/tochinicky/go-payments-ledger/internal/events"
	"github.com/tochinicky/go-payments-ledger/internal/notifier/notifierdb"
)

var meter = otel.Meter("github.com/tochinicky/go-payments-ledger/internal/notifier")

// The stream's health as metrics: gaps and regressions must stay at zero.
var (
	processedTotal   = must(meter.Int64Counter("notifier.processed", metric.WithDescription("Events turned into notifications.")))
	duplicatesTotal  = must(meter.Int64Counter("notifier.duplicates", metric.WithDescription("Redeliveries skipped by the inbox.")))
	gapsTotal        = must(meter.Int64Counter("notifier.gaps", metric.WithDescription("Events whose account_seq skipped ahead.")))
	regressionsTotal = must(meter.Int64Counter("notifier.regressions", metric.WithDescription("Events whose account_seq went back.")))
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// Consumer is the inbox's consumer name and the Kafka consumer group.
const Consumer = "notifier"

// Config configures a notifier.
type Config struct {
	Pool           *pgxpool.Pool // the notifier's own role: no access to the ledger's tables
	Brokers        []string
	SessionTimeout time.Duration // how soon the group notices a crashed member (default 45 s)
	Log            *slog.Logger
}

// Notifier consumes the ledger's events.
type Notifier struct {
	cfg         Config
	processed   atomic.Int64
	duplicates  atomic.Int64
	gaps        atomic.Int64
	regressions atomic.Int64
}

// New returns a notifier.
func New(cfg Config) *Notifier {
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = 45 * time.Second
	}
	return &Notifier{cfg: cfg}
}

// Stats counts what the notifier has seen since it started.
type Stats struct {
	Processed   int64 // events turned into notifications
	Duplicates  int64 // redeliveries skipped by the inbox
	Gaps        int64 // events whose account_seq skipped ahead: an event is missing
	Regressions int64 // events whose account_seq went back: out of order
}

// Stats returns the counters. Gaps and regressions must stay at zero; they are the stream's health metric.
func (n *Notifier) Stats() Stats {
	return Stats{n.processed.Load(), n.duplicates.Load(), n.gaps.Load(), n.regressions.Load()}
}

// Run consumes until ctx ends. A processing failure is returned: the offsets of the failed batch stay uncommitted,
// so a restarted notifier gets those events again.
func (n *Notifier) Run(ctx context.Context) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(n.cfg.Brokers...),
		kgo.ConsumerGroup(Consumer),
		kgo.ConsumeTopics(events.Topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(), // never commit offsets for a partition this member no longer owns
		kgo.SessionTimeout(n.cfg.SessionTimeout),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()
	// With BlockRebalanceOnPoll, every poll must be followed by AllowRebalance, including the last one: otherwise
	// Close waits forever to leave the group. (Deferred after Close, so it runs first.)
	defer cl.AllowRebalance()
	for {
		fetches := cl.PollRecords(ctx, 100)
		if ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			n.cfg.Log.WarnContext(ctx, "fetch failed", slog.String("topic", topic), slog.Int("partition", int(partition)), slog.Any("error", err))
		})
		records := fetches.Records()
		// A batch already polled is finished even if a stop arrives meanwhile: its transaction and the offset
		// commit run on a context the stop doesn't cancel, so a SIGTERM never leaves a half-done batch to redeliver.
		batch := context.WithoutCancel(ctx)
		if len(records) > 0 {
			if err := n.processBatch(batch, records); err != nil {
				return err
			}
		}
		fault("notifier.after_db_commit")
		if len(records) > 0 {
			if err := cl.CommitRecords(batch, records...); err != nil {
				// Not fatal: the events are already recorded, and a redelivery is absorbed by the inbox.
				n.cfg.Log.WarnContext(ctx, "offset commit failed", slog.Any("error", err))
			}
		}
		cl.AllowRebalance()
	}
}

// processBatch records a polled batch in one database transaction: each event's inbox row, sequence check and
// notification. One commit per batch instead of one per event: the commit (its WAL flush) is the expensive part,
// and the guarantee is the same, since the inbox rows and the notifications still commit together, before the
// offsets. If anything fails, nothing of the batch is recorded and it is redelivered.
func (n *Notifier) processBatch(ctx context.Context, records []*kgo.Record) error {
	var s Stats
	err := pgx.BeginFunc(ctx, n.cfg.Pool, func(tx pgx.Tx) error {
		s = Stats{}
		q := notifierdb.New(tx)
		for _, rec := range records {
			if err := n.process(ctx, q, rec, &s); err != nil {
				return fmt.Errorf("event at %s/%d@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Counted only once committed, so a rolled-back batch doesn't count twice when it comes back.
	n.processed.Add(s.Processed)
	n.duplicates.Add(s.Duplicates)
	n.gaps.Add(s.Gaps)
	n.regressions.Add(s.Regressions)
	processedTotal.Add(ctx, s.Processed)
	duplicatesTotal.Add(ctx, s.Duplicates)
	gapsTotal.Add(ctx, s.Gaps)
	regressionsTotal.Add(ctx, s.Regressions)
	return nil
}

// process records one event inside the batch's transaction: inbox row, sequence check and notification.
func (n *Notifier) process(ctx context.Context, q *notifierdb.Queries, rec *kgo.Record, s *Stats) error {
	var e events.Event
	if err := json.Unmarshal(rec.Value, &e); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	inserted, err := q.InsertInbox(ctx, notifierdb.InsertInboxParams{Consumer: Consumer, EventID: e.EventID})
	if err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	if inserted == 0 {
		s.Duplicates++
		return nil
	}
	previous, err := q.LastSeq(ctx, e.AccountID)
	if errors.Is(err, pgx.ErrNoRows) {
		previous, err = 0, nil // the account's first event
	}
	if err != nil {
		return fmt.Errorf("progress: %w", err)
	}
	if err := q.SetLastSeq(ctx, notifierdb.SetLastSeqParams{AccountID: e.AccountID, LastSeq: e.AccountSeq}); err != nil {
		return fmt.Errorf("progress: %w", err)
	}
	switch {
	case e.AccountSeq <= previous:
		s.Regressions++
		n.cfg.Log.ErrorContext(ctx, "account_seq went back", slog.String("account", e.AccountID.String()), slog.Int64("seq", e.AccountSeq), slog.Int64("previous", previous))
	case e.AccountSeq > previous+1:
		s.Gaps++
		n.cfg.Log.ErrorContext(ctx, "account_seq skipped ahead", slog.String("account", e.AccountID.String()), slog.Int64("seq", e.AccountSeq), slog.Int64("previous", previous))
	}
	if err := q.InsertNotification(ctx, notifierdb.InsertNotificationParams{
		ID: uuid.Must(uuid.NewV7()), EventID: e.EventID, PartnerID: e.PartnerID, AccountID: e.AccountID,
		Kind: e.EventType, AmountMinor: e.AmountMinor, AccountSeq: e.AccountSeq,
	}); err != nil {
		return fmt.Errorf("notification: %w", err)
	}
	s.Processed++
	return nil
}
