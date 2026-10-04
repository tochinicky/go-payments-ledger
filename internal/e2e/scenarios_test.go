package e2e_test

import (
	"context"
	"testing"
	"time"
)

// Scenario 2, crash between publish and mark: the relay process dies after Kafka acknowledged a batch but before
// it marked the rows published. A restarted relay publishes the batch again; the notifier processes each event
// once (the duplicates are in the topic, and the inbox skips them).
func TestScenario2RelayCrashAfterPublish(t *testing.T) {
	p := newPartner(t, 3)
	p.traffic(t, 40)
	events := p.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1")

	crash(t, "relay", "relay.after_produce", "DATABASE_URL="+tdb.AppDSN)
	if unpublished := p.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1 AND published_at IS NULL"); unpublished != events {
		t.Fatalf("%d of %d events unpublished after the crash: the crash came too late", unpublished, events)
	}

	startRelay(t)
	n := startNotifier(t)
	p.waitDelivered(t, 60*time.Second)
	p.checkDelivery(t)
	if copies := p.topicCopies(t); copies < 2*events {
		t.Errorf("%d copies of %d events in the topic: the crashed batch wasn't really published first", copies, events)
	}
	if s := n.Stats(); s.Duplicates == 0 {
		t.Errorf("notifier stats %+v: it never saw the duplicates the crash produced", s)
	}
}

// Scenario 3, Kafka down: with the broker stopped, transfers keep succeeding and the outbox grows; once the broker
// is back, every event is delivered in per-account order with no gaps.
func TestScenario3BrokerDown(t *testing.T) {
	ctx := context.Background()
	startRelay(t)
	startNotifier(t)
	p := newPartner(t, 3)
	p.traffic(t, 20)
	p.waitDelivered(t, 60*time.Second)

	if err := kafka.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := false
	t.Cleanup(func() {
		if !restarted {
			_ = kafka.Restart(ctx)
		}
	})
	p.traffic(t, 40) // the ledger doesn't notice
	time.Sleep(5 * time.Second)
	if waiting := p.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1 AND published_at IS NULL"); waiting == 0 {
		t.Fatal("events were published while the broker was down")
	}
	if err := kafka.Restart(ctx); err != nil {
		t.Fatal(err)
	}
	restarted = true
	p.waitDelivered(t, 120*time.Second)
	p.checkDelivery(t)
}

// Scenario 4, consumer crash between the DB commit and the offset commit: the notifier process dies after
// recording a batch but before committing its offsets. The group hands the partitions to a new member, which gets
// the batch again; the inbox makes the second delivery a no-op, so there is exactly one notification per event.
func TestScenario4NotifierCrashBeforeOffsetCommit(t *testing.T) {
	startRelay(t)
	p := newPartner(t, 3)
	p.traffic(t, 80) // more events than one poll (100), so the crash comes partway through the stream
	events := p.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1")
	eventually(t, 60*time.Second, "not every event was published", func() bool {
		return p.count(t, "SELECT count(*) FROM outbox WHERE partner_id = $1 AND published_at IS NULL") == 0
	})

	crash(t, "notifier", "notifier.after_db_commit", "DATABASE_URL="+notifierDSN, "SESSION_TIMEOUT="+sessionTimeout.String())
	before := p.count(t, "SELECT count(*) FROM notifier.notifications WHERE partner_id = $1")
	if before == 0 || before == events {
		t.Fatalf("the crashed notifier recorded %d of %d events: the crash should come partway through", before, events)
	}

	// The new member joins once the group gives up on the crashed one (its session timeout), gets the uncommitted
	// batch again, skips it, and records the rest.
	n := startNotifier(t)
	eventually(t, 60*time.Second, "the new member didn't finish", func() bool {
		s := n.Stats()
		return s.Duplicates == int64(before) && s.Processed == int64(events-before)
	})
	p.waitDelivered(t, 10*time.Second)
	p.checkDelivery(t)
}
