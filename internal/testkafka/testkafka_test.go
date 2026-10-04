package testkafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tochinicky/go-payments-ledger/internal/events"
	"github.com/tochinicky/go-payments-ledger/internal/testkafka"
)

// The "Kafka down" scenario depends on this: a stopped broker comes back on the same address and serves again.
func TestBrokerServesAgainAfterRestart(t *testing.T) {
	ctx := context.Background()
	k, err := testkafka.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = k.Terminate(ctx) }()
	produce := func() error {
		cl, err := k.Client()
		if err != nil {
			return err
		}
		defer cl.Close()
		pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		return cl.ProduceSync(pctx, &kgo.Record{Topic: events.Topic, Key: []byte("k"), Value: []byte("v")}).FirstErr()
	}
	if err := produce(); err != nil {
		t.Fatal(err)
	}
	if err := k.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := k.Restart(ctx); err != nil {
		t.Fatal(err)
	}
	if err := produce(); err != nil {
		t.Fatalf("after restart: %v\n%s", err, k.Logs(ctx))
	}
}
