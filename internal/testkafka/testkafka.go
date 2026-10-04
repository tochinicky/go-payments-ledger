// Package testkafka starts a throwaway Redpanda broker (Testcontainers) for integration tests, with the events
// topic created and auto-creation off. The Kafka port is bound to a fixed host port, so the broker can be stopped
// and started again (the "Kafka down" scenario) and still advertise the address its clients know.
package testkafka

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/netip"
	"strconv"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tochinicky/go-payments-ledger/internal/events"
)

// Image is the pinned broker.
const Image = "docker.redpanda.com/redpandadata/redpanda:v26.2.3"

// Kafka is a running broker.
type Kafka struct {
	container *redpanda.Container
	Broker    string // host:port, the same before and after a restart
}

// Start runs Redpanda on a fixed random host port (retrying if the port is taken) and creates the topic.
func Start(ctx context.Context) (*Kafka, error) {
	var lastErr error
	for range 5 {
		port := strconv.Itoa(20000 + rand.IntN(20000)) //nolint:gosec // a test port, not a secret
		ctr, err := redpanda.Run(ctx, Image, testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			if hc.PortBindings == nil {
				hc.PortBindings = network.PortMap{}
			}
			hc.PortBindings[network.MustParsePort("9092/tcp")] = []network.PortBinding{{HostIP: netip.IPv4Unspecified(), HostPort: port}}
		}))
		if err != nil {
			lastErr = err
			if ctr != nil {
				_ = ctr.Terminate(ctx)
			}
			continue
		}
		k := &Kafka{container: ctr}
		if k.Broker, err = ctr.KafkaSeedBroker(ctx); err != nil {
			return nil, err
		}
		cl, err := k.Client()
		if err != nil {
			return nil, err
		}
		defer cl.Close()
		if err := events.CreateTopic(ctx, cl, 1); err != nil {
			return nil, err
		}
		return k, nil
	}
	return nil, fmt.Errorf("start redpanda: %w", lastErr)
}

// Client returns a new client for the broker; the caller closes it.
func (k *Kafka) Client(opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(k.Broker)}, opts...)...)
}

// Stop stops the broker (connections are refused until Start).
func (k *Kafka) Stop(ctx context.Context) error {
	timeout := 10 * time.Second
	return k.container.Stop(ctx, &timeout)
}

// Restart starts a stopped broker again, on the same port, and waits until it serves.
//
// The module's entrypoint waits for a "# Injected by testcontainers" marker in redpanda.yaml before it starts
// Redpanda, but Redpanda rewrites that file on its first start and drops the comment, so a restarted container
// would wait forever. Restart puts the marker back.
func (k *Kafka) Restart(ctx context.Context) error {
	if err := k.container.Start(ctx); err != nil {
		return err
	}
	code, out, err := k.container.Exec(ctx, []string{"sh", "-c", `echo "# Injected by testcontainers" >> /etc/redpanda/redpanda.yaml`})
	if err != nil {
		return fmt.Errorf("re-inject the config marker: %w", err)
	}
	if code != 0 {
		b, _ := io.ReadAll(out)
		return fmt.Errorf("re-inject the config marker: exit %d: %s", code, b)
	}
	cl, err := k.Client()
	if err != nil {
		return err
	}
	defer cl.Close()
	deadline := time.Now().Add(60 * time.Second)
	for {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := cl.Ping(pctx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("broker not serving after restart: %w", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Logs returns the broker's container log, for diagnosing a failed test.
func (k *Kafka) Logs(ctx context.Context) string {
	r, err := k.container.Logs(ctx)
	if err != nil {
		return err.Error()
	}
	defer func() { _ = r.Close() }()
	b, _ := io.ReadAll(r)
	return string(b)
}

// Terminate removes the broker.
func (k *Kafka) Terminate(ctx context.Context) error {
	return k.container.Terminate(ctx)
}
