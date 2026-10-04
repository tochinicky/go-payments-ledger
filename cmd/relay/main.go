// Command relay publishes the ledger's outbox to Kafka. Run several for availability: one leads, the rest wait.
//
//	DATABASE_URL       the app's login role (the relay uses one connection of its own)
//	KAFKA_BROKERS      comma-separated seed brokers
//	ADMIN_ADDR         /metrics and /healthz (default :9091)
//	SHUTDOWN_TIMEOUT   how long a stop waits for an in-flight publish (default 15s)
//
//	relay                serve
//	relay create-topic   create the events topic with its fixed partition count (REPLICATION_FACTOR, default 1)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tochinicky/go-payments-ledger/internal/events"
	"github.com/tochinicky/go-payments-ledger/internal/obs"
	"github.com/tochinicky/go-payments-ledger/internal/relay"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger, os.Args[1:]); err != nil {
		logger.Error("relay stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger, args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		return errors.New("KAFKA_BROKERS is not set")
	}
	// The idempotent producer is franz-go's default: acks=all, retries without duplicates, per-partition order.
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}

	if len(args) > 0 && args[0] == "create-topic" {
		defer cl.Close()
		rf := int16(1)
		if v := os.Getenv("REPLICATION_FACTOR"); v != "" {
			n, err := strconv.ParseInt(v, 10, 16)
			if err != nil {
				return fmt.Errorf("REPLICATION_FACTOR %q: %w", v, err)
			}
			rf = int16(n)
		}
		return events.CreateTopic(ctx, cl, rf)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is not set")
	}
	shutdownTimeout := 15 * time.Second
	if v := os.Getenv("SHUTDOWN_TIMEOUT"); v != "" {
		if shutdownTimeout, err = time.ParseDuration(v); err != nil {
			return fmt.Errorf("SHUTDOWN_TIMEOUT %q: %w", v, err)
		}
	}
	telemetry, err := obs.Setup(ctx, "relay")
	if err != nil {
		return err
	}
	admin := &http.Server{Addr: envOr("ADMIN_ADDR", ":9091"), Handler: telemetry.AdminHandler(&obs.Health{}), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = admin.ListenAndServe() }()

	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx, relay.Config{DatabaseURL: dsn, Kafka: cl, Log: logger}) }()
	<-ctx.Done()
	logger.Info("relay shutting down")
	// A publish already sent to Kafka can't be cancelled (the idempotent producer waits for its outcome), so the
	// stop is bounded: if Kafka doesn't answer in time, exit anyway. Rows Kafka acknowledged but the relay didn't
	// mark are simply published again by the next leader, and consumers deduplicate them.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	select {
	case err = <-done:
		cl.Close()
	case <-shutdownCtx.Done():
		logger.Warn("relay stop timed out waiting for Kafka; exiting")
	}
	return errors.Join(err, admin.Shutdown(shutdownCtx), telemetry.Shutdown(shutdownCtx))
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
