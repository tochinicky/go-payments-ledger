// Command relay publishes the ledger's outbox to Kafka. Run several for availability: one leads, the rest wait.
//
//	DATABASE_URL    the app's login role (the relay uses one connection of its own)
//	KAFKA_BROKERS   comma-separated seed brokers
//
//	relay                serve
//	relay create-topic   create the events topic with its fixed partition count (REPLICATION_FACTOR, default 1)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tochinicky/go-payments-ledger/internal/events"
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
	defer cl.Close()

	if len(args) > 0 && args[0] == "create-topic" {
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
	return relay.Run(ctx, relay.Config{DatabaseURL: dsn, Kafka: cl, Log: logger})
}
