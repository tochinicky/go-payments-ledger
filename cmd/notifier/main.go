// Command notifier consumes the ledger's events and records one notification per event.
//
//	DATABASE_URL              the notifier's own login role
//	KAFKA_BROKERS             comma-separated seed brokers
//	SESSION_TIMEOUT           how soon the group notices a crashed member (a Go duration, default 45s)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tochinicky/go-payments-ledger/internal/notifier"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("notifier stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn, brokers := os.Getenv("DATABASE_URL"), os.Getenv("KAFKA_BROKERS")
	if dsn == "" || brokers == "" {
		return errors.New("DATABASE_URL and KAFKA_BROKERS must be set")
	}
	var session time.Duration
	if v := os.Getenv("SESSION_TIMEOUT"); v != "" {
		var err error
		if session, err = time.ParseDuration(v); err != nil {
			return fmt.Errorf("SESSION_TIMEOUT %q: %w", v, err)
		}
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	return notifier.New(notifier.Config{
		Pool: pool, Brokers: strings.Split(brokers, ","), SessionTimeout: session, Log: logger,
	}).Run(ctx)
}
