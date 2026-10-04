// Command ledger-api serves the ledger's HTTP API.
//
//	ledger-api           serve on LISTEN_ADDR (default :8080), connecting with DATABASE_URL as the app role
//	ledger-api migrate   apply the migrations, connecting with DATABASE_URL as the owner role
//
// IDEMPOTENCY_RETENTION (a Go duration, default 24h) is how long completed idempotency keys are kept.
// REQUEST_TIMEOUT, STATEMENT_TIMEOUT and LOCK_TIMEOUT (defaults 10s, 8s, 5s) bound each request; ledger-api refuses
// to start unless idempotency lease (30s) > request > statement ≥ lock. IDLE_IN_TRANSACTION_TIMEOUT (default 15s,
// longer than a request) ends a session left idle inside a transaction.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tochinicky/go-payments-ledger/internal/api"
	"github.com/tochinicky/go-payments-ledger/internal/store"
	"github.com/tochinicky/go-payments-ledger/internal/version"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger, os.Args[1:]); err != nil {
		logger.Error("ledger-api stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger, args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("DATABASE_URL: %w", err)
	}
	if len(args) > 0 && args[0] == "migrate" { // migrations run without the request timeouts
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return fmt.Errorf("database: %w", err)
		}
		defer pool.Close()
		return store.Migrate(ctx, pool)
	}

	timeouts := store.DefaultTimeouts
	for name, d := range map[string]*time.Duration{
		"REQUEST_TIMEOUT": &timeouts.Request, "STATEMENT_TIMEOUT": &timeouts.Statement, "LOCK_TIMEOUT": &timeouts.Lock,
		"IDLE_IN_TRANSACTION_TIMEOUT": &timeouts.IdleInTransaction,
	} {
		if v := os.Getenv(name); v != "" {
			if *d, err = time.ParseDuration(v); err != nil {
				return fmt.Errorf("%s %q: %w", name, v, err)
			}
		}
	}
	if err := timeouts.Validate(); err != nil {
		return err
	}
	timeouts.Apply(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	retention := 24 * time.Hour
	if v := os.Getenv("IDEMPOTENCY_RETENTION"); v != "" {
		if retention, err = time.ParseDuration(v); err != nil || retention <= 0 {
			return fmt.Errorf("IDEMPOTENCY_RETENTION %q: a positive duration such as 24h", v)
		}
	}
	st := store.New(pool, store.NewID)
	go cleanupKeys(ctx, logger, st, retention)

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(st, logger, timeouts.Request).Handler(),
		ReadHeaderTimeout: 5 * time.Second, // a client can't hold a connection open by sending headers slowly
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      timeouts.Request + 5*time.Second, // the handler's own deadline ends it first
		IdleTimeout:       2 * time.Minute,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("ledger-api listening", slog.String("addr", addr), slog.String("version", version.String()))

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	// Stop accepting connections and let in-flight requests finish. (The full graceful shutdown, with the relay
	// and Kafka, is part of the operations slice.)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// cleanupKeys deletes expired idempotency keys once a minute until ctx ends. Every replica runs it; the batches
// skip rows another replica is deleting, so they share the work instead of waiting on each other.
func cleanupKeys(ctx context.Context, logger *slog.Logger, st *store.Store, retention time.Duration) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		n, err := st.CleanupKeys(ctx, retention, 1000)
		if err != nil {
			logger.WarnContext(ctx, "idempotency key cleanup failed", slog.Any("error", err))
			continue
		}
		if n > 0 {
			logger.InfoContext(ctx, "idempotency keys cleaned up", slog.Int64("deleted", n))
		}
	}
}
