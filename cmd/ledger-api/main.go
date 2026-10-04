// Command ledger-api serves the ledger's HTTP API, and runs the hold-expiry and idempotency-cleanup loops.
//
//	ledger-api           serve on LISTEN_ADDR (default :8080), connecting with DATABASE_URL as the app role
//	ledger-api migrate   apply the migrations, connecting with DATABASE_URL as the owner role
//
// ADMIN_ADDR (default :9090) serves /metrics, /healthz and /readyz, apart from the API.
// MAX_AMOUNT_MINOR (default 10,000,000,000) caps a single amount_minor.
// IDEMPOTENCY_RETENTION (a Go duration, default 24h) is how long completed idempotency keys are kept.
// REQUEST_TIMEOUT, STATEMENT_TIMEOUT and LOCK_TIMEOUT (defaults 10s, 8s, 5s) bound each request; ledger-api refuses
// to start unless idempotency lease (30s) > request > statement ≥ lock. IDLE_IN_TRANSACTION_TIMEOUT (default 15s,
// longer than a request) ends a session left idle inside a transaction.
// SHUTDOWN_DRAIN (default 5s) is how long /readyz fails before the server stops accepting connections, so load
// balancers stop sending traffic first. OTEL_EXPORTER_OTLP_ENDPOINT, if set, receives traces.
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
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tochinicky/go-payments-ledger/internal/api"
	"github.com/tochinicky/go-payments-ledger/internal/obs"
	"github.com/tochinicky/go-payments-ledger/internal/store"
	"github.com/tochinicky/go-payments-ledger/internal/version"
	"github.com/tochinicky/go-payments-ledger/migrations"
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
	retention, err := durationEnv("IDEMPOTENCY_RETENTION", 24*time.Hour)
	if err != nil {
		return err
	}
	drain, err := durationEnv("SHUTDOWN_DRAIN", 5*time.Second)
	if err != nil {
		return err
	}
	maxAmount := int64(api.DefaultMaxAmountMinor)
	if v := os.Getenv("MAX_AMOUNT_MINOR"); v != "" {
		if maxAmount, err = strconv.ParseInt(v, 10, 64); err != nil || maxAmount <= 0 {
			return fmt.Errorf("MAX_AMOUNT_MINOR %q: a positive integer", v)
		}
	}

	telemetry, err := obs.Setup(ctx, "ledger-api")
	if err != nil {
		return err
	}
	timeouts.Apply(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	st := store.New(pool, store.NewID)

	health := &obs.Health{Ready: func(ctx context.Context) error { return st.Ready(ctx, migrations.Version) }}
	admin := &http.Server{Addr: envOr("ADMIN_ADDR", ":9090"), Handler: telemetry.AdminHandler(health), ReadHeaderTimeout: 5 * time.Second}
	srv := &http.Server{
		Addr:              envOr("LISTEN_ADDR", ":8080"),
		Handler:           api.New(api.Config{Store: st, Log: logger, RequestTimeout: timeouts.Request, MaxAmountMinor: maxAmount}).Handler(),
		ReadHeaderTimeout: 5 * time.Second, // a client can't hold a connection open by sending headers slowly
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      timeouts.Request + 5*time.Second, // the handler's own deadline ends it first
		IdleTimeout:       2 * time.Minute,
	}

	// The background loops get their own context: they stop after the HTTP server has drained, not before.
	loops, stopLoops := context.WithCancel(context.WithoutCancel(ctx))
	var wg sync.WaitGroup
	wg.Go(func() { cleanupKeys(loops, logger, st, retention) })
	wg.Go(func() { expireHolds(loops, logger, st) })

	errc := make(chan error, 2)
	go func() { errc <- admin.ListenAndServe() }()
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("ledger-api listening", slog.String("addr", srv.Addr), slog.String("admin", admin.Addr), slog.String("version", version.String()))

	var serveErr error
	select {
	case serveErr = <-errc:
	case <-ctx.Done():
	}
	// Graceful shutdown: fail readiness first so traffic moves away, then stop accepting and let in-flight requests
	// finish (each is bounded by its own deadline), then stop the loops, flush telemetry, close the pool.
	logger.Info("ledger-api shutting down")
	health.Drain()
	if serveErr == nil {
		time.Sleep(drain)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeouts.Request+5*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	stopLoops()
	wg.Wait()
	err = errors.Join(err, admin.Shutdown(shutdownCtx), telemetry.Shutdown(shutdownCtx))
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		err = errors.Join(serveErr, err)
	}
	logger.Info("ledger-api stopped")
	return err
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s %q: a duration such as 5s", name, v)
	}
	return d, nil
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

// expireHolds ends due holds every few seconds until ctx ends, so a hold is released at most a few seconds after
// its time (and no client can capture it meanwhile: a hold past its time is no longer active). Every replica runs
// it; each hold is expired in its own short transaction, skipping any that is busy.
func expireHolds(ctx context.Context, logger *slog.Logger, st *store.Store) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		n, err := st.ExpireHolds(ctx, 100)
		if err != nil {
			logger.WarnContext(ctx, "hold expiry failed", slog.Any("error", err))
			continue
		}
		if n > 0 {
			logger.InfoContext(ctx, "holds expired", slog.Int("expired", n))
		}
	}
}
