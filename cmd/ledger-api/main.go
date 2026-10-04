// Command ledger-api serves the ledger's HTTP API.
//
//	ledger-api           serve on LISTEN_ADDR (default :8080), connecting with DATABASE_URL as the app role
//	ledger-api migrate   apply the migrations, connecting with DATABASE_URL as the owner role
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
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()

	if len(args) > 0 && args[0] == "migrate" {
		return store.Migrate(ctx, pool)
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(store.New(pool, store.NewID), logger).Handler(),
		ReadHeaderTimeout: 5 * time.Second, // a client can't hold a connection open by sending headers slowly
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
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
