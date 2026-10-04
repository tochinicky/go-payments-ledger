// Command reconcile recomputes every balance from the postings and holds and checks the ledger's invariants
// (1–4 and 6) over the whole database. It prints a report and exits non-zero on any break. Run it after the
// integration and load tests, and on a schedule in production.
//
//	DATABASE_URL         any role that can read the ledger (it only reads)
//	PUBLISHED_WITHIN     if set (a Go duration such as 60s), events older than this must be published too
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tochinicky/go-payments-ledger/internal/store"
)

func main() {
	ok, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "reconcile:", err)
		os.Exit(2)
	}
	if !ok {
		os.Exit(1)
	}
}

func run() (bool, error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return false, errors.New("DATABASE_URL is not set")
	}
	var within time.Duration
	if v := os.Getenv("PUBLISHED_WITHIN"); v != "" {
		var err error
		if within, err = time.ParseDuration(v); err != nil {
			return false, fmt.Errorf("PUBLISHED_WITHIN %q: %w", v, err)
		}
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return false, err
	}
	defer pool.Close()
	report, err := store.New(pool, store.NewID).Reconcile(ctx, within.Seconds())
	if err != nil {
		return false, err
	}
	for _, c := range report.Checks {
		mark := "PASS"
		if c.Breaks > 0 {
			mark = "FAIL"
		}
		fmt.Printf("%s  %s", mark, c.Name)
		if c.Breaks > 0 {
			fmt.Printf("  (%d)", c.Breaks)
		}
		fmt.Println()
		for _, e := range c.Examples {
			fmt.Println("        " + e)
		}
	}
	if report.OK() {
		fmt.Println("reconciliation clean")
	} else {
		fmt.Println("reconciliation FAILED")
	}
	return report.OK(), nil
}
