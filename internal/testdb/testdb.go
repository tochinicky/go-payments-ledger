// Package testdb starts a throwaway Postgres in Docker (Testcontainers) for integration tests, migrated, with two
// connection pools: the owner (what ledgerctl and migrations use) and the app's login role, so the tests run with
// the same grants as production.
package testdb

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/tochinicky/go-payments-ledger/internal/store"
)

// Image is the Postgres the tests run against.
const Image = "postgres:18-alpine"

// DB is a migrated test database.
type DB struct {
	Owner *pgxpool.Pool // the owner role: creates partners, can read everything
	App   *pgxpool.Pool // ledger_app, a login role in ledger_writer: what ledger-api connects as
}

// Run starts Postgres, runs the package's tests and stops it: call it from TestMain. One container per test
// package keeps the suite fast; tests isolate themselves by creating their own partner. Each setup function runs
// once the database is ready, before the tests.
func Run(m *testing.M, db **DB, setup ...func()) int {
	ctx := context.Background()
	container, err := postgres.Run(ctx, Image,
		postgres.WithDatabase("ledger"), postgres.WithUsername("owner"), postgres.WithPassword("owner"),
		postgres.BasicWaitStrategies())
	if err != nil {
		fmt.Fprintln(os.Stderr, "testdb: start postgres:", err)
		return 1
	}
	defer func() { _ = container.Terminate(ctx) }()
	d, err := open(ctx, container)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testdb:", err)
		return 1
	}
	defer d.Owner.Close()
	defer d.App.Close()
	*db = d
	for _, f := range setup {
		f()
	}
	return m.Run()
}

func open(ctx context.Context, container *postgres.PostgresContainer) (*DB, error) {
	ownerDSN, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, err
	}
	owner, err := pgxpool.New(ctx, ownerDSN)
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(ctx, owner); err != nil {
		return nil, err
	}
	if _, err := owner.Exec(ctx, "CREATE ROLE ledger_app LOGIN PASSWORD 'app' IN ROLE ledger_writer"); err != nil {
		return nil, fmt.Errorf("create app role: %w", err)
	}
	appCfg, err := pgxpool.ParseConfig(ownerDSN)
	if err != nil {
		return nil, err
	}
	appCfg.ConnConfig.User, appCfg.ConnConfig.Password = "ledger_app", "app"
	appCfg.MaxConns = 20
	app, err := pgxpool.NewWithConfig(ctx, appCfg)
	if err != nil {
		return nil, err
	}
	return &DB{Owner: owner, App: app}, nil
}
