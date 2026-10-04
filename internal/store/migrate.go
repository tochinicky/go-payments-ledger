package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/tochinicky/go-payments-ledger/migrations"
)

// Migrate applies every pending migration. pool must connect as the owner role: migrations create tables and
// grant the app role its rights.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	sqlDB := stdlib.OpenDBFromPool(pool) // goose speaks database/sql
	defer func() { _ = sqlDB.Close() }()
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, migrations.FS)
	if err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// SchemaVersion returns the newest applied migration, as the app role sees it (0 if none).
func (s *Store) SchemaVersion(ctx context.Context) (int64, error) {
	var v int64
	if err := s.pool.QueryRow(ctx, "SELECT coalesce(max(version_id), 0) FROM goose_db_version WHERE is_applied").Scan(&v); err != nil {
		return 0, fmt.Errorf("schema version: %w", err)
	}
	return v, nil
}

// Ready reports whether the database answers and its schema has reached want, so a replica never takes traffic
// for a schema its code doesn't expect. A newer schema is fine (migrations are additive during a rollout).
func (s *Store) Ready(ctx context.Context, want int64) error {
	v, err := s.SchemaVersion(ctx)
	if err != nil {
		return err
	}
	if v < want {
		return fmt.Errorf("schema at migration %d, this build needs %d", v, want)
	}
	return nil
}
