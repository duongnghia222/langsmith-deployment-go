package db_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
)

func TestMigrate_AppliesAllUpMigrations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()
	dsn := testdb.Start(t, ctx)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	// pre-seed the upstream schema (thread, run) since LSD migrations
	// expect those tables to exist.
	testdb.SeedBaseSchema(t, ctx, pool)

	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var version string
	if err := pool.QueryRow(ctx, "SELECT value FROM lsd_meta WHERE key='schema_version'").Scan(&version); err != nil {
		t.Fatalf("query lsd_meta: %v", err)
	}
	if version != "15" {
		t.Errorf("schema_version = %q, want 15", version)
	}

	// Idempotent: running again is a no-op.
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("Migrate (second run): %v", err)
	}
}

func TestMigrate_FreshDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()
	dsn := testdb.Start(t, ctx)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	// No SeedBaseSchema — that's the point. Fresh DB.

	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Verify a Python-baseline table exists (created by 0000003).
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name='assistant'`,
	).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 1 {
		t.Errorf("assistant table missing after fresh-DB migrate")
	}

	// Verify lease columns exist on run (added by 0000001 self-bootstrap + ALTER).
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.columns
		 WHERE table_name='run' AND column_name='lease_holder_id'`,
	).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 1 {
		t.Errorf("lease_holder_id column missing on run")
	}

	// Verify the 0000003 ALTER block ran (run.assistant_id is added by ALTER, not by 0000001 stub).
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.columns
		 WHERE table_name='run' AND column_name='assistant_id'`,
	).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 1 {
		t.Errorf("assistant_id column missing on run (0000003 ALTER block didn't run)")
	}

	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name='cron'`,
	).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 1 {
		t.Errorf("cron table missing after fresh-DB migrate")
	}

	// Idempotent: running again must be a no-op.
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate (second run): %v", err)
	}

	// Verify lsd_meta schema_version is set (proves 0000004 ran).
	var version string
	if err := pool.QueryRow(ctx,
		"SELECT value FROM lsd_meta WHERE key='schema_version'").Scan(&version); err != nil {
		t.Fatalf("query lsd_meta: %v", err)
	}
	if version != "15" {
		t.Errorf("schema_version = %q, want 15", version)
	}
}
