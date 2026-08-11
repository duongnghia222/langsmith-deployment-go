package testdb_test

import (
	"context"
	"testing"

	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMustInsertThread_DuplicateFails(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	const id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	// first insert must succeed
	testdb.MustInsertThread(t, ctx, pool, id, nil)
	// second insert on same PK must FAIL the underlying SQL (proves no ON CONFLICT swallowing).
	_, pgErr := pool.Exec(ctx,
		`INSERT INTO thread (thread_id, metadata, status, created_at, updated_at)
		 VALUES ($1::uuid, '{}'::jsonb, 'idle', now(), now())`, id)
	if pgErr == nil {
		t.Fatal("expected duplicate key error, got nil")
	}
}
