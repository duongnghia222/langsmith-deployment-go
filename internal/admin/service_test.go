package admin_test

import (
	"context"
	"testing"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	lsdv1 "github.com/duongnghia222/langsmith-deployment-go/gen/lsd/v1"
	"github.com/duongnghia222/langsmith-deployment-go/internal/admin"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCapabilities(t *testing.T) {
	svc := admin.New("0.1.0", "1")
	resp, err := svc.Capabilities(context.Background(), &lsdv1.CapabilitiesRequest{})
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if resp.Version != "0.1.0" {
		t.Errorf("Version=%q", resp.Version)
	}
	if resp.SchemaVersion != "1" {
		t.Errorf("SchemaVersion=%q", resp.SchemaVersion)
	}
	want := map[string]bool{"threads": true, "runs": true}
	got := map[string]bool{}
	for _, s := range resp.Services {
		got[s] = true
	}
	for k := range want {
		if !got[k] {
			t.Errorf("missing service %q", k)
		}
	}
}

func setupPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}
	return pool
}

func TestTruncate_PermissionDenied_WhenNotDev(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}
	pool := setupPool(t)
	svc := admin.NewWithPool("test", "11", pool, "prod")
	_, err := svc.Truncate(context.Background(), &coreapi.TruncateRequest{Runs: true})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("got code %v, want PermissionDenied", status.Code(err))
	}
}

func TestTruncate_Succeeds_WhenDev(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}
	pool := setupPool(t)
	ctx := context.Background()
	svc := admin.NewWithPool("test", "11", pool, "dev")

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "graph1", nil)
	threadID := "00000000-0000-0000-0000-000000000099"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "pending")

	_, err := svc.Truncate(ctx, &coreapi.TruncateRequest{
		Runs:    true,
		Threads: true,
	})
	if err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	var runCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM run`).Scan(&runCount); err != nil {
		t.Fatalf("count runs: %v", err)
	}
	if runCount != 0 {
		t.Errorf("run table: got %d rows, want 0", runCount)
	}

	var threadCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM thread`).Scan(&threadCount); err != nil {
		t.Fatalf("count threads: %v", err)
	}
	if threadCount != 0 {
		t.Errorf("thread table: got %d rows, want 0", threadCount)
	}
}

func TestTruncate_CheckpointerFlag_TruncatesCheckpointTables(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}
	pool := setupPool(t)
	ctx := context.Background()
	svc := admin.NewWithPool("test", "11", pool, "dev")

	threadID := "00000000-0000-0000-0000-000000000098"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	if _, err := pool.Exec(ctx, `
		INSERT INTO checkpoints (thread_id, checkpoint_ns, checkpoint_id, checkpoint, metadata)
		VALUES ($1::uuid, '', '00000000-0000-0000-0000-000000000001'::uuid, '{}'::jsonb, '{}'::jsonb)
	`, threadID); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	_, err := svc.Truncate(ctx, &coreapi.TruncateRequest{Checkpointer: true})
	if err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM checkpoints`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("checkpoints: got %d rows after Truncate, want 0", count)
	}
}
