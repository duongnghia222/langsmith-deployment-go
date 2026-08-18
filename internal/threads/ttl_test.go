package threads_test

import (
	"context"
	"testing"
	"time"

	"github.com/duongnghia222/langsmith-deployment-go/internal/logger"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/duongnghia222/langsmith-deployment-go/internal/threads"
)

// TestTTLSweeper_DeletesExpiredThreads_KeepsUnexpired is 3h: the Go thread
// TTL sweeper must delete threads whose expires_at has passed and leave
// threads with a future (or absent) expires_at untouched.
func TestTTLSweeper_DeletesExpiredThreads_KeepsUnexpired(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, pool := newTestStore(t, ctx)

	expiredID := "a1200001-0000-0000-0000-000000000001"
	futureID := "a1200001-0000-0000-0000-000000000002"
	testdb.MustInsertThread(t, ctx, pool, expiredID, nil)
	testdb.MustInsertThread(t, ctx, pool, futureID, nil)
	if _, err := pool.Exec(ctx,
		`UPDATE thread SET expires_at = now() - interval '1 hour' WHERE thread_id=$1::uuid`, expiredID,
	); err != nil {
		t.Fatalf("seed expired: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE thread SET expires_at = now() + interval '1 hour' WHERE thread_id=$1::uuid`, futureID,
	); err != nil {
		t.Fatalf("seed future: %v", err)
	}

	log := logger.New("error")
	go threads.TTLSweeper(ctx, pool, log, threads.TTLSweeperConfig{Interval: 100 * time.Millisecond})

	deadline := time.Now().Add(5 * time.Second)
	for {
		var exists bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM thread WHERE thread_id=$1::uuid)`, expiredID,
		).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expired thread was not swept within timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}

	var futureExists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM thread WHERE thread_id=$1::uuid)`, futureID,
	).Scan(&futureExists); err != nil {
		t.Fatal(err)
	}
	if !futureExists {
		t.Error("thread with future expires_at should not be swept")
	}
}
