package stream_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/duongnghia222/langsmith-deployment-go/internal/stream"
)

func startRedis(t *testing.T) *goredis.Client {
	t.Helper()
	ctx := context.Background()
	// stream-node-max-entries 1 makes ~MAXLEN trimming per-entry (exact),
	// which is required for the MAXLEN trim test to produce XLEN close to maxLen.
	container, err := tcredis.Run(ctx, "redis:7-alpine",
		testcontainers.WithCmdArgs("--stream-node-max-entries", "1"),
	)
	if err != nil {
		t.Fatalf("redis container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis connection string: %v", err)
	}
	opts, err := goredis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("redis parse url: %v", err)
	}
	rdb := goredis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestXAdd_OrderedDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis testcontainer test in short mode")
	}
	ctx := context.Background()
	rdb := startRedis(t)
	s := stream.NewStreamer(rdb)
	key := "test:ordered"

	var ids []string
	for i := 0; i < 5; i++ {
		id, err := s.XAdd(ctx, key, map[string]any{"n": fmt.Sprintf("%d", i)}, 1000)
		if err != nil {
			t.Fatalf("XAdd %d: %v", i, err)
		}
		ids = append(ids, id)
	}

	entries, err := s.XReadFrom(ctx, key, "0-0", 10, 0)
	if err != nil {
		t.Fatalf("XReadFrom: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}
	for i, e := range entries {
		if e.ID != ids[i] {
			t.Errorf("entry[%d].ID = %q, want %q", i, e.ID, ids[i])
		}
		if e.Fields["n"] != fmt.Sprintf("%d", i) {
			t.Errorf("entry[%d].Fields[n] = %q, want %q", i, e.Fields["n"], fmt.Sprintf("%d", i))
		}
	}
}

func TestXReadFrom_ReplayFromID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis testcontainer test in short mode")
	}
	ctx := context.Background()
	rdb := startRedis(t)
	s := stream.NewStreamer(rdb)
	key := "test:replay"

	var ids []string
	for i := 0; i < 4; i++ {
		id, err := s.XAdd(ctx, key, map[string]any{"seq": fmt.Sprintf("%d", i)}, 1000)
		if err != nil {
			t.Fatalf("XAdd: %v", err)
		}
		ids = append(ids, id)
	}

	// Read from second entry (exclusive: should return entries 2 and 3)
	entries, err := s.XReadFrom(ctx, key, ids[1], 10, 0)
	if err != nil {
		t.Fatalf("XReadFrom from id[1]: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after id[1], got %d", len(entries))
	}
	if entries[0].ID != ids[2] {
		t.Errorf("first entry after replay = %q, want %q", entries[0].ID, ids[2])
	}
}

func TestXAdd_MAXLENTrim(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis testcontainer test in short mode")
	}
	ctx := context.Background()
	rdb := startRedis(t)
	s := stream.NewStreamer(rdb)
	key := "test:maxlen"

	// Insert 200 entries with maxLen=5 so the radix tree macronode fills and
	// approximate trimming actually fires (Redis ~MAXLEN only trims at node
	// boundaries; 10 entries is not enough to trigger it).
	for i := 0; i < 200; i++ {
		if _, err := s.XAdd(ctx, key, map[string]any{"v": fmt.Sprintf("%d", i)}, 5); err != nil {
			t.Fatalf("XAdd: %v", err)
		}
	}

	// XLEN should be ~5 (approximate trim may allow slightly more)
	n, err := rdb.XLen(ctx, key).Result()
	if err != nil {
		t.Fatalf("XLEN: %v", err)
	}
	if n > 8 {
		t.Errorf("XLEN = %d after MAXLEN 5 trim, expected <= 8 (approximate)", n)
	}
}

func TestXReadFrom_BlockTimeoutReturnsEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis testcontainer test in short mode")
	}
	ctx := context.Background()
	rdb := startRedis(t)
	s := stream.NewStreamer(rdb)
	key := "test:block-empty"

	start := time.Now()
	// blockMillis=100 on an empty stream should return empty, no error
	entries, err := s.XReadFrom(ctx, key, "$", 10, 100)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("XReadFrom block: unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries on empty stream, got %d", len(entries))
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("expected ~100ms block, elapsed only %v", elapsed)
	}
}
