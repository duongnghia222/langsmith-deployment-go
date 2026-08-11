package cache_test

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/duongnghia222/langsmith-deployment-go/internal/cache"
)

func startRedis(t *testing.T) *goredis.Client {
	t.Helper()
	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7-alpine")
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

func TestCache_SetGet_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis testcontainer test in short mode")
	}
	ctx := context.Background()
	rdb := startRedis(t)
	c := cache.NewCache(rdb)

	key := "test:roundtrip"
	want := []byte("hello-world")

	if err := c.Set(ctx, key, want, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("Get = %q, want %q", got, want)
	}
}

func TestCache_TTL_Expiry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis testcontainer test in short mode")
	}
	ctx := context.Background()
	rdb := startRedis(t)
	c := cache.NewCache(rdb)

	key := "test:ttl"
	if err := c.Set(ctx, key, []byte("expires-soon"), 100*time.Millisecond); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Key should be present immediately.
	if _, err := c.Get(ctx, key); err != nil {
		t.Fatalf("Get (before expiry): %v", err)
	}

	// Wait for the key to expire.
	time.Sleep(300 * time.Millisecond)

	_, err := c.Get(ctx, key)
	if err == nil {
		t.Fatal("expected ErrNotFound after TTL expiry, got nil")
	}
	if err != cache.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCache_GetMissing_ReturnsErrNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis testcontainer test in short mode")
	}
	ctx := context.Background()
	rdb := startRedis(t)
	c := cache.NewCache(rdb)

	_, err := c.Get(ctx, "test:nonexistent-key-xyz")
	if err == nil {
		t.Fatal("expected ErrNotFound, got nil")
	}
	if err != cache.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
