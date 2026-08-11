package redis_test

import (
	"context"
	"testing"
	"time"

	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	lsdredis "github.com/duongnghia222/langsmith-deployment-go/internal/redis"
)

func TestNew_PingsSuccessfully(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	client, err := lsdredis.New(ctx, lsdredis.Config{URL: connStr, PoolSize: 5})
	if err != nil {
		t.Fatalf("redis.New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestNew_RejectsInvalidURL(t *testing.T) {
	_, err := lsdredis.New(context.Background(), lsdredis.Config{URL: "not-a-url"})
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}
