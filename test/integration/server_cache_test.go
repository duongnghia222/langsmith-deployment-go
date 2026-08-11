package integration_test

import (
	"context"
	"net"
	"testing"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/cache"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/server"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// TestServerStarts_ReflectsCache verifies the Cache service is registered
// on the LSD gRPC server. Calls Cache.Set + Cache.Get round-trip to prove it.
func TestServerStarts_ReflectsCache(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()

	// Postgres (only needed because server.New defaults; not used by Cache)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Redis
	rc, err := tcredis.Run(ctx, "redis:7")
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	t.Cleanup(func() { _ = rc.Terminate(ctx) })
	uri, err := rc.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis uri: %v", err)
	}
	opts, err := goredis.ParseURL(uri)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	rdb := goredis.NewClient(opts)

	cacheSvc := cache.NewService(cache.NewCache(rdb))
	srv := server.New(server.Deps{Cache: cacheSvc})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := coreapi.NewCacheClient(conn)
	mdctx := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-user-id", "alice"))
	if _, err := client.Set(mdctx, &coreapi.CacheSetRequest{Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("Cache.Set: %v", err)
	}
	resp, err := client.Get(mdctx, &coreapi.CacheGetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Cache.Get: %v", err)
	}
	if string(resp.GetValue()) != "v" {
		t.Errorf("Cache.Get got %q, want %q", resp.GetValue(), "v")
	}
}
