package cache_test

import (
	"context"
	"net"
	"testing"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func newCacheClient(t *testing.T) coreapi.CacheClient {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	rdb := startRedis(t) // helper from cache_test.go (R5.11)
	svc := cache.NewService(cache.NewCache(rdb))
	srv := grpc.NewServer()
	coreapi.RegisterCacheServer(srv, svc)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)
	cc, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return coreapi.NewCacheClient(cc)
}

func ctxWithUser(ctx context.Context, userID string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("x-user-id", userID))
}

func TestCacheService_SetGet_RoundTrip(t *testing.T) {
	client := newCacheClient(t)
	ctx := ctxWithUser(context.Background(), "user-abc")

	_, err := client.Set(ctx, &coreapi.CacheSetRequest{Key: "mykey", Value: []byte("hello")})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	resp, err := client.Get(ctx, &coreapi.CacheGetRequest{Key: "mykey"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(resp.GetValue()) != "hello" {
		t.Errorf("Get: got %q, want %q", resp.GetValue(), "hello")
	}
}

func TestCacheService_MissingKey_ReturnsNilValue(t *testing.T) {
	client := newCacheClient(t)
	ctx := ctxWithUser(context.Background(), "user-abc")

	resp, err := client.Get(ctx, &coreapi.CacheGetRequest{Key: "nosuchkey"})
	if err != nil {
		t.Fatalf("Get missing key returned error: %v", err)
	}
	if resp.GetValue() != nil {
		t.Errorf("Get missing key: expected nil value, got %q", resp.GetValue())
	}
}

func TestCacheService_InvalidKey_ReturnsInvalidArgument(t *testing.T) {
	client := newCacheClient(t)
	ctx := ctxWithUser(context.Background(), "user-abc")

	_, err := client.Set(ctx, &coreapi.CacheSetRequest{Key: "bad key!", Value: []byte("x")})
	if err == nil {
		t.Fatal("expected error for invalid key, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestCacheService_NamespaceIsolation(t *testing.T) {
	client := newCacheClient(t)
	ctxA := ctxWithUser(context.Background(), "userA")
	ctxB := ctxWithUser(context.Background(), "userB")

	_, err := client.Set(ctxA, &coreapi.CacheSetRequest{Key: "shared", Value: []byte("for-A")})
	if err != nil {
		t.Fatalf("Set userA: %v", err)
	}

	resp, err := client.Get(ctxB, &coreapi.CacheGetRequest{Key: "shared"})
	if err != nil {
		t.Fatalf("Get userB: %v", err)
	}
	if resp.GetValue() != nil {
		t.Errorf("namespace isolation broken: userB can read userA's key")
	}
}

func TestCacheService_TTL_Expiry(t *testing.T) {
	client := newCacheClient(t)
	ctx := ctxWithUser(context.Background(), "ttl-user")

	ttl := durationpb.New(100 * time.Millisecond)
	_, err := client.Set(ctx, &coreapi.CacheSetRequest{
		Key:   "expiring",
		Value: []byte("gone soon"),
		Ttl:   ttl,
	})
	if err != nil {
		t.Fatalf("Set with TTL: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	resp, err := client.Get(ctx, &coreapi.CacheGetRequest{Key: "expiring"})
	if err != nil {
		t.Fatalf("Get after TTL expiry returned error: %v", err)
	}
	if resp.GetValue() != nil {
		t.Errorf("expected nil value after TTL expiry, got %q", resp.GetValue())
	}
}
