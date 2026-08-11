package integration_test

import (
	"context"
	"net"
	"testing"

	checkpointerpb "github.com/duongnghia222/langsmith-deployment-go/gen/checkpointer"
	"github.com/duongnghia222/langsmith-deployment-go/internal/checkpointer"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/server"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestServerStarts_ReflectsCheckpointer dials LSD and calls GetCapabilities via
// the Checkpointer gRPC client. If the service is registered, the call
// succeeds; if not, it returns codes.Unimplemented.
func TestServerStarts_ReflectsCheckpointer(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	cpStore := checkpointer.NewStore(pool)
	cpSvc := checkpointer.NewService(cpStore)
	srv := server.New(server.Deps{Checkpointer: cpSvc})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cpClient := checkpointerpb.NewCheckpointerClient(conn)
	caps, err := cpClient.GetCapabilities(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetCapabilities: %v", err)
	}
	if !caps.SupportsCopyThread {
		t.Errorf("Checkpointer service not properly registered: SupportsCopyThread=false")
	}
}
