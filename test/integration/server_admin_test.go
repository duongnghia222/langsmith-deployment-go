package integration_test

import (
	"context"
	"net"
	"testing"

	"github.com/duongnghia222/langsmith-deployment-go/internal/admin"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/server"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

// TestServerStarts_ReflectsAdminTruncate verifies that coreApi.Admin is
// present in the gRPC reflection listing after R5.16 wires it.
func TestServerStarts_ReflectsAdminTruncate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e integration test in short mode")
	}
	ctx := context.Background()
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{
		Admin: admin.NewWithPool("test", "11", pool, "prod"),
	})
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	refClient := grpc_reflection_v1alpha.NewServerReflectionClient(conn)
	stream, err := refClient.ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatalf("reflection stream: %v", err)
	}
	if err := stream.Send(&grpc_reflection_v1alpha.ServerReflectionRequest{
		MessageRequest: &grpc_reflection_v1alpha.ServerReflectionRequest_ListServices{
			ListServices: "",
		},
	}); err != nil {
		t.Fatalf("send list_services: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	wantCore := "coreApi.Admin"
	wantLsd := "lsd.v1.Admin"
	var foundCore, foundLsd bool
	for _, svc := range resp.GetListServicesResponse().GetService() {
		switch svc.GetName() {
		case wantCore:
			foundCore = true
		case wantLsd:
			foundLsd = true
		}
	}
	if !foundCore {
		t.Errorf("%s not found in reflection listing", wantCore)
	}
	if !foundLsd {
		t.Errorf("%s not found in reflection listing", wantLsd)
	}
}
