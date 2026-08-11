package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	lsdv1 "github.com/duongnghia222/langsmith-deployment-go/gen/lsd/v1"
	"github.com/duongnghia222/langsmith-deployment-go/internal/admin"
	"github.com/duongnghia222/langsmith-deployment-go/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestServer_HealthAndCapabilities(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := server.New(server.Deps{Admin: admin.New("test", "1")})
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	hc := grpc_health_v1.NewHealthClient(conn)
	hresp, err := hc.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if hresp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Errorf("status=%v", hresp.Status)
	}

	ac := lsdv1.NewAdminClient(conn)
	cresp, err := ac.Capabilities(ctx, &lsdv1.CapabilitiesRequest{})
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if cresp.Version != "test" {
		t.Errorf("Version=%q", cresp.Version)
	}
}
