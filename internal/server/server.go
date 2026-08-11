package server

import (
	"net"

	checkpointerpb "github.com/duongnghia222/langsmith-deployment-go/gen/checkpointer"
	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	lsdv1 "github.com/duongnghia222/langsmith-deployment-go/gen/lsd/v1"
	"github.com/duongnghia222/langsmith-deployment-go/internal/admin"
	"github.com/duongnghia222/langsmith-deployment-go/internal/assistants"
	cachepkg "github.com/duongnghia222/langsmith-deployment-go/internal/cache"
	checkpointerpkg "github.com/duongnghia222/langsmith-deployment-go/internal/checkpointer"
	"github.com/duongnghia222/langsmith-deployment-go/internal/crons"
	"github.com/duongnghia222/langsmith-deployment-go/internal/runs"
	"github.com/duongnghia222/langsmith-deployment-go/internal/threads"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Deps wires concrete service implementations into the gRPC server.
type Deps struct {
	Admin        *admin.Service
	Assistants   *assistants.Service
	Threads      *threads.Service
	Runs         *runs.Service
	Crons        *crons.Service
	Checkpointer *checkpointerpkg.Service
	Cache        *cachepkg.Service
}

type Server struct {
	g *grpc.Server
	h *health.Server
}

func New(deps Deps) *Server {
	// otelgrpc StatsHandler emits one span per RPC. When tracing.Init has not
	// been called (OTEL_EXPORTER_OTLP_ENDPOINT unset), the global TracerProvider
	// is a no-op so spans are cheap and discarded.
	g := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	if deps.Admin != nil {
		lsdv1.RegisterAdminServer(g, deps.Admin)
		coreapi.RegisterAdminServer(g, admin.NewCoreAPIAdapter(deps.Admin))
	}
	if deps.Assistants != nil {
		coreapi.RegisterAssistantsServer(g, deps.Assistants)
	}
	if deps.Threads != nil {
		coreapi.RegisterThreadsServer(g, deps.Threads)
	}
	if deps.Runs != nil {
		coreapi.RegisterRunsServer(g, deps.Runs)
	}
	if deps.Crons != nil {
		coreapi.RegisterCronsServer(g, deps.Crons)
	}
	if deps.Checkpointer != nil {
		checkpointerpb.RegisterCheckpointerServer(g, deps.Checkpointer)
	}
	if deps.Cache != nil {
		coreapi.RegisterCacheServer(g, deps.Cache)
	}
	h := health.NewServer()
	h.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(g, h)
	reflection.Register(g)
	return &Server{g: g, h: h}
}

func (s *Server) Serve(lis net.Listener) error {
	return s.g.Serve(lis)
}

func (s *Server) GracefulStop() {
	s.h.Shutdown()
	s.g.GracefulStop()
}
