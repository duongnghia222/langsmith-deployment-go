package admin

import (
	"context"
	"fmt"
	"strings"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	lsdv1 "github.com/duongnghia222/langsmith-deployment-go/gen/lsd/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements lsdv1.AdminServer (Capabilities) and additionally
// exposes a Truncate method compatible with coreapi.AdminServer.
// It embeds lsdv1.UnimplementedAdminServer to satisfy forward-compatibility
// requirements for lsdv1.RegisterAdminServer.
// Note: coreapi.UnimplementedAdminServer is NOT embedded because both packages
// export a type named UnimplementedAdminServer; embedding both causes a
// compiler "redeclared" error. coreapi.RegisterAdminServer is not called in
// this codebase, so only Truncate as a plain method is needed.
type Service struct {
	lsdv1.UnimplementedAdminServer
	version       string
	schemaVersion string
	pool          *pgxpool.Pool
	env           string
}

// New creates a Service without a database pool (Capabilities-only).
func New(version, schemaVersion string) *Service {
	return &Service{version: version, schemaVersion: schemaVersion, env: "prod"}
}

// NewWithPool creates a Service with a database pool and env string.
// env should be sourced from cfg.Env (LSD_ENV, default "prod").
func NewWithPool(version, schemaVersion string, pool *pgxpool.Pool, env string) *Service {
	return &Service{
		version:       version,
		schemaVersion: schemaVersion,
		pool:          pool,
		env:           env,
	}
}

// Capabilities implements lsdv1.AdminServer.
func (s *Service) Capabilities(_ context.Context, _ *lsdv1.CapabilitiesRequest) (*lsdv1.CapabilitiesResponse, error) {
	return &lsdv1.CapabilitiesResponse{
		Version:       s.version,
		SchemaVersion: s.schemaVersion,
		Services:      []string{"threads", "runs", "assistants", "crons", "checkpointer", "cache", "admin"},
		Features: []string{
			"threads.crud",
			"threads.search",
			"threads.copy",
			"threads.set_status",
			"threads.sweep_ttl",
			"runs.crud",
			"runs.lease",
			"runs.heartbeat",
			"runs.cancel",
			"runs.join",
			"runs.sweep",
			"checkpointer.put",
			"checkpointer.get_tuple",
			"checkpointer.list",
			"checkpointer.copy_thread",
			"checkpointer.delete_thread",
			"checkpointer.prune",
			"cache.set",
			"cache.get",
			"admin.truncate",
		},
	}, nil
}

// Truncate is a dev-only handler that truncates one or more tables.
// Signature matches coreapi.AdminServer.Truncate so that *Service can be
// passed to coreapi.RegisterAdminServer if wired in the future.
func (s *Service) Truncate(ctx context.Context, req *coreapi.TruncateRequest) (*emptypb.Empty, error) {
	if s.env != "dev" {
		return nil, status.Error(codes.PermissionDenied,
			"Truncate is dev-only; set LSD_ENV=dev")
	}
	if s.pool == nil {
		return nil, status.Error(codes.Internal, "admin service has no database pool")
	}

	var tables []string
	if req.GetRuns() {
		tables = append(tables, "run")
	}
	if req.GetThreads() {
		tables = append(tables, "thread")
	}
	if req.GetAssistants() {
		tables = append(tables, "assistant", "assistant_versions")
	}
	if req.GetCheckpointer() {
		tables = append(tables, "checkpoints", "checkpoint_blobs", "checkpoint_writes")
	}
	if req.GetStore() {
		tables = append(tables,
			"langchain_pg_collection",
			"langchain_pg_embedding",
			"langchain_key_value_stores",
		)
	}
	if len(tables) == 0 {
		return &emptypb.Empty{}, nil
	}

	sql := fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", strings.Join(tables, ", "))
	if _, err := s.pool.Exec(ctx, sql); err != nil {
		return nil, status.Errorf(codes.Internal, "truncate: %v", err)
	}
	return &emptypb.Empty{}, nil
}
