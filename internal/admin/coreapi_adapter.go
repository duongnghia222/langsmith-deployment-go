package admin

import (
	"context"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"google.golang.org/protobuf/types/known/emptypb"
)

// CoreAPIAdapter exposes admin.Service's Truncate handler under the
// coreapi.AdminServer interface. A separate type is needed because Go does
// not allow a single struct to embed both lsdv1.UnimplementedAdminServer and
// coreapi.UnimplementedAdminServer (identical unqualified type name).
type CoreAPIAdapter struct {
	coreapi.UnimplementedAdminServer
	svc *Service
}

// NewCoreAPIAdapter returns a coreapi.AdminServer-satisfying wrapper that
// forwards Truncate calls to svc.
func NewCoreAPIAdapter(svc *Service) *CoreAPIAdapter {
	return &CoreAPIAdapter{svc: svc}
}

// Truncate forwards to the underlying admin.Service.
func (a *CoreAPIAdapter) Truncate(ctx context.Context, req *coreapi.TruncateRequest) (*emptypb.Empty, error) {
	return a.svc.Truncate(ctx, req)
}
