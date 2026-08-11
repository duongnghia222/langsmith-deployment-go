package assistants

import (
	"context"
	"errors"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	engcommon "github.com/duongnghia222/langsmith-deployment-go/gen/engine_common"
	"github.com/duongnghia222/langsmith-deployment-go/internal/jsonbutil"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is a gRPC adapter that implements coreapi.AssistantsServer.
// Unimplemented methods (mutations) are covered by the embedded stub.
type Service struct {
	coreapi.UnimplementedAssistantsServer
	store *Store
}

// NewService creates a Service backed by a pgxpool.Pool.
func NewService(pool *pgxpool.Pool) *Service { return &Service{store: NewStore(pool)} }

// Get implements AssistantsServer.Get.
//
// Deviation: GetAssistantRequest.AssistantId is a plain string (not *UUID).
// The plan assumed UUID wrapper; actual proto uses string directly.
func (s *Service) Get(ctx context.Context, req *coreapi.GetAssistantRequest) (*coreapi.Assistant, error) {
	a, err := s.store.Get(ctx, req.GetAssistantId(), req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return toPB(a), nil
}

// assistantsSortByName maps the proto AssistantsSortBy enum to a SQL column name
// that the store's sortClause whitelist understands.
// (ops.py:186-193) Python valid_sort_fields list — keep-verbatim mapping.
var assistantsSortByName = map[coreapi.AssistantsSortBy]string{
	coreapi.AssistantsSortBy_ASSISTANT_ID: "assistant_id",
	coreapi.AssistantsSortBy_GRAPH_ID:     "graph_id",
	coreapi.AssistantsSortBy_NAME:         "name",
	coreapi.AssistantsSortBy_CREATED_AT:   "created_at",
	coreapi.AssistantsSortBy_UPDATED_AT:   "updated_at",
}

// Search implements AssistantsServer.Search.
func (s *Service) Search(ctx context.Context, req *coreapi.SearchAssistantsRequest) (*coreapi.SearchAssistantsResponse, error) {
	in := SearchInput{
		Limit:  req.GetLimit(),
		Offset: req.GetOffset(),
	}
	if req.GraphId != nil {
		in.GraphID = req.GetGraphId()
	}
	if req.Name != nil {
		in.Name = req.GetName()
	}
	if len(req.GetMetadataJson()) > 0 {
		in.MetadataFilter = req.GetMetadataJson()
	}
	// (ops.py:184-200) sort_by whitelist + sort_order.
	if req.SortBy != nil {
		in.SortBy = assistantsSortByName[req.GetSortBy()]
	}
	if req.SortOrder != nil {
		if req.GetSortOrder() == coreapi.SortOrder_ASC {
			in.SortOrder = "asc"
		} else {
			in.SortOrder = "desc"
		}
	}
	rows, err := s.store.Search(ctx, in, req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	out := &coreapi.SearchAssistantsResponse{}
	for _, r := range rows {
		out.Assistants = append(out.Assistants, toPB(r))
	}
	return out, nil
}

// Count implements AssistantsServer.Count.
func (s *Service) Count(ctx context.Context, req *coreapi.CountAssistantsRequest) (*coreapi.CountResponse, error) {
	in := SearchInput{}
	if req.GraphId != nil {
		in.GraphID = req.GetGraphId()
	}
	if req.Name != nil {
		in.Name = req.GetName()
	}
	if len(req.GetMetadataJson()) > 0 {
		in.MetadataFilter = req.GetMetadataJson()
	}
	n, err := s.store.Count(ctx, in, req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return &coreapi.CountResponse{Count: n}, nil
}

// GetVersions implements AssistantsServer.GetVersions.
//
// Deviation: GetAssistantVersionsRequest.AssistantId is a plain string (not *UUID).
func (s *Service) GetVersions(ctx context.Context, req *coreapi.GetAssistantVersionsRequest) (*coreapi.GetAssistantVersionsResponse, error) {
	// (ops.py:599) metadata @> filter passed through — was silently dropped before.
	versions, err := s.store.GetVersions(ctx, req.GetAssistantId(), req.GetLimit(), req.GetOffset(), req.GetMetadataJson(), req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	out := &coreapi.GetAssistantVersionsResponse{}
	for _, v := range versions {
		out.Versions = append(out.Versions, toVersionPB(v))
	}
	return out, nil
}

// toPB converts the internal Assistant to the proto Assistant message.
// Config is deserialized from JSONB via protojson; rows in pre-protojson
// (Python-dict) format will degrade to a sparse EngineRunnableConfig.
func toPB(a *Assistant) *coreapi.Assistant {
	pb := &coreapi.Assistant{
		AssistantId:  a.AssistantID,
		GraphId:      a.GraphID,
		Version:      uint64(a.Version),
		CreatedAt:    timestamppb.New(a.CreatedAt),
		UpdatedAt:    timestamppb.New(a.UpdatedAt),
		Name:         a.Name,
		MetadataJson: a.Metadata,
		ContextJson:  a.ContextJSON,
	}
	if cfg, ok := decodeConfig(a.Config); ok {
		pb.Config = cfg
	}
	if a.Description != nil {
		pb.Description = a.Description
	}
	return pb
}

// toVersionPB converts the internal AssistantVersion to the proto AssistantVersion message.
func toVersionPB(v *AssistantVersion) *coreapi.AssistantVersion {
	pb := &coreapi.AssistantVersion{
		AssistantId:  v.AssistantID,
		GraphId:      v.GraphID,
		Name:         v.Name,
		Version:      uint64(v.Version),
		CreatedAt:    timestamppb.New(v.CreatedAt),
		MetadataJson: v.Metadata,
		ContextJson:  v.ContextJSON,
	}
	if cfg, ok := decodeConfig(v.Config); ok {
		pb.Config = cfg
	}
	if v.Description != nil {
		pb.Description = v.Description
	}
	return pb
}

// decodeConfig deserializes the JSONB config column into an EngineRunnableConfig.
// Returns (nil, false) when the column is empty / "{}" / unparseable so callers
// leave the proto's Config field unset rather than emitting a zero-value message.
func decodeConfig(raw []byte) (*engcommon.EngineRunnableConfig, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	cfg := &engcommon.EngineRunnableConfig{}
	if err := jsonbutil.Unmarshal(raw, cfg); err != nil {
		return nil, false
	}
	if proto.Equal(cfg, &engcommon.EngineRunnableConfig{}) {
		return nil, false
	}
	return cfg, true
}

// Create implements AssistantsServer.Create.
//
// (C3) atomic — if_exists is forwarded to the store's single-CTE implementation:
//   - DO_NOTHING: returns existing assistant if assistant_id conflicts (no TOCTOU).
//   - RAISE (default): returns codes.AlreadyExists on duplicate key.
func (s *Service) Create(ctx context.Context, req *coreapi.CreateAssistantRequest) (*coreapi.Assistant, error) {
	in := CreateInput{
		AssistantID: req.GetAssistantId(),
		GraphID:     req.GetGraphId(),
		Name:        req.GetName(),
		Metadata:    req.GetMetadataJson(),
		ContextJSON: req.GetContextJson(),
		Filters:     req.GetFilters(),
	}
	if req.GetConfig() != nil {
		b, err := jsonbutil.Marshal(req.GetConfig())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		in.Config = b
	}
	if req.GetDescription() != "" {
		desc := req.GetDescription()
		in.Description = &desc
	}

	// Forward if_exists to the atomic store (ops.py:356-374).
	if req.GetIfExists() == coreapi.OnConflictBehavior_DO_NOTHING {
		in.IfExists = "do_nothing"
	} else {
		in.IfExists = "raise"
	}

	a, err := s.store.Create(ctx, in)
	if err != nil {
		return nil, mapErr(err)
	}
	return toPB(a), nil
}

// Patch implements AssistantsServer.Patch.
func (s *Service) Patch(ctx context.Context, req *coreapi.PatchAssistantRequest) (*coreapi.Assistant, error) {
	in := PatchInput{
		Name:        req.GetName(),
		Metadata:    req.GetMetadataJson(),
		ContextJSON: req.GetContextJson(),
		GraphID:     req.GetGraphId(),
	}
	if req.GetConfig() != nil {
		b, err := jsonbutil.Marshal(req.GetConfig())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		in.Config = b
	}
	if req.GetDescription() != "" {
		desc := req.GetDescription()
		in.Description = &desc
	}
	a, err := s.store.Patch(ctx, req.GetAssistantId(), in, req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return toPB(a), nil
}

// Delete implements AssistantsServer.Delete.
func (s *Service) Delete(ctx context.Context, req *coreapi.DeleteAssistantRequest) (*coreapi.DeleteAssistantsResponse, error) {
	ids, err := s.store.Delete(ctx, req.GetAssistantId(), req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return &coreapi.DeleteAssistantsResponse{AssistantIds: ids}, nil
}

// SetLatest implements AssistantsServer.SetLatest.
func (s *Service) SetLatest(ctx context.Context, req *coreapi.SetLatestAssistantRequest) (*coreapi.Assistant, error) {
	a, err := s.store.SetLatest(ctx, req.GetAssistantId(), req.GetVersion(), req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return toPB(a), nil
}

// mapErr converts store errors to gRPC status errors.
func mapErr(err error) error {
	if errors.Is(err, ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, ErrAlreadyExists) {
		return status.Error(codes.AlreadyExists, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
