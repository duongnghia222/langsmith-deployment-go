package crons

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	enumcronorc "github.com/duongnghia222/langsmith-deployment-go/gen/enum_cron_on_run_completed"
	"github.com/duongnghia222/langsmith-deployment-go/internal/jsonbutil"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is a gRPC adapter that implements coreapi.CronsServer.
// Unimplemented methods (mutations) are covered by the embedded stub.
type Service struct {
	coreapi.UnimplementedCronsServer
	store *Store
}

// NewService creates a Service backed by a pgxpool.Pool.
func NewService(pool *pgxpool.Pool) *Service {
	return &Service{store: NewStore(pool)}
}

// Search implements CronsServer.Search.
//
// Deviation: SearchCronsRequest.Limit and Offset are *uint64 (oneof), not plain uint64.
// We call GetLimit()/GetOffset() which dereference safely, returning 0 on nil.
// AssistantId and ThreadId are *UUID wrappers; we call GetValue() to extract the string.
// SortBy/SortOrder are forwarded to the store (whitelist-validated there).
// Enabled is a *bool oneof; when non-nil it is forwarded as an LSD-only filter.
func (s *Service) Search(ctx context.Context, req *coreapi.SearchCronsRequest) (*coreapi.SearchCronsResponse, error) {
	in := SearchInput{
		Limit:         req.GetLimit(),
		Offset:        req.GetOffset(),
		ThreadFilters: req.GetThreadFilters(),
		// Map proto CronsSortBy enum to the column-name string the store expects.
		SortBy:    cronsSortByToString(req.GetSortBy()),
		SortOrder: sortOrderToString(req.GetSortOrder()),
	}
	if req.AssistantId != nil {
		in.AssistantID = req.GetAssistantId().GetValue()
	}
	if req.ThreadId != nil {
		in.ThreadID = req.GetThreadId().GetValue()
	}
	// Enabled is a *bool oneof in the proto — only forward when explicitly set.
	if req.Enabled != nil {
		v := req.GetEnabled()
		in.Enabled = &v
	}
	rows, err := s.store.Search(ctx, in, req.GetFilters())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &coreapi.SearchCronsResponse{}
	for _, c := range rows {
		out.Crons = append(out.Crons, toPB(c))
	}
	return out, nil
}

// cronsSortByToString maps the proto CronsSortBy enum to the column-name string
// expected by cronsSortColumns in store.go.
func cronsSortByToString(sb coreapi.CronsSortBy) string {
	switch sb {
	case coreapi.CronsSortBy_CRONS_SORT_BY_CRON_ID:
		return "cron_id"
	case coreapi.CronsSortBy_CRONS_SORT_BY_ASSISTANT_ID:
		return "assistant_id"
	case coreapi.CronsSortBy_CRONS_SORT_BY_THREAD_ID:
		return "thread_id"
	case coreapi.CronsSortBy_CRONS_SORT_BY_NEXT_RUN_DATE:
		return "next_run_date"
	case coreapi.CronsSortBy_CRONS_SORT_BY_END_TIME:
		return "end_time"
	case coreapi.CronsSortBy_CRONS_SORT_BY_CREATED_AT:
		return "created_at"
	case coreapi.CronsSortBy_CRONS_SORT_BY_UPDATED_AT:
		return "updated_at"
	default:
		return "" // unspecified → store defaults to created_at DESC
	}
}

// sortOrderToString maps the proto SortOrder enum to "asc" or "desc".
func sortOrderToString(so coreapi.SortOrder) string {
	if so == coreapi.SortOrder_ASC {
		return "asc"
	}
	return "desc"
}

// Count implements CronsServer.Count.
//
// Deviation: CountCronsRequest carries AssistantId and ThreadId as *UUID wrappers.
// We call GetValue() to extract the underlying string before passing to the store.
func (s *Service) Count(ctx context.Context, req *coreapi.CountCronsRequest) (*coreapi.CountResponse, error) {
	in := SearchInput{
		ThreadFilters: req.GetThreadFilters(),
	}
	if req.AssistantId != nil {
		in.AssistantID = req.GetAssistantId().GetValue()
	}
	if req.ThreadId != nil {
		in.ThreadID = req.GetThreadId().GetValue()
	}
	n, err := s.store.Count(ctx, in, req.GetFilters())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &coreapi.CountResponse{Count: n}, nil
}

// toPB converts the internal Cron to the proto Cron message.
// Payload JSONB is deserialized via protojson; empty/"{}" returns nil.
// OnRunCompleted is populated from a dedicated column (added by a later task);
// it remains nil until that column exists.
func toPB(c *Cron) *coreapi.Cron {
	pb := &coreapi.Cron{
		CronId:       &coreapi.UUID{Value: c.CronID},
		AssistantId:  c.AssistantID,
		Schedule:     c.Schedule,
		CreatedAt:    timestamppb.New(c.CreatedAt),
		UpdatedAt:    timestamppb.New(c.UpdatedAt),
		MetadataJson: c.Metadata,
		Enabled:      c.Enabled,
	}
	if p, ok := decodePayload(c.Payload); ok {
		pb.Payload = p
	}
	if c.ThreadID != "" {
		pb.ThreadId = &coreapi.UUID{Value: c.ThreadID}
	}
	if c.UserID != "" {
		pb.UserId = &c.UserID
	}
	if c.NextRunDate != nil {
		pb.NextRunDate = timestamppb.New(*c.NextRunDate)
	}
	if c.EndTime != nil {
		pb.EndTime = timestamppb.New(*c.EndTime)
	}
	if c.Timezone != "" {
		pb.Timezone = &c.Timezone
	}
	if c.OnRunCompleted != "" {
		if v, ok := enumcronorc.CronOnRunCompleted_value[c.OnRunCompleted]; ok {
			e := enumcronorc.CronOnRunCompleted(v)
			pb.OnRunCompleted = &e
		}
	}
	return pb
}

// decodePayload deserializes the cron.payload JSONB column into CronPayload.
// Returns (nil, false) for empty/{} so a cron with no payload degrades cleanly.
//
// New rows (4a) store payload as the Python-shaped dict crons.py's
// _payload_proto_to_dict produces (payloadDictToProto is its Go inverse).
// Rows written before 4a are protojson-shaped CronPayload messages instead;
// they're detected by the presence of "input_json"/"extra_json" — proto
// field names a Python payload dict would never carry at the top level —
// and parsed via jsonbutil as before.
func decodePayload(raw []byte) (*coreapi.CronPayload, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("{}")) {
		return nil, false
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil || len(fields) == 0 {
		return nil, false
	}

	if isLegacyPayloadShape(fields) {
		p := &coreapi.CronPayload{}
		if err := jsonbutil.Unmarshal(trimmed, p); err != nil {
			return nil, false
		}
		return p, true
	}

	var dict map[string]any
	if err := json.Unmarshal(trimmed, &dict); err != nil {
		return nil, false
	}
	return payloadDictToProto(dict), true
}

// Create implements CronsServer.Create.
//
// Auth filters: req.Filters (cron-scoped), req.AssistantFilters, req.ThreadFilters
// are forwarded to store.Create which implements the CTE-based authorization
// mirroring Python ops.py:2182-2260.  Missing/unauthorized → codes.NotFound
// ("Thread not found or not authorized" — ops.py:2279-2280).
//
// Invalid schedule → codes.InvalidArgument ("Invalid cron schedule" — ops.py:2174).
func (s *Service) Create(ctx context.Context, req *coreapi.CreateCronRequest) (*coreapi.Cron, error) {
	in := CreateCronInput{
		Schedule:         req.GetSchedule(),
		Timezone:         req.GetTimezone(),
		Enabled:          req.GetEnabled(),
		Metadata:         req.GetMetadataJson(),
		Filters:          req.GetFilters(),
		AssistantFilters: req.GetAssistantFilters(),
		ThreadFilters:    req.GetThreadFilters(),
	}
	if p := req.GetPayload(); p != nil {
		in.AssistantID = p.GetAssistantId()
		// 4a: store the Python-shaped dict (crons.py:_payload_proto_to_dict),
		// not protojson, so storage/ops.py's raw-SQL fallbacks and the gRPC
		// client agree on payload shape.
		b, err := json.Marshal(payloadProtoToDict(p))
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		in.Payload = b
	}
	if req.GetCronId() != nil {
		in.CronID = req.GetCronId().GetValue()
	}
	if req.GetThreadId() != nil {
		in.ThreadID = req.GetThreadId().GetValue()
	}
	in.UserID = req.GetUserId()
	if req.GetEndTime() != nil {
		t := req.GetEndTime().AsTime()
		in.EndTime = &t
	}
	if req.OnRunCompleted != nil {
		in.OnRunCompleted = req.GetOnRunCompleted().String()
	}
	c, err := s.store.Create(ctx, in)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// ops.py:2279: "Thread not found or not authorized"
			return nil, status.Error(codes.NotFound, "Thread not found or not authorized")
		}
		// Map schedule parse errors to InvalidArgument (ops.py:2174: 422 "Invalid cron schedule").
		// The bridge translates codes.InvalidArgument → HTTP 422.
		if isScheduleParseErr(err) {
			return nil, status.Error(codes.InvalidArgument, "Invalid cron schedule")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toPB(c), nil
}

// Patch implements CronsServer.Patch.
//
// PatchCronRequest.end_time is forwarded to PatchCronInput.EndTime (major gap fix).
// Invalid schedule → codes.InvalidArgument ("Invalid cron schedule").
func (s *Service) Patch(ctx context.Context, req *coreapi.PatchCronRequest) (*coreapi.Cron, error) {
	in := PatchCronInput{
		Schedule: req.GetSchedule(),     // proto Schedule is optional *string; getter returns "" when nil → store skips empty
		Timezone: req.GetTimezone(),     // same
		Metadata: req.GetMetadataJson(), // same — store skips empty bytes
	}
	if req.Enabled != nil { // honor optionality
		v := req.GetEnabled()
		in.Enabled = &v
	}
	if req.GetEndTime() != nil {
		t := req.GetEndTime().AsTime()
		in.EndTime = &t
	}
	if p := req.GetPayload(); p != nil {
		// 4a: same Python-shaped-dict encoding as Create.
		b, err := json.Marshal(payloadProtoToDict(p))
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		in.Payload = b
	}
	if req.OnRunCompleted != nil {
		s := req.GetOnRunCompleted().String()
		in.OnRunCompleted = &s
	}
	c, err := s.store.Patch(ctx, req.GetCronId().GetValue(), in, req.GetFilters())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		// Map schedule parse errors to InvalidArgument (ops.py:2174).
		if isScheduleParseErr(err) {
			return nil, status.Error(codes.InvalidArgument, "Invalid cron schedule")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toPB(c), nil
}

// isScheduleParseErr reports whether err originated from computeNextRun's cron-parse step.
// store.go wraps these as "schedule parse: <underlying>".
func isScheduleParseErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return len(msg) > 15 && msg[:15] == "schedule parse:"
}

// Delete implements CronsServer.Delete.
//
// 4f: zero rows affected (not found / not authorized) → codes.NotFound, so
// the client only yields the cron_id on an actual delete.
func (s *Service) Delete(ctx context.Context, req *coreapi.DeleteCronRequest) (*emptypb.Empty, error) {
	if err := s.store.Delete(ctx, req.GetCronId().GetValue(), req.GetFilters()); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Error(codes.NotFound, "Cron not found or not authorized")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &emptypb.Empty{}, nil
}

// Next implements CronsServer.Next. Returns all enabled, non-expired crons whose
// next_run_date <= now(). Called by the CronScheduler goroutine.
//
// The DB-clock snapshot (now()) is captured alongside each row in store.Next
// so the scheduler uses the DB's consistent "now" as the base for next-run
// computation (ops.py:2325 "select *, now() as now").
func (s *Service) Next(ctx context.Context, _ *emptypb.Empty) (*coreapi.NextCronsResponse, error) {
	cs, err := s.store.Next(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &coreapi.NextCronsResponse{}
	for _, cw := range cs {
		resp.Crons = append(resp.Crons, &coreapi.CronWithNow{
			Cron: toPB(cw.Cron),
			Now:  timestamppb.New(cw.Now), // DB now() snapshot (ops.py:2325)
		})
	}
	return resp, nil
}

// SetNextRunDate implements CronsServer.SetNextRunDate.
func (s *Service) SetNextRunDate(ctx context.Context, req *coreapi.SetNextRunDateRequest) (*emptypb.Empty, error) {
	t := req.GetNextRunDate().AsTime()
	if err := s.store.SetNextRunDate(ctx, req.GetCronId().GetValue(), t); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &emptypb.Empty{}, nil
}
