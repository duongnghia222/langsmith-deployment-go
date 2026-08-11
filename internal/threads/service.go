package threads

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	enumts "github.com/duongnghia222/langsmith-deployment-go/gen/enum_thread_status"
	"github.com/duongnghia222/langsmith-deployment-go/internal/config"
	"github.com/duongnghia222/langsmith-deployment-go/internal/jsonbutil"
	lsdstream "github.com/duongnghia222/langsmith-deployment-go/internal/stream"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CheckpointerCopier is the subset of the checkpointer.Store interface used by
// Threads.Copy. Defined as a local interface for testability (D6).
type CheckpointerCopier interface {
	CopyThread(ctx context.Context, fromThreadID, toThreadID string) error
}

// Service is a gRPC adapter that implements coreapi.ThreadsServer.
// Unimplemented methods (mutations) are covered by the embedded stub.
type Service struct {
	coreapi.UnimplementedThreadsServer
	pool         *pgxpool.Pool
	rdb          *goredis.Client
	store        *Store
	streamer     *lsdstream.Streamer
	cfg          *config.Config
	checkpointer CheckpointerCopier
}

// NewService creates a Service backed by a pgxpool.Pool.
func NewService(pool *pgxpool.Pool) *Service { return &Service{pool: pool, store: NewStore(pool)} }

// NewServiceWithStream creates a Service with Redis Streams support enabled.
func NewServiceWithStream(pool *pgxpool.Pool, rdb *goredis.Client, streamer *lsdstream.Streamer, cfg *config.Config) *Service {
	return &Service{pool: pool, rdb: rdb, store: NewStore(pool), streamer: streamer, cfg: cfg}
}

// NewServiceWithCheckpointer creates a Service that also copies checkpoints
// during Threads.Copy. Pass nil copier to get row-only Copy semantics.
func NewServiceWithCheckpointer(pool *pgxpool.Pool, copier CheckpointerCopier) *Service {
	return &Service{pool: pool, store: NewStore(pool), checkpointer: copier}
}

// WithCheckpointer wires a CheckpointerCopier onto an existing Service.
// Returns the same service for fluent chaining.
func (s *Service) WithCheckpointer(copier CheckpointerCopier) *Service {
	s.checkpointer = copier
	return s
}

// Get implements ThreadsServer.Get.
func (s *Service) Get(ctx context.Context, req *coreapi.GetThreadRequest) (*coreapi.Thread, error) {
	t, err := s.store.Get(ctx, req.GetThreadId().GetValue(), req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return toPB(t), nil
}

// Search implements ThreadsServer.Search.
// (C13) ids filter — ops.py:660-663
// (C14) sort_by / sort_order — ops.py:684-693
func (s *Service) Search(ctx context.Context, req *coreapi.SearchThreadsRequest) (*coreapi.SearchThreadsResponse, error) {
	in := SearchInput{
		Limit:  req.GetLimit(),
		Offset: req.GetOffset(),
	}
	if req.Status != nil {
		in.Status = thStatusEnumToText(*req.Status)
	}
	if len(req.GetMetadataJson()) > 0 {
		in.MetadataFilter = req.GetMetadataJson()
	}
	if len(req.GetValuesJson()) > 0 {
		in.ValuesFilter = req.GetValuesJson()
	}
	// (C13) ids filter
	if ids := req.GetIds(); len(ids) > 0 {
		in.Ids = make([]string, 0, len(ids))
		for _, u := range ids {
			in.Ids = append(in.Ids, u.GetValue())
		}
	}
	// (C14) sort_by / sort_order — map proto enum → column name (whitelist)
	if req.SortBy != nil {
		in.SortBy = threadsSortByToColumn(*req.SortBy)
	}
	if req.SortOrder != nil {
		if *req.SortOrder == coreapi.SortOrder_ASC {
			in.SortOrder = "asc"
		} else {
			in.SortOrder = "desc"
		}
	}
	rows, err := s.store.Search(ctx, in, req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	out := &coreapi.SearchThreadsResponse{}
	for _, r := range rows {
		out.Threads = append(out.Threads, toPB(r))
	}
	return out, nil
}

// Count implements ThreadsServer.Count.
func (s *Service) Count(ctx context.Context, req *coreapi.CountThreadsRequest) (*coreapi.CountResponse, error) {
	in := SearchInput{}
	if req.Status != nil {
		in.Status = thStatusEnumToText(*req.Status)
	}
	if len(req.GetMetadataJson()) > 0 {
		in.MetadataFilter = req.GetMetadataJson()
	}
	if len(req.GetValuesJson()) > 0 {
		in.ValuesFilter = req.GetValuesJson()
	}
	n, err := s.store.Count(ctx, in, req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return &coreapi.CountResponse{Count: n}, nil
}

// GetGraphID implements ThreadsServer.GetGraphID.
// Note: GetGraphIDRequest carries only ThreadId; there are no auth filters.
func (s *Service) GetGraphID(ctx context.Context, req *coreapi.GetGraphIDRequest) (*coreapi.GetGraphIDResponse, error) {
	gid, err := s.store.GetGraphID(ctx, req.GetThreadId().GetValue(), nil)
	if err != nil {
		return nil, mapErr(err)
	}
	return &coreapi.GetGraphIDResponse{GraphId: &gid}, nil
}

// toPB converts the internal Thread to the proto Thread message.
// The proto Thread uses *Fragment for Metadata/Config/Values. Interrupts JSONB
// is decoded as a map<string, Interrupts> where each value is a
// protojson-encoded Interrupts message; empty/"{}" yields a nil map.
func toPB(t *Thread) *coreapi.Thread {
	out := &coreapi.Thread{
		ThreadId:  &coreapi.UUID{Value: t.ThreadID},
		Status:    thStatusTextToEnum(t.Status),
		CreatedAt: timestamppb.New(t.CreatedAt),
		UpdatedAt: timestamppb.New(t.UpdatedAt),
		Metadata:  &coreapi.Fragment{Value: t.Metadata},
		Config:    &coreapi.Fragment{Value: t.Config},
		Values:    &coreapi.Fragment{Value: t.Values},
	}
	if m, ok := decodeInterrupts(t.Interrupts); ok {
		out.Interrupts = m
	}
	if t.ExpiresAt != nil {
		remaining := time.Until(*t.ExpiresAt).Minutes()
		if remaining < 0 {
			remaining = 0
		}
		out.Ttl = &coreapi.ThreadTTLInfo{
			ExpiresAt:  timestamppb.New(*t.ExpiresAt),
			TtlMinutes: remaining,
		}
	}
	if t.StateUpdatedAt != nil {
		out.StateUpdatedAt = timestamppb.New(*t.StateUpdatedAt)
	}
	return out
}

// decodeInterrupts parses the interrupts JSONB column. The column stores a
// JSON object whose values are themselves protojson-encoded Interrupts
// messages. Empty/"{}" returns (nil, false).
func decodeInterrupts(raw []byte) (map[string]*coreapi.Interrupts, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte(`{}`)) {
		return nil, false
	}
	var outer map[string]json.RawMessage
	if err := json.Unmarshal(raw, &outer); err != nil {
		return nil, false
	}
	if len(outer) == 0 {
		return nil, false
	}
	result := make(map[string]*coreapi.Interrupts, len(outer))
	for k, v := range outer {
		one := &coreapi.Interrupts{}
		if err := jsonbutil.Unmarshal(v, one); err != nil {
			continue
		}
		result[k] = one
	}
	if len(result) == 0 {
		return nil, false
	}
	return result, true
}

// mapErr converts store errors to gRPC status errors.
func mapErr(err error) error {
	if errors.Is(err, ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	// ops.py always surfaces 404 for auth-filter exclusion (ops.py:2018, 2280).
	if errors.Is(err, ErrForbidden) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// thStatusEnumToText converts a ThreadStatus enum to the lowercase text stored
// in the DB. The proto enum values are already lowercase ("idle", "busy", etc.),
// so String() returns the correct text directly.
func thStatusEnumToText(e enumts.ThreadStatus) string {
	return e.String()
}

// thStatusTextToEnum converts a DB status text to a ThreadStatus enum value.
// Falls back to ThreadStatus_idle (0) for unknown values.
func thStatusTextToEnum(s string) enumts.ThreadStatus {
	if v, ok := enumts.ThreadStatus_value[s]; ok {
		return enumts.ThreadStatus(v)
	}
	return enumts.ThreadStatus(0)
}

// Create implements ThreadsServer.Create.
// (C12) Atomic ON CONFLICT DO NOTHING / raise via store.Create.
func (s *Service) Create(ctx context.Context, req *coreapi.CreateThreadRequest) (*coreapi.Thread, error) {
	var threadID string
	if req.ThreadId != nil {
		threadID = req.GetThreadId().GetValue()
	}
	var ifExists string
	switch req.GetIfExists() {
	case coreapi.OnConflictBehavior_DO_NOTHING:
		ifExists = "do_nothing"
	default: // RAISE
		ifExists = "raise"
	}
	ttlSecs := extractTTLSeconds(req.GetTtl())
	th, err := s.store.Create(ctx, CreateThreadInput{
		ThreadID:   threadID,
		Metadata:   req.GetMetadataJson(),
		TTLSeconds: ttlSecs,
		IfExists:   ifExists,
		Filters:    req.GetFilters(),
	})
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, mapErr(err)
	}
	return toPB(th), nil
}

// Patch implements ThreadsServer.Patch.
func (s *Service) Patch(ctx context.Context, req *coreapi.PatchThreadRequest) (*coreapi.Thread, error) {
	ttlSecs := extractTTLSeconds(req.GetTtl())
	th, err := s.store.Patch(ctx, req.GetThreadId().GetValue(), PatchThreadInput{
		Metadata:   req.GetMetadataJson(),
		TTLSeconds: ttlSecs,
	}, req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return toPB(th), nil
}

// Delete implements ThreadsServer.Delete.
func (s *Service) Delete(ctx context.Context, req *coreapi.DeleteThreadRequest) (*coreapi.UUID, error) {
	id, err := s.store.Delete(ctx, req.GetThreadId().GetValue(), req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return &coreapi.UUID{Value: id}, nil
}

// SetStatus implements ThreadsServer.SetStatus.
// (C11) Mirrors Python ops.py:891-944 exactly:
//   - Computes status from exception/next fields
//   - Writes values and interrupts from the checkpoint payload
//   - Applies the busy CASE (existing pending/running runs → 'busy')
//   - When the result is 'busy', wakes a worker (ops.py:940-944)
func (s *Service) SetStatus(ctx context.Context, req *coreapi.SetThreadStatusRequest) (*emptypb.Empty, error) {
	var statusText string
	if len(req.GetExceptionJson()) > 0 {
		statusText = "error"
	} else if cp := req.GetCheckpoint(); cp != nil && len(cp.GetNext()) > 0 {
		statusText = "interrupted"
	} else {
		statusText = "idle"
	}
	// (C11) Extract values/interrupts from the checkpoint payload.
	// ops.py:936: values = Jsonb(checkpoint["values"]) if checkpoint else None
	var valuesJSON []byte
	var interruptsJSON []byte
	if cp := req.GetCheckpoint(); cp != nil {
		if len(cp.GetValuesJson()) > 0 {
			valuesJSON = cp.GetValuesJson()
		}
		if len(cp.GetInterruptsJson()) > 0 {
			interruptsJSON = cp.GetInterruptsJson()
		}
	}
	busy, err := s.store.SetStatus(ctx, SetStatusInput{
		ThreadID:   req.GetThreadId().GetValue(),
		StatusText: statusText,
		ValuesJSON: valuesJSON,
		Interrupts: interruptsJSON,
	})
	if err != nil {
		return nil, mapErr(err)
	}
	// (C11) ops.py:940-944: if row["status"] == "busy": await wake_up_worker()
	if busy && s.rdb != nil {
		_ = s.rdb.RPush(ctx, lsdstream.RunQueueKey(), req.GetThreadId().GetValue()).Err()
	}
	return &emptypb.Empty{}, nil
}

// SetJointStatus implements ThreadsServer.SetJointStatus.
// EncryptionContext is intentionally ignored in R3 (encryption is out of scope).
// (C4) ops.py:1031-1032: if run_status == "pending": await wake_up_worker()
func (s *Service) SetJointStatus(ctx context.Context, req *coreapi.SetThreadJointStatusRequest) (*emptypb.Empty, error) {
	in := SetJointStatusInput{
		ThreadID:       req.GetThreadId().GetValue(),
		RunID:          req.GetRunId().GetValue(),
		RunStatus:      req.GetRunStatus(),
		GraphID:        req.GetGraphId(),
		Next:           req.GetCheckpoint().GetNext(),
		ValuesJSON:     req.GetCheckpoint().GetValuesJson(),
		InterruptsJSON: req.GetCheckpoint().GetInterruptsJson(),
		ExceptionJSON:  req.GetExceptionJson(),
	}
	if err := s.store.SetJointStatus(ctx, in); err != nil {
		return nil, mapErr(err)
	}
	// (C4) ops.py:1031-1032: wake up worker when run_status == "pending"
	if in.RunStatus == "pending" && s.rdb != nil {
		_ = s.rdb.RPush(ctx, lsdstream.RunQueueKey(), in.RunID).Err()
	}
	return &emptypb.Empty{}, nil
}

// Copy implements ThreadsServer.Copy.
// Copies the thread row, then delegates checkpoint copy to the wired
// CheckpointerCopier (D6). If no copier is wired, only the row is copied.
func (s *Service) Copy(ctx context.Context, req *coreapi.CopyThreadRequest) (*coreapi.Thread, error) {
	th, err := s.store.Copy(ctx, req.GetThreadId().GetValue(), req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	if s.checkpointer != nil {
		if err := s.checkpointer.CopyThread(ctx, req.GetThreadId().GetValue(), th.ThreadID); err != nil {
			return nil, status.Errorf(codes.Internal, "copy checkpoints: %v", err)
		}
	}
	return toPB(th), nil
}

// Stream implements ThreadsServer.Stream — a server-stream RPC that tails
// thread:{id}:events and delivers StreamEvents to the client.
//
// Auth filters are validated once at the start via ThreadExistsAndAuth.
// The stream tails the Redis stream from LastEventId (or "0-0" if absent),
// delivering events in order until the client's context is cancelled.
func (s *Service) Stream(req *coreapi.StreamThreadRequest, grpcStream coreapi.Threads_StreamServer) error {
	if s.streamer == nil {
		return status.Error(codes.Unavailable, "streaming not configured")
	}

	ctx := grpcStream.Context()
	threadID := req.GetThreadId().GetValue()

	threadUUID, err := uuid.Parse(threadID)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid thread_id")
	}

	if err := s.store.ThreadExistsAndAuth(ctx, threadID, req.GetFilters()); err != nil {
		switch {
		case errors.Is(err, ErrForbidden):
			// ops.py always surfaces 404 for auth-filter exclusion (ops.py:2018, 2280).
			return status.Error(codes.NotFound, err.Error())
		case errors.Is(err, ErrNotFound):
			return status.Error(codes.NotFound, err.Error())
		default:
			return status.Error(codes.Internal, err.Error())
		}
	}

	cursor := "0-0"
	if req.LastEventId != nil && *req.LastEventId != "" {
		cursor = *req.LastEventId
	}

	replayBatch := int64(100)
	if s.cfg != nil && s.cfg.StreamReplayBatch > 0 {
		replayBatch = s.cfg.StreamReplayBatch
	}
	blockMs := 500
	if s.cfg != nil && s.cfg.StreamReadBlockMs > 0 {
		blockMs = s.cfg.StreamReadBlockMs
	}
	streamKey := lsdstream.ThreadStreamKey(threadUUID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		entries, err := s.streamer.XReadFrom(ctx, streamKey, cursor, replayBatch, blockMs)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return status.Error(codes.Internal, fmt.Sprintf("xread: %v", err))
		}

		for _, entry := range entries {
			cursor = entry.ID
			eventType, _ := entry.Fields["event_type"].(string)
			var msgBytes []byte
			switch v := entry.Fields["message"].(type) {
			case []byte:
				msgBytes = v
			case string:
				msgBytes = []byte(v)
			}
			streamID := entry.ID
			if err := grpcStream.Send(&coreapi.StreamEvent{
				EventType: eventType,
				Message:   msgBytes,
				StreamId:  &streamID,
			}); err != nil {
				return err
			}
		}
	}
}

// extractTTLSeconds converts a ThreadTTLConfig.default_ttl (minutes per Python
// convention in api/config.py) into seconds, or returns nil if no TTL is set.
func extractTTLSeconds(cfg *coreapi.ThreadTTLConfig) *float64 {
	if cfg == nil || cfg.DefaultTtl == nil {
		return nil
	}
	secs := cfg.GetDefaultTtl() * 60
	return &secs
}

// threadsSortByToColumn maps the ThreadsSortBy enum to a safe SQL column name.
// Keep-verbatim from Python: valid_sort_fields = ["thread_id","status","created_at","updated_at"]
// THREADS_SORT_BY_STATE_UPDATED_AT is not in Python's whitelist; falls back to "".
func threadsSortByToColumn(sb coreapi.ThreadsSortBy) string {
	switch sb {
	case coreapi.ThreadsSortBy_THREADS_SORT_BY_THREAD_ID:
		return "thread_id"
	case coreapi.ThreadsSortBy_THREADS_SORT_BY_STATUS:
		return "status"
	case coreapi.ThreadsSortBy_THREADS_SORT_BY_CREATED_AT:
		return "created_at"
	case coreapi.ThreadsSortBy_THREADS_SORT_BY_UPDATED_AT:
		return "updated_at"
	default:
		return "" // falls back to "created_at DESC" in sortClause
	}
}

