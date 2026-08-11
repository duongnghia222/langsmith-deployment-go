package runs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	enumca "github.com/duongnghia222/langsmith-deployment-go/gen/enum_cancel_run_action"
	enumcs "github.com/duongnghia222/langsmith-deployment-go/gen/enum_control_signal"
	enumms "github.com/duongnghia222/langsmith-deployment-go/gen/enum_multitask_strategy"
	enumrs "github.com/duongnghia222/langsmith-deployment-go/gen/enum_run_status"
	"github.com/duongnghia222/langsmith-deployment-go/internal/config"
	lsdstream "github.com/duongnghia222/langsmith-deployment-go/internal/stream"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is a gRPC adapter that implements coreapi.RunsServer.
// Unimplemented methods (mutations) are covered by the embedded stub.
type Service struct {
	coreapi.UnimplementedRunsServer
	pool     *pgxpool.Pool
	rdb      *goredis.Client
	store    *Store
	streamer *lsdstream.Streamer
	cfg      *config.Config
}

// NewService creates a Service backed by a pgxpool.Pool and a Redis client.
// streamer and cfg are set to nil; Publish will return Unavailable if called.
func NewService(pool *pgxpool.Pool, rdb *goredis.Client) *Service {
	return &Service{pool: pool, rdb: rdb, store: NewStore(pool)}
}

// NewServiceWithStream creates a Service with Redis Streams support enabled.
// (item 6) Uses NewStoreWithLeaseTTL so the service store uses cfg.LeaseTTL
// for ExtendLease and Next's lease grant.
func NewServiceWithStream(pool *pgxpool.Pool, rdb *goredis.Client, streamer *lsdstream.Streamer, cfg *config.Config) *Service {
	var leaseSecs int64 = 30
	if cfg != nil {
		leaseSecs = int64(cfg.LeaseTTL.Seconds())
	}
	return &Service{pool: pool, rdb: rdb, store: NewStoreWithLeaseTTL(pool, leaseSecs), streamer: streamer, cfg: cfg}
}

// Get implements RunsServer.Get.
//
// Deviation: GetRunRequest carries RunId and ThreadId as *UUID wrappers, not plain strings.
// We call .GetValue() to extract the underlying string before passing to the store.
func (s *Service) Get(ctx context.Context, req *coreapi.GetRunRequest) (*coreapi.Run, error) {
	r, err := s.store.Get(ctx, req.GetRunId().GetValue(), req.GetThreadId().GetValue(), req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return toPB(r), nil
}

// Search implements RunsServer.Search.
func (s *Service) Search(ctx context.Context, req *coreapi.SearchRunsRequest) (*coreapi.SearchRunsResponse, error) {
	in := SearchInput{
		Limit:  req.GetLimit(),
		Offset: req.GetOffset(),
	}
	if req.ThreadId != nil {
		in.ThreadID = req.GetThreadId().GetValue()
	}
	if req.Status != nil {
		in.Statuses = []string{req.GetStatus().String()}
	}
	rows, err := s.store.Search(ctx, in, req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	out := &coreapi.SearchRunsResponse{}
	for _, r := range rows {
		out.Runs = append(out.Runs, toPB(r))
	}
	return out, nil
}

// Count implements RunsServer.Count.
func (s *Service) Count(ctx context.Context, req *coreapi.CountRunsRequest) (*coreapi.CountResponse, error) {
	in := SearchInput{}
	if req.ThreadId != nil {
		in.ThreadID = req.GetThreadId().GetValue()
	}
	if len(req.GetStatuses()) > 0 {
		in.Statuses = req.GetStatuses()
	}
	n, err := s.store.Count(ctx, in, nil)
	if err != nil {
		return nil, mapErr(err)
	}
	return &coreapi.CountResponse{Count: n}, nil
}

// Stats implements RunsServer.Stats.
func (s *Service) Stats(ctx context.Context, _ *emptypb.Empty) (*coreapi.RunStats, error) {
	st, err := s.store.Stats(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &coreapi.RunStats{
		NPending:                            st.NPending,
		NRunning:                            st.NRunning,
		PendingRunsWaitTimeMaxSecs:          st.PendingWaitMaxSecs,
		PendingRunsWaitTimeMedSecs:          st.PendingWaitMedSecs,
		PendingUnblockedRunsWaitTimeMaxSecs: st.PendingUnblockedWaitMaxSecs,
	}, nil
}

// PoolStats implements RunsServer.PoolStats.
//
// Postgres mapping:
//   PoolMax       = MaxConns()    — configured upper bound
//   PoolSize      = TotalConns()  — currently open connections (idle + acquired + constructing)
//   PoolAvailable = IdleConns()   — connections ready to use immediately
//   RequestsQueued = EmptyAcquireCount() — times a caller had to wait because the pool was empty
//   RequestsErrors = 0            — pgxpool does not expose a connection-error counter
//
// Redis mapping (via go-redis PoolStats):
//   IdleConnections  = IdleConns    — connections not currently checked out
//   InUseConnections = TotalConns - IdleConns
//   MaxConnections   = TotalConns   — go-redis PoolStats has no separate MaxConns field
func (s *Service) PoolStats(ctx context.Context, _ *emptypb.Empty) (*coreapi.ConnectionPoolStats, error) {
	if s.rdb == nil {
		return nil, status.Error(codes.Unavailable, "redis not configured")
	}
	pgStat := s.pool.Stat()
	rdStat := s.rdb.PoolStats()

	// safe in practice — pool sizes are configured in the hundreds
	inUse := int32(rdStat.TotalConns) - int32(rdStat.IdleConns)
	if inUse < 0 {
		inUse = 0
	}

	return &coreapi.ConnectionPoolStats{
		Postgres: &coreapi.PostgresPoolStats{
			PoolMax:        pgStat.MaxConns(),
			PoolSize:       pgStat.TotalConns(),
			PoolAvailable:  pgStat.IdleConns(),
			RequestsQueued: pgStat.EmptyAcquireCount(),
			RequestsErrors: 0, // pgxpool does not expose a connection-error counter
		},
		Redis: &coreapi.RedisPoolStats{
			IdleConnections:  int32(rdStat.IdleConns),
			InUseConnections: inUse,
			MaxConnections:   int32(rdStat.TotalConns),
		},
	}, nil
}

// toPB converts the internal Run to the proto Run message.
// Kwargs JSONB is passed through verbatim as opaque bytes so Python receives
// the dict shape it wrote (preserving arbitrary keys under config.configurable).
// MultitaskStrategy mapped from text to enum; falls back to zero value on unknown strings.
func toPB(r *Run) *coreapi.Run {
	return &coreapi.Run{
		RunId:             &coreapi.UUID{Value: r.RunID},
		ThreadId:          &coreapi.UUID{Value: r.ThreadID},
		AssistantId:       &coreapi.UUID{Value: r.AssistantID},
		Status:            runStatusTextToEnum(r.Status),
		MultitaskStrategy: multitaskTextToEnum(r.MultitaskStrategy),
		CreatedAt:         timestamppb.New(r.CreatedAt),
		UpdatedAt:         timestamppb.New(r.UpdatedAt),
		Metadata:          &coreapi.Fragment{Value: r.Metadata},
		KwargsJson:        r.Kwargs,
	}
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

// runStatusTextToEnum converts a DB status string to a RunStatus enum value.
// Falls back to RunStatus_pending (0) for unknown values.
func runStatusTextToEnum(s string) enumrs.RunStatus {
	if v, ok := enumrs.RunStatus_value[s]; ok {
		return enumrs.RunStatus(v)
	}
	return enumrs.RunStatus(0)
}

// multitaskTextToEnum converts a DB multitask_strategy string to a MultitaskStrategy enum value.
// Falls back to MultitaskStrategy_reject (0) for unknown values.
func multitaskTextToEnum(s string) enumms.MultitaskStrategy {
	if v, ok := enumms.MultitaskStrategy_value[s]; ok {
		return enumms.MultitaskStrategy(v)
	}
	return enumms.MultitaskStrategy(0)
}

// Create implements RunsServer.Create.
//
// Auth: both thread_filters and assistant_filters from the request are forwarded
// to the store layer, which validates them in sub-selects before inserting.
// Redis RPUSH to the run queue signals workers waiting on BLPOP.
//
// (item 1) req.UserId is forwarded to CreateRunInput.UserID for injection into
//   kwargs.config.configurable.user_id (ops.py:1571 params["user_id"]).
// (item 3) req.IfNotExists is forwarded to CreateRunInput.IfNotExists; when
//   CREATE_THREAD_IF_THREAD_NOT_EXISTS the store auto-creates the thread.
func (s *Service) Create(ctx context.Context, req *coreapi.CreateRunRequest) (*coreapi.CreateRunResponse, error) {
	strategy := req.GetMultitaskStrategy().String()
	in := CreateRunInput{
		ThreadID:          req.GetThreadId().GetValue(),
		AssistantID:       req.GetAssistantId().GetValue(),
		Status:            req.GetStatus().String(),
		KwargsJSON:        req.GetKwargsJson(),
		Metadata:          req.GetMetadataJson(),
		MultitaskStrategy: strategy,
		AfterSeconds:      req.GetAfterSeconds(),
		UserID:            req.GetUserId(),                       // (item 1)
		IfNotExists:       int32(req.GetIfNotExists()),           // (item 3)
	}
	if req.RunId != nil {
		in.RunID = req.GetRunId().GetValue()
	}
	r, err := s.store.Create(ctx, in, req.GetThreadFilters(), req.GetAssistantFilters())
	if err != nil {
		if errors.Is(err, ErrInflight) {
			return nil, status.Error(codes.AlreadyExists, "run inflight on thread")
		}
		return nil, mapErr(err)
	}
	// Wake up any worker waiting on BLPOP queue:runs:pending.
	// Skip for delayed runs — they'll be picked up on the next poll cycle.
	if s.rdb != nil && in.AfterSeconds == 0 {
		_ = s.rdb.RPush(ctx, lsdstream.RunQueueKey(), r.RunID).Err()
	}
	return &coreapi.CreateRunResponse{Runs: []*coreapi.Run{toPB(r)}}, nil
}

// Delete implements RunsServer.Delete.
func (s *Service) Delete(ctx context.Context, req *coreapi.DeleteRunRequest) (*coreapi.UUID, error) {
	id, err := s.store.Delete(ctx, req.GetRunId().GetValue(), req.GetThreadId().GetValue(), req.GetFilters())
	if err != nil {
		return nil, mapErr(err)
	}
	return &coreapi.UUID{Value: id}, nil
}

// Cancel implements RunsServer.Cancel.
//
// Supports both targets:
//   - run_ids: cancel a specific set of runs on a thread.
//   - status:  cancel all runs in the given status bucket (pending / running / all).
//
// (C6) After DB update: for each affected run publish the control signal via
// s.streamer to RunControlChannel AND set the Redis control key with 60s TTL.
// JSON format {"signal":"interrupt"|"rollback"} for the publish.
// (C2) action=rollback deletes pending runs; action=interrupt transitions them
// to 'interrupted'. Running runs always get cancel_requested_at (worker handles).
func (s *Service) Cancel(ctx context.Context, req *coreapi.CancelRunRequest) (*emptypb.Empty, error) {
	// Determine the action ("interrupt" or "rollback") from the proto request.
	// Default is "interrupt" when unset (CancelRunAction zero value = interrupt).
	action := "interrupt"
	if req.Action != nil && *req.Action == enumca.CancelRunAction_rollback {
		action = "rollback"
	}

	if target := req.GetRunIds(); target != nil {
		runIDs := make([]string, 0, len(target.GetRunIds()))
		for _, u := range target.GetRunIds() {
			runIDs = append(runIDs, u.GetValue())
		}
		threadID := ""
		if target.ThreadId != nil {
			threadID = target.GetThreadId().GetValue()
		}
		results, err := s.store.CancelWithAction(ctx, runIDs, threadID, action, req.GetFilters())
		if err != nil {
			return nil, mapErr(err)
		}
		s.publishCancelSignals(ctx, results, action)
		return &emptypb.Empty{}, nil
	}
	if target := req.GetStatus(); target != nil {
		statuses := cancelStatusToRunStatuses(target.GetStatus())
		if len(statuses) == 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid cancel status")
		}
		ids, err := s.store.CancelByStatus(ctx, statuses, req.GetFilters())
		if err != nil {
			return nil, mapErr(err)
		}
		// CancelByStatus only sets cancel_requested_at (no action semantics),
		// always treated as interrupt for signal purposes.
		fakeResults := make([]CancelResult, len(ids))
		for i, id := range ids {
			fakeResults[i] = CancelResult{RunID: id, Deleted: false}
		}
		s.publishCancelSignals(ctx, fakeResults, "interrupt")
		return &emptypb.Empty{}, nil
	}
	return nil, status.Error(codes.InvalidArgument, "either run_ids or status target is required")
}

// publishCancelSignals publishes the cancel control signal to Redis for each
// non-deleted run in results. Called after DB update in Cancel.
//
// (C6) Python ops.py:1834-1837: for each run_id:
//   - SET run:{id}:control = action EX 60  (STRING_RUN_CONTROL, 60s TTL)
//   - PUBLISH run:{id}:control action
//
// Go publishes JSON {"signal":"..."} (its own consumer format) and also
// sets the plain-string control key (Python worker compat — ops.py:2432).
func (s *Service) publishCancelSignals(ctx context.Context, results []CancelResult, action string) {
	if len(results) == 0 {
		return
	}
	for _, r := range results {
		if r.Deleted {
			continue // hard-deleted pending runs have no worker to signal
		}
		runUUID, err := uuid.Parse(r.RunID)
		if err != nil {
			continue
		}
		controlCh := lsdstream.RunControlChannel(runUUID)

		// (C6) Set the control STRING key with 60s TTL so late-starting workers
		// see the cancel signal (Python ops.py:1836: ex=60 keep-verbatim).
		if s.rdb != nil {
			_ = s.rdb.Set(ctx, controlCh, action, 60*time.Second).Err()
		}

		// (C6) Publish JSON payload on the control channel.
		// Go workers (Enter) parse {"signal":"..."} JSON format.
		if s.streamer != nil {
			payload, _ := json.Marshal(map[string]string{"signal": action})
			_ = s.streamer.Publish(ctx, controlCh, payload)
		}
	}
}

// cancelStatusToRunStatuses maps a CancelRunStatus enum to the set of run.status
// values it selects. CANCEL_RUN_STATUS_ALL means pending OR running — terminal
// states (success, error, timeout, interrupted) are never cancelled.
func cancelStatusToRunStatuses(cs coreapi.CancelRunStatus) []string {
	switch cs {
	case coreapi.CancelRunStatus_CANCEL_RUN_STATUS_PENDING:
		return []string{"pending"}
	case coreapi.CancelRunStatus_CANCEL_RUN_STATUS_RUNNING:
		return []string{"running"}
	case coreapi.CancelRunStatus_CANCEL_RUN_STATUS_ALL:
		return []string{"pending", "running"}
	}
	return nil
}

// SetStatus implements RunsServer.SetStatus.
//
// (C7) When the new status is terminal, publish "done" to RunTerminalChannel
// so Stream consumers know the run has finished. Mirrors Python ops.py:1436
// (enter's finally block publishes "done" on CHANNEL_RUN_CONTROL).
//
// (C6) SetStatus to 'pending' wakes workers (Python ops.py:1960-1961).
func (s *Service) SetStatus(ctx context.Context, req *coreapi.SetRunStatusRequest) (*emptypb.Empty, error) {
	newStatus := req.GetStatus().String()
	if err := s.store.SetStatus(ctx, req.GetRunId().GetValue(), newStatus); err != nil {
		return nil, mapErr(err)
	}
	runID := req.GetRunId().GetValue()
	if isTerminalStatus(newStatus) {
		s.publishTerminalDone(ctx, runID)
	}
	// (C6) Python ops.py:1960-1961: wake_up_worker() when status set to 'pending'.
	if newStatus == "pending" && s.rdb != nil {
		_ = s.rdb.RPush(ctx, lsdstream.RunQueueKey(), runID).Err()
	}
	return &emptypb.Empty{}, nil
}

// MarkDone implements RunsServer.MarkDone.
//
// (C7) Publish "done" to RunTerminalChannel after DB update, mirroring Python
// ops.py:1436: `await get_redis().publish(CHANNEL_RUN_CONTROL.format(run_id), "done")`.
func (s *Service) MarkDone(ctx context.Context, req *coreapi.MarkRunDoneRequest) (*emptypb.Empty, error) {
	if err := s.store.MarkDone(ctx, req.GetRunId().GetValue(), req.GetResumable()); err != nil {
		return nil, mapErr(err)
	}
	s.publishTerminalDone(ctx, req.GetRunId().GetValue())
	return &emptypb.Empty{}, nil
}

// publishTerminalDone publishes "done" to RunTerminalChannel for a run that has
// reached a terminal state. Stream consumers (Runs.Stream) listen on this channel
// to know when to close the stream.
//
// (C7) Keep-verbatim payload: "done" (Python ops.py:1436).
func (s *Service) publishTerminalDone(ctx context.Context, runID string) {
	if s.streamer == nil {
		return
	}
	runUUID, err := uuid.Parse(runID)
	if err != nil {
		return
	}
	_ = s.streamer.Publish(ctx, lsdstream.RunTerminalChannel(runUUID), []byte("done"))
}

// Next implements RunsServer.Next.
//
// If req.GetWait() is true and no runs are pending, the handler blocks up to
// 5 seconds via Redis BLPOP on the run queue before retrying once.
func (s *Service) Next(ctx context.Context, req *coreapi.NextRunRequest) (*coreapi.NextRunResponse, error) {
	limit := req.GetLimit()
	if limit == 0 {
		limit = 1
	}
	claimed, err := s.store.Next(ctx, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	if len(claimed) == 0 && req.GetWait() && s.rdb != nil {
		// Block up to 5 s waiting for a new run to be queued.
		_ = s.rdb.BLPop(ctx, 5*time.Second, lsdstream.RunQueueKey()).Err()
		claimed, err = s.store.Next(ctx, limit)
		if err != nil {
			return nil, mapErr(err)
		}
	}
	resp := &coreapi.NextRunResponse{}
	for _, c := range claimed {
		resp.Runs = append(resp.Runs, &coreapi.RunWithAttempt{
			Run:     toPB(c.Run),
			Attempt: c.Attempt,
		})
	}
	return resp, nil
}

// Sweep implements RunsServer.Sweep.
//
// (C8) After resetting expired runs to 'pending', wake workers via RPUSH on
// the run queue, mirroring Python ops.py:1473: wake_up_worker().
func (s *Service) Sweep(ctx context.Context, _ *emptypb.Empty) (*coreapi.SweepRunsResponse, error) {
	swept, err := s.store.Sweep(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// (C8) Wake workers for each re-queued run (Python ops.py:1473: wake_up_worker()).
	if len(swept) > 0 && s.rdb != nil {
		for _, id := range swept {
			_ = s.rdb.RPush(ctx, lsdstream.RunQueueKey(), id).Err()
		}
	}
	resp := &coreapi.SweepRunsResponse{}
	for _, id := range swept {
		resp.RunIds = append(resp.RunIds, &coreapi.UUID{Value: id})
	}
	return resp, nil
}

// Publish implements RunsServer.Publish.
//
// It validates that the run exists (and satisfies any auth filters), then
// appends the event to both the run stream and the thread stream (best-effort
// mirror). PublishStreamEventRequest has no Filters field, so auth filters are
// passed as nil to PublishExistsAndAuth.
func (s *Service) Publish(ctx context.Context, req *coreapi.PublishStreamEventRequest) (*emptypb.Empty, error) {
	if s.streamer == nil {
		return nil, status.Error(codes.Unavailable, "streaming not configured")
	}

	runID := req.GetRunId().GetValue()
	threadID := req.GetThreadId().GetValue()

	// Validate UUIDs before hitting the database; a malformed ID would produce
	// a Postgres "invalid input syntax for type uuid" error (codes.Internal) rather
	// than the correct codes.InvalidArgument.
	runUUID, err := uuid.Parse(runID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid run_id")
	}
	threadUUID, err := uuid.Parse(threadID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid thread_id")
	}

	// Guard: verify run exists (no auth filters on the Publish RPC).
	if err := s.store.PublishExistsAndAuth(ctx, runID, threadID, nil); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		if errors.Is(err, ErrForbidden) {
			// ops.py always surfaces 404 for auth-filter exclusion (ops.py:2018, 2280).
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	maxLen := int64(1000)
	if s.cfg != nil && s.cfg.StreamMaxLen > 0 {
		maxLen = s.cfg.StreamMaxLen
	}

	fields := map[string]any{
		"event_type": req.GetEventType(),
		"message":    req.GetMessage(),
		"run_id":     runID,
		"resumable":  fmt.Sprintf("%t", req.GetResumable()),
	}

	// Primary write: run stream.
	if _, err := s.streamer.XAdd(ctx, lsdstream.RunStreamKey(runUUID), fields, maxLen); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("xadd run stream: %v", err))
	}

	// Mirror write: thread stream (best-effort; failure does not abort).
	_, _ = s.streamer.XAdd(ctx, lsdstream.ThreadStreamKey(threadUUID), fields, maxLen)

	return &emptypb.Empty{}, nil
}

// Enter implements RunsServer.Enter — a server-streaming RPC for workers.
//
// The worker calls Enter immediately after claiming a run via Next. LSD:
//  1. Spawns a heartbeat goroutine that extends the run's lease every HeartbeatInterval.
//  2. Subscribes to run:{id}:control (Redis Pub/Sub) for interrupt/rollback signals.
//  3. Streams ControlEvents to the worker until the context is cancelled or an error occurs.
//
// Heartbeat failures (lease lost) cause the stream to close, signalling the worker
// that it should stop and requeue.
func (s *Service) Enter(req *coreapi.EnterRunRequest, stream coreapi.Runs_EnterServer) error {
	if s.streamer == nil {
		return status.Error(codes.Unavailable, "streaming not configured")
	}

	ctx := stream.Context()
	runID := req.GetRunId().GetValue()

	runUUID, err := uuid.Parse(runID)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid run_id")
	}

	controlChannel := lsdstream.RunControlChannel(runUUID)

	// (C6) Subscribe FIRST, then check the pre-existing control STRING key.
	// This mirrors Python listen_for_cancellation (ops.py:2431-2436):
	//   1. await pubsub.subscribe(channel)
	//   2. if start_value := await get_redis().get(STRING_RUN_CONTROL...): handle it
	// By subscribing before the GET we avoid a race where Cancel fires between
	// the GET and the subscribe.
	sub, err := s.streamer.Subscribe(ctx, controlChannel)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("subscribe control: %v", err))
	}
	defer sub.Close() //nolint:errcheck

	// (C6) Check for a pre-existing cancel signal in the Redis STRING key
	// (set by Cancel with 60s TTL). A late-starting worker sees the cancel
	// even if it missed the PUBLISH. Mirrors Python ops.py:2432-2436.
	if s.rdb != nil {
		if val, err := s.rdb.Get(ctx, controlChannel).Result(); err == nil && val != "" {
			action := parseControlSignal(val)
			if err := stream.Send(&coreapi.ControlEvent{Action: action}); err != nil {
				return err
			}
			return nil
		}
	}

	// Heartbeat goroutine: extend lease on a ticker.
	heartbeatInterval := 5 * time.Second
	if s.cfg != nil && s.cfg.HeartbeatInterval > 0 {
		heartbeatInterval = s.cfg.HeartbeatInterval
	}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	heartbeatErrCh := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.store.ExtendLease(ctx, runID, ""); err != nil {
					heartbeatErrCh <- err
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatErrCh:
			return status.Error(codes.Aborted, "lease lost")
		case msg, ok := <-sub.Channel():
			if !ok {
				return nil
			}
			action := parseControlSignal(msg.Payload)
			if err := stream.Send(&coreapi.ControlEvent{Action: action}); err != nil {
				return err
			}
		}
	}
}

// Stream implements RunsServer.Stream — the bidirectional SSE handler for run consumers.
//
// Protocol (two-message handshake):
//  1. Client sends StreamRunClientMessage{Subscribe: SubscribeRunRequest{run_id, thread_id}}.
//     Server starts tailing RunStreamKey(runID) into a buffer goroutine.
//     Server responds with a control StreamEvent{event_type:"control", message:"subscribed"}.
//  2. Client sends StreamRunClientMessage{Join: JoinRunRequest{filters, cancel_on_disconnect,
//     last_event_id, ignore_run_not_found}}. Server validates auth, optionally honours
//     last_event_id, then enters the main loop until run is terminal or ctx is done.
func (s *Service) Stream(grpcStream coreapi.Runs_StreamServer) error {
	if s.streamer == nil {
		return status.Error(codes.Unavailable, "streaming not configured")
	}

	ctx := grpcStream.Context()

	// ── Phase 1: SubscribeRunRequest ──────────────────────────────────────────
	firstMsg, err := grpcStream.Recv()
	if err != nil {
		return err
	}
	subReq := firstMsg.GetSubscribe()
	if subReq == nil {
		return status.Error(codes.InvalidArgument, "first message must be SubscribeRunRequest")
	}
	runID := subReq.GetRunId().GetValue()
	threadID := subReq.GetThreadId().GetValue()

	runUUID, err := uuid.Parse(runID)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid run_id")
	}

	streamCh := make(chan lsdstream.Entry, 256)
	bufCtx, bufCancel := context.WithCancel(ctx)
	defer bufCancel()

	replayBatch := int64(100)
	if s.cfg != nil && s.cfg.StreamReplayBatch > 0 {
		replayBatch = s.cfg.StreamReplayBatch
	}
	blockMs := 500
	if s.cfg != nil && s.cfg.StreamReadBlockMs > 0 {
		blockMs = s.cfg.StreamReadBlockMs
	}

	go func() {
		defer close(streamCh)
		cursor := "0-0"
		for {
			select {
			case <-bufCtx.Done():
				return
			default:
			}
			entries, err := s.streamer.XReadFrom(bufCtx, lsdstream.RunStreamKey(runUUID), cursor, replayBatch, blockMs)
			if err != nil {
				return
			}
			for _, e := range entries {
				cursor = e.ID
				select {
				case streamCh <- e:
				case <-bufCtx.Done():
					return
				}
			}
		}
	}()

	termSub, err := s.streamer.Subscribe(ctx, lsdstream.RunTerminalChannel(runUUID))
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("subscribe terminal: %v", err))
	}
	defer termSub.Close() //nolint:errcheck

	if err := grpcStream.Send(&coreapi.StreamEvent{
		EventType: "control",
		Message:   []byte("subscribed"),
	}); err != nil {
		return err
	}

	// ── Phase 2: JoinRunRequest ───────────────────────────────────────────────
	secondMsg, err := grpcStream.Recv()
	if err != nil {
		return err
	}
	joinReq := secondMsg.GetJoin()
	if joinReq == nil {
		return status.Error(codes.InvalidArgument, "second message must be JoinRunRequest")
	}

	if err := s.store.PublishExistsAndAuth(ctx, runID, threadID, joinReq.GetFilters()); err != nil {
		switch {
		case errors.Is(err, ErrForbidden):
			// ops.py always surfaces 404 for auth-filter exclusion (ops.py:2018, 2280).
			return status.Error(codes.NotFound, err.Error())
		case errors.Is(err, ErrNotFound):
			if !joinReq.GetIgnoreRunNotFound() {
				return status.Error(codes.NotFound, err.Error())
			}
			return nil
		default:
			return status.Error(codes.Internal, err.Error())
		}
	}

	lastEventID := ""
	if joinReq.LastEventId != nil {
		lastEventID = *joinReq.LastEventId
	}

	cancelOnDisconnect := joinReq.GetCancelOnDisconnect()

	for {
		select {
		case <-ctx.Done():
			if cancelOnDisconnect {
				// Use a fresh context — ctx is already cancelled.
				cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, _ = s.store.Cancel(cctx, []string{runID}, threadID, nil)
				ccancel()
			}
			return ctx.Err()

		case _, ok := <-termSub.Channel():
			if !ok {
				return nil
			}
			_ = grpcStream.Send(&coreapi.StreamEvent{
				EventType: "control",
				Message:   []byte("done"),
			})
			return nil

		case entry, ok := <-streamCh:
			if !ok {
				return nil
			}
			if lastEventID != "" && entry.ID <= lastEventID {
				continue
			}
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

// parseControlSignal decodes a pub/sub payload into a ControlSignal enum value.
//
// Accepts two formats for compatibility:
//  1. JSON:        {"signal":"interrupt"} or {"signal":"rollback"} — Go's own format
//  2. Plain string: "interrupt" or "rollback" — Python's direct-Redis format
//     (Python ops.py:2437 treats the payload as a plain string; ops.py:1837
//     publishes the action string directly via coredis PUBLISH).
//
// Unknown or malformed payloads return the zero value (ControlSignal_unknown).
func parseControlSignal(payload string) enumcs.ControlSignal {
	// Try JSON {"signal":"..."} first (Go producer format).
	var msg struct {
		Signal string `json:"signal"`
	}
	if err := json.Unmarshal([]byte(payload), &msg); err == nil && msg.Signal != "" {
		if v, ok := enumcs.ControlSignal_value[msg.Signal]; ok {
			return enumcs.ControlSignal(v)
		}
	}
	// Fall back to plain-string (Python producer format: ops.py:2437).
	if v, ok := enumcs.ControlSignal_value[payload]; ok {
		return enumcs.ControlSignal(v)
	}
	return enumcs.ControlSignal(0)
}
