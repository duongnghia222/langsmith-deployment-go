package runs_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	enumca "github.com/duongnghia222/langsmith-deployment-go/gen/enum_cancel_run_action"
	enumms "github.com/duongnghia222/langsmith-deployment-go/gen/enum_multitask_strategy"
	enumrs "github.com/duongnghia222/langsmith-deployment-go/gen/enum_run_status"
	enumsm "github.com/duongnghia222/langsmith-deployment-go/gen/enum_stream_mode"
	"github.com/duongnghia222/langsmith-deployment-go/internal/config"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/runs"
	"github.com/duongnghia222/langsmith-deployment-go/internal/server"
	lsdstream "github.com/duongnghia222/langsmith-deployment-go/internal/stream"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// newTestService returns a runs.Service backed by a fresh Postgres container.
// rdb may be nil for tests that do not exercise PoolStats.
func newTestService(t *testing.T, ctx context.Context, rdb *goredis.Client) (*runs.Service, *pgxpool.Pool) {
	t.Helper()
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Provide a no-op redis client if one was not supplied (pool stats test only).
	if rdb == nil {
		rdb = goredis.NewClient(&goredis.Options{Addr: "localhost:0"})
	}
	return runs.NewService(pool, rdb), pool
}

// startRedis spins up a Redis testcontainer and returns a connected client.
func startRedis(t *testing.T, ctx context.Context) *goredis.Client {
	t.Helper()
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis connection string: %v", err)
	}
	opts, err := goredis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("redis parse url: %v", err)
	}
	return goredis.NewClient(opts)
}

func TestService_Get_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx, nil)
	_, err := svc.Get(ctx, &coreapi.GetRunRequest{
		RunId: &coreapi.UUID{Value: "00000000-0000-0000-0000-000000000000"},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("err code = %s, want NotFound", status.Code(err))
	}
}

func TestService_Get_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "g", nil)
	thID := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	r, err := svc.Get(ctx, &coreapi.GetRunRequest{
		RunId: &coreapi.UUID{Value: runID},
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if r.GetRunId().GetValue() != runID {
		t.Errorf("RunId = %q, want %q", r.GetRunId().GetValue(), runID)
	}
	if r.GetThreadId().GetValue() != thID {
		t.Errorf("ThreadId = %q, want %q", r.GetThreadId().GetValue(), thID)
	}
}

func TestService_Search_HappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "g2", nil)
	thID := "dddddddd-dddd-dddd-dddd-dddddddddddd"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	resp, err := svc.Search(ctx, &coreapi.SearchRunsRequest{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(resp.GetRuns()) != 2 {
		t.Errorf("len(runs) = %d, want 2", len(resp.GetRuns()))
	}
}

func TestService_Count_HappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "g3", nil)
	thID := "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	resp, err := svc.Count(ctx, &coreapi.CountRunsRequest{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if resp.GetCount() != 1 {
		t.Errorf("Count = %d, want 1", resp.GetCount())
	}
}

func TestService_Stats_HappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "g4", nil)
	thID := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	resp, err := svc.Stats(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if resp.GetNPending() != 1 {
		t.Errorf("NPending = %d, want 1", resp.GetNPending())
	}
	if resp.GetNRunning() != 1 {
		t.Errorf("NRunning = %d, want 1", resp.GetNRunning())
	}
}

func TestService_Stats_ReturnsPercentiles(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "g4-pct", nil)
	thID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)

	// Insert a pending run and backdate created_at so wait time is measurable.
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	if _, err := pool.Exec(ctx,
		`UPDATE run SET created_at = now() - interval '5 seconds' WHERE run_id = $1::uuid`,
		rID,
	); err != nil {
		t.Fatalf("backdate created_at: %v", err)
	}

	resp, err := svc.Stats(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if resp.PendingRunsWaitTimeMaxSecs == nil {
		t.Error("PendingRunsWaitTimeMaxSecs is nil, want non-nil")
	}
	if resp.PendingRunsWaitTimeMedSecs == nil {
		t.Error("PendingRunsWaitTimeMedSecs is nil, want non-nil")
	}
}

func TestService_PoolStats_HappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	svc, _ := newTestService(t, ctx, rdb)

	resp, err := svc.PoolStats(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("PoolStats: %v", err)
	}
	if resp.GetPostgres() == nil {
		t.Error("ConnectionPoolStats.Postgres is nil")
	}
	if resp.GetRedis() == nil {
		t.Error("ConnectionPoolStats.Redis is nil")
	}
	// MaxConns for pgxpool defaults to max(4, NumCPU); just verify it is positive.
	if resp.GetPostgres().GetPoolMax() <= 0 {
		t.Errorf("Postgres.PoolMax = %d, want > 0", resp.GetPostgres().GetPoolMax())
	}
}

func TestRunService_Create_Reject(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)
	thID := "22222222-2222-2222-2222-222222222222"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	rejectStrategy := enumms.MultitaskStrategy_reject
	_, err := svc.Create(ctx, &coreapi.CreateRunRequest{
		ThreadId:          &coreapi.UUID{Value: thID},
		AssistantId:       &coreapi.UUID{Value: aID},
		MultitaskStrategy: &rejectStrategy,
		KwargsJson:        []byte(`{}`),
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("want AlreadyExists, got %s: %v", status.Code(err), err)
	}
}

func TestRunService_Create_Enqueue_TwoRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)
	thID := "33333333-3333-3333-3333-333333333333"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)

	enqueueStrategy := enumms.MultitaskStrategy_enqueue
	r1, err := svc.Create(ctx, &coreapi.CreateRunRequest{
		ThreadId:          &coreapi.UUID{Value: thID},
		AssistantId:       &coreapi.UUID{Value: aID},
		MultitaskStrategy: &enqueueStrategy,
		KwargsJson:        []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("Create #1: %v", err)
	}
	r2, err := svc.Create(ctx, &coreapi.CreateRunRequest{
		ThreadId:          &coreapi.UUID{Value: thID},
		AssistantId:       &coreapi.UUID{Value: aID},
		MultitaskStrategy: &enqueueStrategy,
		KwargsJson:        []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("Create #2: %v", err)
	}
	id1 := r1.GetRuns()[0].GetRunId().GetValue()
	id2 := r2.GetRuns()[0].GetRunId().GetValue()
	if id1 == id2 {
		t.Fatal("two enqueue creates returned same run ID")
	}
}

// TestRunService_Create_Interrupt_SignalsExistingRuns verifies 2d: store.Create
// only reports which runs were displaced by the multitask strategy — it is
// service.Create's job to apply exactly what Cancel would: a displaced pending
// run transitions to 'interrupted', and a displaced running run gets the same
// control-channel PUBLISH a Cancel RPC would send (so the worker actually
// wakes up, not just a DB row that nobody is listening for).
func TestRunService_Create_Interrupt_SignalsExistingRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-create-interrupt", nil)
	thID := "44444444-4444-4444-4444-444444444444"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	pendingID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	runningID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	runningUUID := uuid.MustParse(runningID)
	controlCh := lsdstream.RunControlChannel(runningUUID)
	sub := rdb.Subscribe(ctx, controlCh)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe control: %v", err)
	}
	msgCh := sub.Channel()

	interruptStrategy := enumms.MultitaskStrategy_interrupt
	resp, err := svc.Create(ctx, &coreapi.CreateRunRequest{
		ThreadId:          &coreapi.UUID{Value: thID},
		AssistantId:       &coreapi.UUID{Value: aID},
		MultitaskStrategy: &interruptStrategy,
		KwargsJson:        []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("Create (interrupt): %v", err)
	}
	if resp.GetRuns()[0].GetRunId().GetValue() == "" {
		t.Fatal("new run ID empty")
	}

	// The pending run must have transitioned to 'interrupted'.
	var pendingStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM run WHERE run_id = $1::uuid`, pendingID).Scan(&pendingStatus); err != nil {
		t.Fatalf("select pending status: %v", err)
	}
	if pendingStatus != "interrupted" {
		t.Errorf("pending run status = %q, want interrupted", pendingStatus)
	}

	// The running run must receive the control signal (worker wakes up).
	select {
	case msg := <-msgCh:
		if msg.Payload == "" {
			t.Error("received empty payload on running run's control channel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for control signal on displaced running run")
	}
}

func TestService_KwargsRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g-kw", nil)
	thID := "99999999-9999-9999-9999-999999999999"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)

	kjs := []byte(`{"config":{"configurable":{"custom":"val"}},"stream_mode":["values","custom"],"durability":"async","webhook":"https://example.invalid/hook","temporary":true,"feedback_keys":["k1","k2"]}`)

	resp, err := svc.Create(ctx, &coreapi.CreateRunRequest{
		AssistantId: &coreapi.UUID{Value: aID},
		ThreadId:    &coreapi.UUID{Value: thID},
		KwargsJson:  kjs,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(resp.GetRuns()) == 0 {
		t.Fatal("no runs returned")
	}
	got := resp.GetRuns()[0]
	assertKwargsRoundTrip(t, aID, thID, got.GetRunId().GetValue(), got.GetKwargsJson())

	gotGet, err := svc.Get(ctx, &coreapi.GetRunRequest{
		RunId:    got.GetRunId(),
		ThreadId: got.GetThreadId(),
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertKwargsRoundTrip(t, aID, thID, got.GetRunId().GetValue(), gotGet.GetKwargsJson())
}

// assertKwargsRoundTrip checks that user-supplied kwargs are preserved and that
// the configurable was enriched with the assistant's graph_id, run_id, thread_id,
// and assistant_id (matching the Python reference implementation).
func assertKwargsRoundTrip(t *testing.T, aID, thID, runID string, kw []byte) {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(kw, &got); err != nil {
		t.Fatalf("unmarshal kwargs: %v", err)
	}
	for k, want := range map[string]any{
		"durability": "async",
		"webhook":    "https://example.invalid/hook",
		"temporary":  true,
	} {
		if got[k] != want {
			t.Errorf("kwargs[%q] = %v, want %v", k, got[k], want)
		}
	}
	cfg, _ := got["config"].(map[string]any)
	conf, _ := cfg["configurable"].(map[string]any)
	if conf["custom"] != "val" {
		t.Errorf("configurable.custom = %v, want val", conf["custom"])
	}
	if conf["graph_id"] != "g-kw" {
		t.Errorf("configurable.graph_id = %v, want g-kw", conf["graph_id"])
	}
	if conf["run_id"] != runID {
		t.Errorf("configurable.run_id = %v, want %s", conf["run_id"], runID)
	}
	if conf["thread_id"] != thID {
		t.Errorf("configurable.thread_id = %v, want %s", conf["thread_id"], thID)
	}
	if conf["assistant_id"] != aID {
		t.Errorf("configurable.assistant_id = %v, want %s", conf["assistant_id"], aID)
	}
}

// jsonEqual compares two JSON byte slices for semantic equality (ignoring
// key order and whitespace), which is necessary since PostgreSQL's JSONB
// storage reformats the input on read.
func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("unmarshal a: %v", err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("unmarshal b: %v", err)
	}
	return reflect.DeepEqual(av, bv)
}

func TestRunService_Delete_ReturnsID(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)
	thID := "44444444-4444-4444-4444-444444444444"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	resp, err := svc.Delete(ctx, &coreapi.DeleteRunRequest{
		RunId: &coreapi.UUID{Value: rID},
	})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if resp.GetValue() != rID {
		t.Errorf("deleted ID = %q, want %q", resp.GetValue(), rID)
	}
}

func TestRunService_Cancel_RunIds(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)
	thID := "55555555-5555-5555-5555-555555555555"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	_, err := svc.Cancel(ctx, &coreapi.CancelRunRequest{
		Target: &coreapi.CancelRunRequest_RunIds{
			RunIds: &coreapi.CancelRunIdsTarget{
				RunIds: []*coreapi.UUID{{Value: rID}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}

// TestRunService_Cancel_RunIds_NotFoundOnUnmatched asserts 2f-i: Cancel must
// raise NotFound unless every requested run_id matched a pending/running row.
// One of the two IDs here is already terminal ("success"), so it can never
// match — the whole call must fail even though the other ID is cancellable.
func TestRunService_Cancel_RunIds_NotFoundOnUnmatched(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g-cancel-notfound", nil)
	thID := "5b5b5b5b-5b5b-5b5b-5b5b-5b5b5b5b5b5b"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	pendingID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	terminalID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "success")

	_, err := svc.Cancel(ctx, &coreapi.CancelRunRequest{
		Target: &coreapi.CancelRunRequest_RunIds{
			RunIds: &coreapi.CancelRunIdsTarget{
				RunIds: []*coreapi.UUID{{Value: pendingID}, {Value: terminalID}},
			},
		},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("err code = %s, want NotFound", status.Code(err))
	}
}

func TestRunService_Cancel_StatusTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g-status-cancel", nil)
	thID := "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	pendingID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	runningID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")
	successID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "success")

	cancelReqAt := func(t *testing.T, runID string) *time.Time {
		t.Helper()
		var ts *time.Time
		if err := pool.QueryRow(ctx,
			`SELECT cancel_requested_at FROM run WHERE run_id = $1::uuid`, runID,
		).Scan(&ts); err != nil {
			t.Fatalf("select cancel_requested_at: %v", err)
		}
		return ts
	}

	// PENDING target: only the pending run should be cancelled.
	if _, err := svc.Cancel(ctx, &coreapi.CancelRunRequest{
		Target: &coreapi.CancelRunRequest_Status{
			Status: &coreapi.CancelStatusTarget{
				Status: coreapi.CancelRunStatus_CANCEL_RUN_STATUS_PENDING,
			},
		},
	}); err != nil {
		t.Fatalf("Cancel pending: %v", err)
	}
	if cancelReqAt(t, pendingID) == nil {
		t.Errorf("pending run was not cancelled")
	}
	if cancelReqAt(t, runningID) != nil {
		t.Errorf("running run should not be cancelled by PENDING target")
	}
	if cancelReqAt(t, successID) != nil {
		t.Errorf("success run should never be cancelled")
	}

	// ALL target: running run picks up the cancel; pending stays as already-cancelled.
	if _, err := svc.Cancel(ctx, &coreapi.CancelRunRequest{
		Target: &coreapi.CancelRunRequest_Status{
			Status: &coreapi.CancelStatusTarget{
				Status: coreapi.CancelRunStatus_CANCEL_RUN_STATUS_ALL,
			},
		},
	}); err != nil {
		t.Fatalf("Cancel all: %v", err)
	}
	if cancelReqAt(t, runningID) == nil {
		t.Errorf("running run was not cancelled by ALL target")
	}
	if cancelReqAt(t, successID) != nil {
		t.Errorf("success run should still not be cancelled")
	}
}

func TestRunService_Cancel_NeitherTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx, nil)
	_, err := svc.Cancel(ctx, &coreapi.CancelRunRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("err code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestRunService_SetStatus_Transitions(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)
	thID := "66666666-6666-6666-6666-666666666666"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	_, err := svc.SetStatus(ctx, &coreapi.SetRunStatusRequest{
		RunId:  &coreapi.UUID{Value: rID},
		Status: enumrs.RunStatus_running,
	})
	if err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
}

func TestPublish_AppendsToRedisStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Start Redis and Postgres containers.
	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Seed data.
	aID := testdb.MustInsertAssistant(t, ctx, pool, "pub-test", nil)
	thID := "77777777-7777-7777-7777-777777777777"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	// Build service with streamer.
	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000, StreamReadBlockMs: 500, StreamReplayBatch: 100}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	// Start an in-process gRPC server.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Call Publish.
	client := coreapi.NewRunsClient(conn)
	_, err = client.Publish(ctx, &coreapi.PublishStreamEventRequest{
		RunId:     &coreapi.UUID{Value: runID},
		ThreadId:  &coreapi.UUID{Value: thID},
		EventType: "values",
		Message:   []byte(`{"key":"value"}`),
		Resumable: false,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Assert run stream has 1 entry with correct event_type.
	runStreamKey := lsdstream.RunStreamKey(uuid.MustParse(runID))
	runEntries, err := streamer.XReadFrom(ctx, runStreamKey, "0-0", 10, 0)
	if err != nil {
		t.Fatalf("XReadFrom run stream: %v", err)
	}
	if len(runEntries) != 1 {
		t.Fatalf("run stream len = %d, want 1", len(runEntries))
	}
	if got := runEntries[0].Fields["event_type"]; got != "values" {
		t.Errorf("run stream event_type = %q, want %q", got, "values")
	}

	// Assert thread stream has 1 entry.
	threadStreamKey := lsdstream.ThreadStreamKey(uuid.MustParse(thID))
	threadEntries, err := streamer.XReadFrom(ctx, threadStreamKey, "0-0", 10, 0)
	if err != nil {
		t.Fatalf("XReadFrom thread stream: %v", err)
	}
	if len(threadEntries) != 1 {
		t.Fatalf("thread stream len = %d, want 1", len(threadEntries))
	}
}

// TestPublish_SucceedsOnMissingRun asserts 2j-ii: by the time an event is
// published the run row may already be gone (e.g. rolled back); Publish must
// swallow that and return success rather than surfacing NotFound.
func TestPublish_SucceedsOnMissingRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
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

	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	// Both IDs are valid UUIDs but the row doesn't exist.
	missingRunID := "11111111-1111-1111-1111-111111111111"
	missingThreadID := "22222222-2222-2222-2222-222222222222"
	_, err = coreapi.NewRunsClient(conn).Publish(ctx, &coreapi.PublishStreamEventRequest{
		RunId:     &coreapi.UUID{Value: missingRunID},
		ThreadId:  &coreapi.UUID{Value: missingThreadID},
		EventType: "values",
		Message:   []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("Publish on missing run: expected success (2j-ii), got %v", err)
	}
}

// TestPublish_ThreadLevelOnly asserts 2j-i: an absent (or "*") run_id skips
// run validation entirely and writes only to the thread-level stream — no
// run row needs to exist at all.
func TestPublish_ThreadLevelOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
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

	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	threadID := "33333333-3333-3333-3333-333333333333"
	_, err = coreapi.NewRunsClient(conn).Publish(ctx, &coreapi.PublishStreamEventRequest{
		ThreadId:  &coreapi.UUID{Value: threadID},
		EventType: "values",
		Message:   []byte(`{"key":"value"}`),
	})
	if err != nil {
		t.Fatalf("Publish (thread-level only): %v", err)
	}

	threadStreamKey := lsdstream.ThreadStreamKey(uuid.MustParse(threadID))
	entries, err := streamer.XReadFrom(ctx, threadStreamKey, "0-0", 10, 0)
	if err != nil {
		t.Fatalf("XReadFrom thread stream: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("thread stream len = %d, want 1", len(entries))
	}
}

func TestPublish_UnavailableWhenStreamerNil(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
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

	rdb := startRedis(t, ctx)

	// Use the non-stream constructor — streamer is nil.
	svc := runs.NewService(pool, rdb)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	_, err = coreapi.NewRunsClient(conn).Publish(ctx, &coreapi.PublishStreamEventRequest{
		RunId:     &coreapi.UUID{Value: "11111111-1111-1111-1111-111111111111"},
		ThreadId:  &coreapi.UUID{Value: "22222222-2222-2222-2222-222222222222"},
		EventType: "values",
		Message:   []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected Unavailable, got nil")
	}
	if status.Code(err) != codes.Unavailable {
		t.Errorf("expected codes.Unavailable, got %v: %v", status.Code(err), err)
	}
}

func TestEnter_UnavailableWhenStreamerNil(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	// NewService (NOT NewServiceWithStream) — streamer stays nil.
	svc := runs.NewService(pool, nil)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	enterStream, err := coreapi.NewRunsClient(conn).Enter(ctx, &coreapi.EnterRunRequest{
		RunId:    &coreapi.UUID{Value: "11111111-1111-1111-1111-111111111111"},
		ThreadId: &coreapi.UUID{Value: "22222222-2222-2222-2222-222222222222"},
	})
	if err != nil {
		t.Fatalf("Enter: %v", err)
	}

	_, err = enterStream.Recv()
	if err == nil {
		t.Fatal("expected error from Recv, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected status error, got %v", err)
	}
	if st.Code() != codes.Unavailable {
		t.Errorf("status code = %v, want Unavailable", st.Code())
	}
}

func TestEnter_ReceivesControlEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "graph-enter", nil)
	threadID := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")

	cfg := &config.Config{
		StreamMaxLen:      1000,
		StreamReadBlockMs: 500,
		HeartbeatInterval: 30 * time.Second, // long to avoid heartbeat noise during the test
	}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	enterCtx, enterCancel := context.WithTimeout(ctx, 5*time.Second)
	defer enterCancel()

	enterStream, err := coreapi.NewRunsClient(conn).Enter(enterCtx, &coreapi.EnterRunRequest{
		RunId:    &coreapi.UUID{Value: runID},
		ThreadId: &coreapi.UUID{Value: threadID},
	})
	if err != nil {
		t.Fatalf("Enter: %v", err)
	}

	// Publish an interrupt signal on the control channel from a side goroutine.
	controlChannel := lsdstream.RunControlChannel(uuid.MustParse(runID))
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = streamer.Publish(ctx, controlChannel, []byte(`{"signal":"interrupt"}`))
	}()

	evt, err := enterStream.Recv()
	if err != nil {
		t.Fatalf("enterStream.Recv: %v", err)
	}
	if evt == nil {
		t.Fatal("received nil ControlEvent")
	}
	if evt.GetAction().String() != "interrupt" {
		t.Errorf("ControlEvent.Action = %q, want %q", evt.GetAction().String(), "interrupt")
	}
}

func TestStream_ReceivesPublishedEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "stream-recv", nil)
	threadID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")

	cfg := &config.Config{
		StreamMaxLen:      1000,
		StreamReadBlockMs: 500,
		StreamReplayBatch: 100,
	}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	streamCtx, streamCancel := context.WithTimeout(ctx, 15*time.Second)
	defer streamCancel()

	bidiStream, err := coreapi.NewRunsClient(conn).Stream(streamCtx)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// Phase 1: send Subscribe.
	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Subscribe{
			Subscribe: &coreapi.SubscribeRunRequest{
				RunId:    &coreapi.UUID{Value: runID},
				ThreadId: &coreapi.UUID{Value: threadID},
			},
		},
	}); err != nil {
		t.Fatalf("Send Subscribe: %v", err)
	}

	// Expect "subscribed" control event.
	confirmEvt, err := bidiStream.Recv()
	if err != nil {
		t.Fatalf("Recv subscribed: %v", err)
	}
	if confirmEvt.GetEventType() != "control" {
		t.Errorf("first event EventType = %q, want %q", confirmEvt.GetEventType(), "control")
	}
	if string(confirmEvt.GetMessage()) != "subscribed" {
		t.Errorf("first event Message = %q, want %q", string(confirmEvt.GetMessage()), "subscribed")
	}

	// Phase 2: send Join.
	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Join{
			Join: &coreapi.JoinRunRequest{},
		},
	}); err != nil {
		t.Fatalf("Send Join: %v", err)
	}

	runUUID := uuid.MustParse(runID)

	// Side goroutine: XADD a "values" event and then publish terminal.
	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = streamer.XAdd(ctx, lsdstream.RunStreamKey(runUUID), map[string]any{
			"event_type": "values",
			"message":    `{"key":"val"}`,
		}, 1000)
		time.Sleep(200 * time.Millisecond)
		_ = streamer.Publish(ctx, lsdstream.RunTerminalChannel(runUUID), []byte("done"))
	}()

	// Collect events until "done" control event.
	var valuesEvt *coreapi.StreamEvent
	for {
		ev, err := bidiStream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.GetEventType() == "control" && string(ev.GetMessage()) == "done" {
			break
		}
		if ev.GetEventType() == "values" {
			valuesEvt = ev
		}
	}

	if valuesEvt == nil {
		t.Fatal("never received a values event")
	}
	if valuesEvt.GetStreamId() == "" {
		t.Error("values event StreamId is empty, want non-empty")
	}
}

func TestStream_CancelOnDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "stream-cancel", nil)
	threadID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")

	cfg := &config.Config{
		StreamMaxLen:      1000,
		StreamReadBlockMs: 500,
		StreamReplayBatch: 100,
	}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	streamCtx, streamCancel := context.WithCancel(ctx)

	bidiStream, err := coreapi.NewRunsClient(conn).Stream(streamCtx)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// Phase 1: Subscribe.
	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Subscribe{
			Subscribe: &coreapi.SubscribeRunRequest{
				RunId:    &coreapi.UUID{Value: runID},
				ThreadId: &coreapi.UUID{Value: threadID},
			},
		},
	}); err != nil {
		t.Fatalf("Send Subscribe: %v", err)
	}

	// Drain the subscribed control event.
	if _, err := bidiStream.Recv(); err != nil {
		t.Fatalf("Recv subscribed: %v", err)
	}

	// Phase 2: Join with cancel_on_disconnect=true.
	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Join{
			Join: &coreapi.JoinRunRequest{
				CancelOnDisconnect: true,
			},
		},
	}); err != nil {
		t.Fatalf("Send Join: %v", err)
	}

	// Give the handler time to enter its main select loop, then cancel the context.
	time.Sleep(300 * time.Millisecond)
	streamCancel()

	// Wait a moment for the cancel-on-disconnect path to run.
	time.Sleep(1 * time.Second)

	// Assert cancel_requested_at is non-NULL in the database.
	var cancelRequestedAt *time.Time
	err = pool.QueryRow(ctx,
		`SELECT cancel_requested_at FROM run WHERE run_id = $1::uuid`, runID,
	).Scan(&cancelRequestedAt)
	if err != nil {
		t.Fatalf("QueryRow cancel_requested_at: %v", err)
	}
	if cancelRequestedAt == nil {
		t.Error("cancel_requested_at is NULL, want non-NULL after CancelOnDisconnect")
	}
}

// TestStream_CancelOnDisconnect_PublishesSignal verifies 2g: disconnecting
// with cancel_on_disconnect must go through the same signal-publishing path
// as the Cancel RPC (control channel PUBLISH), not just update the DB — a
// bare store.Cancel call never wakes a running worker.
func TestStream_CancelOnDisconnect_PublishesSignal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "stream-cancel-signal", nil)
	threadID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbc"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")

	cfg := &config.Config{
		StreamMaxLen:      1000,
		StreamReadBlockMs: 500,
		StreamReplayBatch: 100,
	}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	runUUID := uuid.MustParse(runID)
	controlCh := lsdstream.RunControlChannel(runUUID)
	sub := rdb.Subscribe(ctx, controlCh)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe control: %v", err)
	}
	msgCh := sub.Channel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	streamCtx, streamCancel := context.WithCancel(ctx)

	bidiStream, err := coreapi.NewRunsClient(conn).Stream(streamCtx)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Subscribe{
			Subscribe: &coreapi.SubscribeRunRequest{
				RunId:    &coreapi.UUID{Value: runID},
				ThreadId: &coreapi.UUID{Value: threadID},
			},
		},
	}); err != nil {
		t.Fatalf("Send Subscribe: %v", err)
	}
	if _, err := bidiStream.Recv(); err != nil {
		t.Fatalf("Recv subscribed: %v", err)
	}

	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Join{
			Join: &coreapi.JoinRunRequest{CancelOnDisconnect: true},
		},
	}); err != nil {
		t.Fatalf("Send Join: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	streamCancel()

	select {
	case msg := <-msgCh:
		if msg.Payload == "" {
			t.Error("received empty payload on control channel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for control channel signal after disconnect")
	}
}

func TestStream_LastEventIDResume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "stream-resume", nil)
	threadID := "cccccccc-dddd-dddd-dddd-cccccccccccc"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")

	cfg := &config.Config{
		StreamMaxLen:      1000,
		StreamReadBlockMs: 500,
		StreamReplayBatch: 100,
	}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	runUUID := uuid.MustParse(runID)

	// Pre-populate two entries.
	firstID, err := streamer.XAdd(ctx, lsdstream.RunStreamKey(runUUID), map[string]any{
		"event_type": "metadata",
		"message":    `{"seq":1}`,
	}, 1000)
	if err != nil {
		t.Fatalf("XAdd entry 1: %v", err)
	}
	_, err = streamer.XAdd(ctx, lsdstream.RunStreamKey(runUUID), map[string]any{
		"event_type": "updates",
		"message":    `{"seq":2}`,
	}, 1000)
	if err != nil {
		t.Fatalf("XAdd entry 2: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	streamCtx, streamCancel := context.WithTimeout(ctx, 15*time.Second)
	defer streamCancel()

	bidiStream, err := coreapi.NewRunsClient(conn).Stream(streamCtx)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// Phase 1: Subscribe.
	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Subscribe{
			Subscribe: &coreapi.SubscribeRunRequest{
				RunId:    &coreapi.UUID{Value: runID},
				ThreadId: &coreapi.UUID{Value: threadID},
			},
		},
	}); err != nil {
		t.Fatalf("Send Subscribe: %v", err)
	}

	// Drain the subscribed control event.
	if _, err := bidiStream.Recv(); err != nil {
		t.Fatalf("Recv subscribed: %v", err)
	}

	// Phase 2: Join with LastEventId set to the first entry's ID.
	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Join{
			Join: &coreapi.JoinRunRequest{
				LastEventId: &firstID,
			},
		},
	}); err != nil {
		t.Fatalf("Send Join: %v", err)
	}

	// Side goroutine: publish terminal after a short delay.
	go func() {
		time.Sleep(800 * time.Millisecond)
		_ = streamer.Publish(ctx, lsdstream.RunTerminalChannel(runUUID), []byte("done"))
	}()

	// Collect events until "done" control.
	var receivedEventTypes []string
	for {
		ev, err := bidiStream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.GetEventType() == "control" && string(ev.GetMessage()) == "done" {
			break
		}
		receivedEventTypes = append(receivedEventTypes, ev.GetEventType())
	}

	// Should only have received the second event ("updates"), not the first ("metadata").
	if len(receivedEventTypes) != 1 {
		t.Errorf("received %d events, want 1; types: %v", len(receivedEventTypes), receivedEventTypes)
	} else if receivedEventTypes[0] != "updates" {
		t.Errorf("received event_type = %q, want %q", receivedEventTypes[0], "updates")
	}
}

// TestStream_JoinAlreadyTerminalRun_EndsPromptly verifies (2e): joining a run
// that is already in a terminal status must close the stream immediately via
// the pre-loop checkRunFinished/drainAndClose path, not wait out the 5s
// statusTicker or block forever on the control/event channels.
func TestStream_JoinAlreadyTerminalRun_EndsPromptly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "stream-already-done", nil)
	threadID := "dddddddd-eeee-eeee-eeee-dddddddddddd"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "success")

	cfg := &config.Config{
		StreamMaxLen:      1000,
		StreamReadBlockMs: 500,
		StreamReplayBatch: 100,
	}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	streamCtx, streamCancel := context.WithTimeout(ctx, 15*time.Second)
	defer streamCancel()

	bidiStream, err := coreapi.NewRunsClient(conn).Stream(streamCtx)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Subscribe{
			Subscribe: &coreapi.SubscribeRunRequest{
				RunId:    &coreapi.UUID{Value: runID},
				ThreadId: &coreapi.UUID{Value: threadID},
			},
		},
	}); err != nil {
		t.Fatalf("Send Subscribe: %v", err)
	}
	if _, err := bidiStream.Recv(); err != nil {
		t.Fatalf("Recv subscribed: %v", err)
	}

	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Join{
			Join: &coreapi.JoinRunRequest{},
		},
	}); err != nil {
		t.Fatalf("Send Join: %v", err)
	}

	start := time.Now()
	_, recvErr := bidiStream.Recv()
	elapsed := time.Since(start)

	if recvErr != io.EOF {
		t.Fatalf("Recv after join on terminal run: got err=%v, want io.EOF", recvErr)
	}
	if elapsed >= 3*time.Second {
		t.Errorf("stream took %v to close on an already-terminal run; want well under the 5s statusTicker", elapsed)
	}
}

// TestStream_HonorsStreamModes verifies (2h): JoinRunRequest.StreamModes must
// filter streamed entries server-side by event_type before sending — events
// published under a mode not in the requested set must never reach the client.
func TestStream_HonorsStreamModes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "stream-modes", nil)
	threadID := "eeeeeeee-ffff-ffff-ffff-eeeeeeeeeeee"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")
	runUUID := uuid.MustParse(runID)

	cfg := &config.Config{
		StreamMaxLen:      1000,
		StreamReadBlockMs: 500,
		StreamReplayBatch: 100,
	}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	streamCtx, streamCancel := context.WithTimeout(ctx, 15*time.Second)
	defer streamCancel()

	bidiStream, err := coreapi.NewRunsClient(conn).Stream(streamCtx)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Subscribe{
			Subscribe: &coreapi.SubscribeRunRequest{
				RunId:    &coreapi.UUID{Value: runID},
				ThreadId: &coreapi.UUID{Value: threadID},
			},
		},
	}); err != nil {
		t.Fatalf("Send Subscribe: %v", err)
	}
	if _, err := bidiStream.Recv(); err != nil {
		t.Fatalf("Recv subscribed: %v", err)
	}

	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Join{
			Join: &coreapi.JoinRunRequest{
				StreamModes: []enumsm.StreamMode{enumsm.StreamMode_updates},
			},
		},
	}); err != nil {
		t.Fatalf("Send Join: %v", err)
	}

	// Side goroutine: after Join's read-loop has started (cursor resolved at
	// "$"), publish one event of each mode, then the terminal control signal.
	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = streamer.XAdd(ctx, lsdstream.RunStreamKey(runUUID), map[string]any{
			"event_type": "values",
			"message":    `{"seq":1}`,
		}, 1000)
		_, _ = streamer.XAdd(ctx, lsdstream.RunStreamKey(runUUID), map[string]any{
			"event_type": "updates",
			"message":    `{"seq":2}`,
		}, 1000)
		time.Sleep(300 * time.Millisecond)
		_ = streamer.Publish(ctx, lsdstream.RunTerminalChannel(runUUID), []byte("done"))
	}()

	var receivedEventTypes []string
	for {
		ev, err := bidiStream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.GetEventType() == "control" && string(ev.GetMessage()) == "done" {
			break
		}
		receivedEventTypes = append(receivedEventTypes, ev.GetEventType())
	}

	if len(receivedEventTypes) != 1 {
		t.Fatalf("received %d events, want 1 (mode-filtered); types: %v", len(receivedEventTypes), receivedEventTypes)
	}
	if receivedEventTypes[0] != "updates" {
		t.Errorf("received event_type = %q, want %q", receivedEventTypes[0], "updates")
	}
}

// TestStream_JoinWithoutLastEventID_NoHistoryReplay is the plan-mandated
// regression test for (2i), and a fix-round-1 regression test for finding 2:
// publish 3 events, then subscribe+join WITHOUT last_event_id, then publish
// more events afterward — only the events published AFTER Join must arrive.
// The 3 pre-existing events are written and confirmed BEFORE Join is even
// sent, so if Join's initial cursor were resolved by handing the literal "$"
// sentinel into a repeatedly re-armed blocking XREAD (the old, buggy
// behavior), the fix under test — resolving the tail to a concrete ID ONCE
// via Streamer.LastID before the buffer goroutine starts — is what this test
// pins down: it proves no history leaks through and (by construction, since
// the post-join event is appended only after a real network round trip) no
// entry is skipped either.
func TestStream_JoinWithoutLastEventID_NoHistoryReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "stream-no-replay", nil)
	threadID := "ffffffff-0000-0000-0000-ffffffffffff"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")
	runUUID := uuid.MustParse(runID)

	cfg := &config.Config{
		StreamMaxLen:      1000,
		StreamReadBlockMs: 500,
		StreamReplayBatch: 100,
	}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	// Pre-existing history: 3 events written and confirmed BEFORE Join is
	// ever sent (Subscribe below has not even happened yet).
	for _, seq := range []string{`{"seq":1}`, `{"seq":2}`, `{"seq":3}`} {
		if _, err := streamer.XAdd(ctx, lsdstream.RunStreamKey(runUUID), map[string]any{
			"event_type": "updates",
			"message":    seq,
		}, 1000); err != nil {
			t.Fatalf("XAdd pre-join entry: %v", err)
		}
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	streamCtx, streamCancel := context.WithTimeout(ctx, 15*time.Second)
	defer streamCancel()

	bidiStream, err := coreapi.NewRunsClient(conn).Stream(streamCtx)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Subscribe{
			Subscribe: &coreapi.SubscribeRunRequest{
				RunId:    &coreapi.UUID{Value: runID},
				ThreadId: &coreapi.UUID{Value: threadID},
			},
		},
	}); err != nil {
		t.Fatalf("Send Subscribe: %v", err)
	}
	if _, err := bidiStream.Recv(); err != nil {
		t.Fatalf("Recv subscribed: %v", err)
	}

	// Join with no last_event_id: must NOT replay the 3 pre-existing events.
	if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
		Message: &coreapi.StreamRunClientMessage_Join{
			Join: &coreapi.JoinRunRequest{},
		},
	}); err != nil {
		t.Fatalf("Send Join: %v", err)
	}

	// Side goroutine: publish one new event after Join, then terminal.
	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = streamer.XAdd(ctx, lsdstream.RunStreamKey(runUUID), map[string]any{
			"event_type": "updates",
			"message":    `{"seq":4}`,
		}, 1000)
		time.Sleep(300 * time.Millisecond)
		_ = streamer.Publish(ctx, lsdstream.RunTerminalChannel(runUUID), []byte("done"))
	}()

	var receivedMessages []string
	for {
		ev, err := bidiStream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.GetEventType() == "control" && string(ev.GetMessage()) == "done" {
			break
		}
		receivedMessages = append(receivedMessages, string(ev.GetMessage()))
	}

	if len(receivedMessages) != 1 {
		t.Fatalf("received %d events, want 1 (only post-join); messages: %v", len(receivedMessages), receivedMessages)
	}
	if receivedMessages[0] != `{"seq":4}` {
		t.Errorf("received message = %q, want post-join seq 4 (pre-existing history must not replay)", receivedMessages[0])
	}
}

// ─── Parity gap tests (C6, C7, C8, C9) ──────────────────────────────────────

// TestCancel_PublishesControlSignal verifies C6 parity:
// Cancel must publish the control signal AND set the Redis STRING key with 60s TTL.
func TestCancel_PublishesControlSignal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-cancel-sig", nil)
	thID := "c1c1c1c1-c1c1-c1c1-c1c1-c1c1c1c1c1c1"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	runUUID := uuid.MustParse(runID)
	controlCh := lsdstream.RunControlChannel(runUUID)

	// Subscribe to the control channel BEFORE calling Cancel so we catch the PUBLISH.
	sub := rdb.Subscribe(ctx, controlCh)
	defer sub.Close()
	// consume subscription confirmation
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe receive: %v", err)
	}
	msgCh := sub.Channel()

	_, err = svc.Cancel(ctx, &coreapi.CancelRunRequest{
		Target: &coreapi.CancelRunRequest_RunIds{
			RunIds: &coreapi.CancelRunIdsTarget{
				ThreadId: &coreapi.UUID{Value: thID},
				RunIds:   []*coreapi.UUID{{Value: runID}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Verify PUBLISH arrived.
	select {
	case msg := <-msgCh:
		if msg.Payload == "" {
			t.Error("received empty payload on control channel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for control channel message")
	}

	// (C6) Verify the control STRING key was set with TTL ~60s.
	ttl := rdb.TTL(ctx, controlCh).Val()
	if ttl <= 0 || ttl > 60*time.Second {
		t.Errorf("control key TTL = %v, want (0, 60s]", ttl)
	}
}

// TestCancel_PublishesSignalForMatchedRun_EvenWhenBatchHasUnmatchedID is a
// fix-round-1 regression test (finding 1): a mixed Cancel run_ids batch — one
// live/matched run plus one bogus/already-terminal ID — must still publish
// the control signal for the matched run before the NotFound shortfall check
// runs, exactly like ops.py (ops.py:1834-1837 SETs+PUBLISHes for every
// requested run_id before the SQL runs; the raise at ops.py:1907 only aborts
// the caller's transaction, not the signal that already went out). The
// service must not silently swallow a live run's signal just because a
// sibling ID in the same request was bogus.
func TestCancel_PublishesSignalForMatchedRun_EvenWhenBatchHasUnmatchedID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	aID := testdb.MustInsertAssistant(t, ctx, pool, "cancel-mixed-batch", nil)
	thID := "d1d1d1d1-d1d1-d1d1-d1d1-d1d1d1d1d1d1"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runningID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")
	terminalID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "success")

	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	runUUID := uuid.MustParse(runningID)
	controlCh := lsdstream.RunControlChannel(runUUID)

	// Subscribe to the matched (still-live) run's control channel BEFORE
	// calling Cancel so we catch the PUBLISH even though the overall call
	// will fail with NotFound because of the sibling bogus/terminal ID.
	sub := rdb.Subscribe(ctx, controlCh)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe receive: %v", err)
	}
	msgCh := sub.Channel()

	_, err = svc.Cancel(ctx, &coreapi.CancelRunRequest{
		Target: &coreapi.CancelRunRequest_RunIds{
			RunIds: &coreapi.CancelRunIdsTarget{
				RunIds: []*coreapi.UUID{{Value: runningID}, {Value: terminalID}},
			},
		},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err code = %s, want NotFound", status.Code(err))
	}

	// The live run must still have been signalled despite the overall NotFound.
	select {
	case msg := <-msgCh:
		if msg.Payload == "" {
			t.Error("received empty payload on control channel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for control channel message; matched run was not signalled before the NotFound shortfall check")
	}
}

// TestCancel_RollbackDeletesPendingViaService verifies C2 parity via the service layer.
func TestCancel_RollbackDeletesPendingViaService(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-rollback", nil)
	thID := "c2c2c2c2-c2c2-c2c2-c2c2-c2c2c2c2c2c2"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	pendingID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	rollbackAction := enumca.CancelRunAction_rollback
	_, err = svc.Cancel(ctx, &coreapi.CancelRunRequest{
		Target: &coreapi.CancelRunRequest_RunIds{
			RunIds: &coreapi.CancelRunIdsTarget{
				ThreadId: &coreapi.UUID{Value: thID},
				RunIds:   []*coreapi.UUID{{Value: pendingID}},
			},
		},
		Action: &rollbackAction,
	})
	if err != nil {
		t.Fatalf("Cancel rollback: %v", err)
	}

	// Pending run must be gone (hard deleted).
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM run WHERE run_id = $1::uuid`, pendingID).Scan(&count); err != nil {
		t.Fatalf("count run: %v", err)
	}
	if count != 0 {
		t.Errorf("pending run count after rollback = %d, want 0 (deleted)", count)
	}
}

// TestMarkDone_PublishesTerminalDone verifies C7 parity:
// MarkDone must publish "done" to RunTerminalChannel (Python ops.py:1436).
func TestMarkDone_PublishesTerminalDone(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-markdone", nil)
	thID := "c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	runUUID := uuid.MustParse(runID)
	terminalCh := lsdstream.RunTerminalChannel(runUUID)

	// Subscribe before calling MarkDone.
	sub := rdb.Subscribe(ctx, terminalCh)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe receive: %v", err)
	}
	msgCh := sub.Channel()

	_, err = svc.MarkDone(ctx, &coreapi.MarkRunDoneRequest{
		RunId:     &coreapi.UUID{Value: runID},
		Resumable: false,
	})
	if err != nil {
		t.Fatalf("MarkDone: %v", err)
	}

	// (C7) Must receive "done" on the terminal channel.
	select {
	case msg := <-msgCh:
		if msg.Payload != "done" {
			t.Errorf("terminal channel payload = %q, want %q", msg.Payload, "done")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for terminal channel message")
	}
}

// TestMarkDone_DoesNotOverwriteTerminalStatus verifies 2a via the service
// layer: MarkDone must not set the run's status (regardless of Resumable),
// only release the lease. Whatever set the terminal status (SetStatus)
// remains the sole source of truth for it.
func TestMarkDone_DoesNotOverwriteTerminalStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx, nil)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-markdone-noclobber", nil)
	thID := "c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c4"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	if _, err := svc.SetStatus(ctx, &coreapi.SetRunStatusRequest{
		RunId:  &coreapi.UUID{Value: runID},
		Status: enumrs.RunStatus_error,
	}); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}

	if _, err := svc.MarkDone(ctx, &coreapi.MarkRunDoneRequest{
		RunId:     &coreapi.UUID{Value: runID},
		Resumable: true, // must have no bearing on status at all (2a)
	}); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}

	var statusText string
	if err := pool.QueryRow(ctx, `SELECT status FROM run WHERE run_id = $1::uuid`, runID).Scan(&statusText); err != nil {
		t.Fatalf("select status: %v", err)
	}
	if statusText != "error" {
		t.Errorf("status = %q, want error (MarkDone must not overwrite)", statusText)
	}
}

// TestSetStatus_TerminalPublishesTerminalDone verifies C7 parity:
// SetStatus to a terminal status must publish "done" to RunTerminalChannel.
func TestSetStatus_TerminalPublishesTerminalDone(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-setstatus", nil)
	thID := "c4c4c4c4-c4c4-c4c4-c4c4-c4c4c4c4c4c4"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	runUUID := uuid.MustParse(runID)
	terminalCh := lsdstream.RunTerminalChannel(runUUID)

	sub := rdb.Subscribe(ctx, terminalCh)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe receive: %v", err)
	}
	msgCh := sub.Channel()

	_, err = svc.SetStatus(ctx, &coreapi.SetRunStatusRequest{
		RunId:  &coreapi.UUID{Value: runID},
		Status: enumrs.RunStatus_success,
	})
	if err != nil {
		t.Fatalf("SetStatus(success): %v", err)
	}

	// (C7) Must receive "done" on the terminal channel.
	select {
	case msg := <-msgCh:
		if msg.Payload != "done" {
			t.Errorf("terminal channel payload = %q, want %q", msg.Payload, "done")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for terminal channel message after SetStatus(success)")
	}
}

// TestSetStatus_PendingWakesQueue verifies C6 parity:
// Python ops.py:1960-1961: wake_up_worker() when status set to 'pending'.
func TestSetStatus_PendingWakesQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-setstatus-pending", nil)
	thID := "c5c5c5c5-c5c5-c5c5-c5c5-c5c5c5c5c5c5"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	// Before call: queue should be empty.
	queueKey := lsdstream.RunQueueKey()
	if n := rdb.LLen(ctx, queueKey).Val(); n != 0 {
		t.Fatalf("queue length before SetStatus = %d, want 0", n)
	}

	_, err = svc.SetStatus(ctx, &coreapi.SetRunStatusRequest{
		RunId:  &coreapi.UUID{Value: runID},
		Status: enumrs.RunStatus_pending,
	})
	if err != nil {
		t.Fatalf("SetStatus(pending): %v", err)
	}

	// Queue must have one entry (C6).
	if n := rdb.LLen(ctx, queueKey).Val(); n != 1 {
		t.Errorf("queue length after SetStatus(pending) = %d, want 1", n)
	}
}

// TestSweep_WakesQueueAfterReset verifies C8 parity:
// After Sweep resets expired runs to 'pending', it must RPUSH to the run queue.
func TestSweep_WakesQueueAfterReset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-sweep-wake", nil)
	thID := "c6c6c6c6-c6c6-c6c6-c6c6-c6c6c6c6c6c6"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	// Expire the lease.
	if _, err := pool.Exec(ctx,
		`UPDATE run SET lease_expires_at = now() - interval '1 second' WHERE run_id = $1::uuid`, runID,
	); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{StreamMaxLen: 1000}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	queueKey := lsdstream.RunQueueKey()
	if n := rdb.LLen(ctx, queueKey).Val(); n != 0 {
		t.Fatalf("queue before sweep = %d, want 0", n)
	}

	resp, err := svc.Sweep(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(resp.GetRunIds()) != 1 {
		t.Fatalf("swept run count = %d, want 1", len(resp.GetRunIds()))
	}

	// (C8) Queue must have one entry pushed by Sweep (Python ops.py:1473).
	if n := rdb.LLen(ctx, queueKey).Val(); n != 1 {
		t.Errorf("queue after sweep = %d, want 1", n)
	}
}

// TestEnter_PreExistingCancelKey verifies C6 parity:
// Enter checks the Redis STRING key for a pre-existing cancel signal before
// subscribing, so late-starting workers see the cancel (Python ops.py:2432-2436).
func TestEnter_PreExistingCancelKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-precancel", nil)
	thID := "c7c7c7c7-c7c7-c7c7-c7c7-c7c7c7c7c7c7"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	runUUID := uuid.MustParse(runID)
	controlKey := lsdstream.RunControlChannel(runUUID)

	// Pre-set the control STRING key (simulating a Cancel that fired before Enter).
	// Use the plain-string format (Python's format) to verify parseControlSignal compat.
	if err := rdb.Set(ctx, controlKey, "interrupt", 60*time.Second).Err(); err != nil {
		t.Fatalf("SET control key: %v", err)
	}

	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{
		StreamMaxLen:      1000,
		HeartbeatInterval: 30 * time.Second,
	}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	enterCtx, enterCancel := context.WithTimeout(ctx, 10*time.Second)
	defer enterCancel()

	enterStream, err := coreapi.NewRunsClient(conn).Enter(enterCtx, &coreapi.EnterRunRequest{
		RunId:    &coreapi.UUID{Value: runID},
		ThreadId: &coreapi.UUID{Value: thID},
	})
	if err != nil {
		t.Fatalf("Enter: %v", err)
	}

	// Must immediately receive the pre-existing cancel signal.
	evt, err := enterStream.Recv()
	if err != nil {
		t.Fatalf("enterStream.Recv: %v", err)
	}
	if evt.GetAction().String() != "interrupt" {
		t.Errorf("ControlEvent.Action = %q, want interrupt (pre-existing key)", evt.GetAction().String())
	}
}

// TestEnter_PlainStringPayloadParsed verifies C6 parity:
// parseControlSignal must accept Python's plain-string format ("interrupt"/"rollback")
// in addition to Go's JSON format.
func TestEnter_PlainStringPayloadParsed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rdb := startRedis(t, ctx)
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-plainstr", nil)
	thID := "c8c8c8c8-c8c8-c8c8-c8c8-c8c8c8c8c8c8"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	streamer := lsdstream.NewStreamer(rdb)
	cfg := &config.Config{
		StreamMaxLen:      1000,
		HeartbeatInterval: 30 * time.Second,
	}
	svc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	enterCtx, enterCancel := context.WithTimeout(ctx, 10*time.Second)
	defer enterCancel()

	enterStream, err := coreapi.NewRunsClient(conn).Enter(enterCtx, &coreapi.EnterRunRequest{
		RunId:    &coreapi.UUID{Value: runID},
		ThreadId: &coreapi.UUID{Value: thID},
	})
	if err != nil {
		t.Fatalf("Enter: %v", err)
	}

	controlChannel := lsdstream.RunControlChannel(uuid.MustParse(runID))
	// Publish plain-string "rollback" (Python format, ops.py:2437).
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = rdb.Publish(ctx, controlChannel, "rollback").Err()
	}()

	evt, err := enterStream.Recv()
	if err != nil {
		t.Fatalf("enterStream.Recv: %v", err)
	}
	if evt.GetAction().String() != "rollback" {
		t.Errorf("ControlEvent.Action = %q, want rollback (plain-string Python format)", evt.GetAction().String())
	}
}
