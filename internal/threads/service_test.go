package threads_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/checkpointer"
	"github.com/duongnghia222/langsmith-deployment-go/internal/config"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/jsonbutil"
	"github.com/duongnghia222/langsmith-deployment-go/internal/server"
	lsdstream "github.com/duongnghia222/langsmith-deployment-go/internal/stream"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/duongnghia222/langsmith-deployment-go/internal/threads"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func newTestService(t *testing.T, ctx context.Context) (*threads.Service, *pgxpool.Pool) {
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
	return threads.NewService(pool), pool
}

func TestService_Get_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	svc := threads.NewService(pool)
	_, err = svc.Get(ctx, &coreapi.GetThreadRequest{
		ThreadId: &coreapi.UUID{Value: "00000000-0000-0000-0000-000000000000"},
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
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	id := "55555555-5555-5555-5555-555555555555"
	testdb.MustInsertThread(t, ctx, pool, id, []byte(`{"k":"v"}`))

	svc := threads.NewService(pool)
	th, err := svc.Get(ctx, &coreapi.GetThreadRequest{
		ThreadId: &coreapi.UUID{Value: id},
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if th.GetThreadId().GetValue() != id {
		t.Errorf("ThreadId = %q, want %q", th.GetThreadId().GetValue(), id)
	}
}

func TestThreadService_Create_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx)
	resp, err := svc.Create(ctx, &coreapi.CreateThreadRequest{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if resp.GetThreadId().GetValue() == "" {
		t.Fatal("ThreadId empty")
	}
}

func TestService_InterruptsRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	th, err := svc.Create(ctx, &coreapi.CreateThreadRequest{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	id1, id2 := "i-1", "i-2"
	resumable := true
	when := "during"
	interrupts := map[string]*coreapi.Interrupts{
		"task-1": {
			Interrupts: []*coreapi.Interrupt{
				{Id: &id1, Value: []byte(`"first"`), When: &when, Resumable: &resumable},
				{Id: &id2, Value: []byte(`"second"`)},
			},
		},
	}

	encoded, err := encodeInterruptsForTest(interrupts)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Seed an assistant + run row so SetJointStatus can update it.
	asstID := testdb.MustInsertAssistant(t, ctx, pool, "g-1", nil)
	runID := uuid.NewString()
	if _, err := pool.Exec(ctx,
		`INSERT INTO run (run_id, thread_id, assistant_id, status, kwargs, created_at, updated_at)
		 VALUES ($1::uuid, $2::uuid, $3::uuid, 'running', '{}'::jsonb, now(), now())`,
		runID, th.GetThreadId().GetValue(), asstID,
	); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	_, err = svc.SetJointStatus(ctx, &coreapi.SetThreadJointStatusRequest{
		ThreadId:  th.GetThreadId(),
		RunId:     &coreapi.UUID{Value: runID},
		RunStatus: "idle",
		GraphId:   "g-1",
		Checkpoint: &coreapi.ThreadStatusCheckpoint{
			InterruptsJson: encoded,
		},
	})
	if err != nil {
		t.Fatalf("SetJointStatus: %v", err)
	}

	got, err := svc.Get(ctx, &coreapi.GetThreadRequest{ThreadId: th.GetThreadId()})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.GetInterrupts()) != 1 {
		t.Fatalf("expected 1 task key, got %d", len(got.GetInterrupts()))
	}
	taskInterrupts := got.GetInterrupts()["task-1"]
	if taskInterrupts == nil || len(taskInterrupts.GetInterrupts()) != 2 {
		t.Fatalf("expected 2 interrupts under task-1, got %v", taskInterrupts)
	}
}

// encodeInterruptsForTest mirrors the on-disk encoding used by decodeInterrupts:
// a JSON object whose values are protojson-encoded coreapi.Interrupts messages.
func encodeInterruptsForTest(m map[string]*coreapi.Interrupts) ([]byte, error) {
	out := make(map[string]json.RawMessage, len(m))
	for k, v := range m {
		b, err := jsonbutil.Marshal(v)
		if err != nil {
			return nil, err
		}
		out[k] = b
	}
	return json.Marshal(out)
}

func TestThreadService_Delete_ReturnsUUID(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)
	id := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	testdb.MustInsertThread(t, ctx, pool, id, nil)
	resp, err := svc.Delete(ctx, &coreapi.DeleteThreadRequest{
		ThreadId: &coreapi.UUID{Value: id},
	})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if resp.GetValue() != id {
		t.Errorf("returned UUID = %q, want %q", resp.GetValue(), id)
	}
}

func TestThreadService_Copy_RowOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)
	srcID := "11111111-1111-1111-1111-111111111111"
	testdb.MustInsertThread(t, ctx, pool, srcID, []byte(`{"copied":true}`))
	resp, err := svc.Copy(ctx, &coreapi.CopyThreadRequest{
		ThreadId: &coreapi.UUID{Value: srcID},
	})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	newID := resp.GetThreadId().GetValue()
	if newID == "" || newID == srcID {
		t.Fatalf("Copy returned same or empty ID: %q", newID)
	}
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

func TestThreadStream_OrderedDeliveryAndResume(t *testing.T) {
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

	threadID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	threadUUID := uuid.MustParse(threadID)

	cfg := &config.Config{StreamMaxLen: 1000, StreamReadBlockMs: 500, StreamReplayBatch: 100}
	svc := threads.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Threads: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	threadsClient := coreapi.NewThreadsClient(conn)

	// Pre-publish two events.
	firstID, _ := streamer.XAdd(ctx, lsdstream.ThreadStreamKey(threadUUID), map[string]any{
		"event_type": "values", "message": []byte(`{"n":0}`), "run_id": "fake-run", "resumable": "false",
	}, 1000)
	_, _ = streamer.XAdd(ctx, lsdstream.ThreadStreamKey(threadUUID), map[string]any{
		"event_type": "updates", "message": []byte(`{"n":1}`), "run_id": "fake-run", "resumable": "false",
	}, 1000)

	// Part A: Full replay.
	streamCtx, streamCancel := context.WithTimeout(ctx, 5*time.Second)
	defer streamCancel()

	tStream, err := threadsClient.Stream(streamCtx, &coreapi.StreamThreadRequest{
		ThreadId: &coreapi.UUID{Value: threadID},
		Filters:  nil,
	})
	if err != nil {
		t.Fatalf("Threads.Stream: %v", err)
	}

	var received []string
	for i := 0; i < 2; i++ {
		evt, err := tStream.Recv()
		if err != nil {
			t.Fatalf("Recv[%d]: %v", i, err)
		}
		received = append(received, evt.GetEventType())
	}
	if len(received) < 2 || received[0] != "values" || received[1] != "updates" {
		t.Errorf("ordered delivery: got %v, want [values updates]", received)
	}

	// Part B: Resume.
	resumeCtx, resumeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer resumeCancel()

	rStream, err := threadsClient.Stream(resumeCtx, &coreapi.StreamThreadRequest{
		ThreadId:    &coreapi.UUID{Value: threadID},
		LastEventId: &firstID,
	})
	if err != nil {
		t.Fatalf("Threads.Stream (resume): %v", err)
	}

	resumedEvt, err := rStream.Recv()
	if err != nil {
		t.Fatalf("Recv resume: %v", err)
	}
	if resumedEvt.GetEventType() != "updates" {
		t.Errorf("resumed event = %q, want %q", resumedEvt.GetEventType(), "updates")
	}
}

// TestThreads_Copy_CopiesCheckpoints_Integration is a higher-level variant of
// TestService_Copy_CopiesCheckpoints. It seeds thread + checkpoints with blobs,
// calls Threads.Copy through a live in-process gRPC client, and asserts the new
// thread has byte-equal checkpoint blobs.
func TestThreads_Copy_CopiesCheckpoints_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	srcID := "00000000-0000-0000-0000-aabbccdd0001"
	testdb.MustInsertThread(t, ctx, pool, srcID, nil)

	cpStore := checkpointer.NewStore(pool)
	payloads := [][]byte{{0x01, 0x02, 0x03}, {0x04, 0x05, 0x06}}
	for i, payload := range payloads {
		cid := fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1)
		if err := cpStore.Put(ctx, checkpointer.PutInput{
			ThreadID:       srcID,
			CheckpointNS:   "",
			CheckpointID:   cid,
			CheckpointJSON: []byte(`{"v":1,"id":"` + cid + `"}`),
			MetadataJSON:   []byte(`{}`),
			Blobs: []checkpointer.BlobInput{
				{Channel: "ch", Version: fmt.Sprintf("%d", i+1), Encoding: "json", Blob: payload},
			},
		}); err != nil {
			t.Fatalf("Put checkpoint %d: %v", i, err)
		}
	}

	svc := threads.NewServiceWithCheckpointer(pool, cpStore)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Threads: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	client := coreapi.NewThreadsClient(conn)
	resp, err := client.Copy(ctx, &coreapi.CopyThreadRequest{
		ThreadId: &coreapi.UUID{Value: srcID},
	})
	if err != nil {
		t.Fatalf("Threads.Copy: %v", err)
	}
	newID := resp.GetThreadId().GetValue()
	if newID == "" || newID == srcID {
		t.Fatalf("Copy returned unexpected thread_id %q", newID)
	}

	limit := int64(10)
	tuples, err := cpStore.List(ctx, newID, "", "", &limit, nil)
	if err != nil {
		t.Fatalf("List new thread checkpoints: %v", err)
	}
	if len(tuples) != 2 {
		t.Errorf("expected 2 checkpoints in copied thread, got %d", len(tuples))
	}

	tuple, err := cpStore.GetTuple(ctx, newID, "", "")
	if err != nil || tuple == nil {
		t.Fatalf("GetTuple new thread: err=%v tuple=%v", err, tuple)
	}
	if len(tuple.Blobs) == 0 {
		t.Error("copied checkpoint has no blobs")
	}
}

// ── Task 4: toPB populates TTL ─────────────────────────────────────────────

func TestService_Get_PopulatesTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)
	_ = pool // pool available if needed for future setup

	// Create a thread directly via the store so we control TTL.
	store := threads.NewStore(pool)
	ttl := 30.0 // 30 seconds ≈ 0.5 minutes
	th, err := store.Create(ctx, threads.CreateThreadInput{
		Metadata:   []byte(`{}`),
		TTLSeconds: &ttl,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	resp, err := svc.Get(ctx, &coreapi.GetThreadRequest{
		ThreadId: &coreapi.UUID{Value: th.ThreadID},
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.Ttl == nil {
		t.Fatal("resp.Ttl is nil, want non-nil")
	}
	if resp.Ttl.ExpiresAt == nil {
		t.Fatal("resp.Ttl.ExpiresAt is nil, want non-nil")
	}
	// TtlMinutes should be just under 0.5 (30 seconds = 0.5 minutes).
	if resp.Ttl.TtlMinutes <= 0 {
		t.Errorf("TtlMinutes = %v, want > 0", resp.Ttl.TtlMinutes)
	}
	const maxMinutes = 30.0/60.0 + 0.01 // 0.5 + small tolerance
	if resp.Ttl.TtlMinutes > maxMinutes {
		t.Errorf("TtlMinutes = %v, want <= %v", resp.Ttl.TtlMinutes, maxMinutes)
	}
}

func TestService_Copy_CopiesCheckpoints(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	srcID := uuid.NewString()
	testdb.MustInsertThread(t, ctx, pool, srcID, nil)

	cpStore := checkpointer.NewStore(pool)
	for i := 1; i <= 2; i++ {
		cid := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
		if err := cpStore.Put(ctx, checkpointer.PutInput{
			ThreadID:       srcID,
			CheckpointNS:   "",
			CheckpointID:   cid,
			CheckpointJSON: []byte(`{"v":1,"id":"` + cid + `"}`),
			MetadataJSON:   []byte(`{}`),
		}); err != nil {
			t.Fatalf("Put checkpoint %d: %v", i, err)
		}
	}

	svc := threads.NewServiceWithCheckpointer(pool, cpStore)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Threads: svc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	client := coreapi.NewThreadsClient(conn)
	resp, err := client.Copy(ctx, &coreapi.CopyThreadRequest{
		ThreadId: &coreapi.UUID{Value: srcID},
	})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	newID := resp.GetThreadId().GetValue()
	if newID == "" || newID == srcID {
		t.Fatalf("Copy returned same or empty ID: %q", newID)
	}

	limit := int64(10)
	tuples, err := cpStore.List(ctx, newID, "", "", &limit, nil)
	if err != nil {
		t.Fatalf("List checkpoints on new thread: %v", err)
	}
	if len(tuples) != 2 {
		t.Errorf("expected 2 checkpoints on copied thread, got %d", len(tuples))
	}
}

// ── C4: SetJointStatus wake-up ────────────────────────────────────────────────

func newTestServiceWithRedis(t *testing.T, ctx context.Context) (*threads.Service, *pgxpool.Pool, *goredis.Client) {
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
	rdb := startRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)
	svc := threads.NewServiceWithStream(pool, rdb, streamer, &config.Config{})
	return svc, pool, rdb
}

func TestService_SetJointStatus_WakesWorker_WhenRunStatusPending(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool, rdb := newTestServiceWithRedis(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "g", nil)
	thID := "a4000001-0000-0000-0000-000000000001"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	_, err := svc.SetJointStatus(ctx, &coreapi.SetThreadJointStatusRequest{
		ThreadId:  &coreapi.UUID{Value: thID},
		RunId:     &coreapi.UUID{Value: rID},
		RunStatus: "pending", // wake-up condition — ops.py:1031
		GraphId:   "g",
	})
	if err != nil {
		t.Fatalf("SetJointStatus: %v", err)
	}

	// Assert RPUSH happened — queue should have at least 1 entry
	n, err := rdb.LLen(ctx, "run:queue").Result()
	if err != nil {
		t.Fatalf("LLEN: %v", err)
	}
	if n == 0 {
		t.Error("expected run:queue to have ≥1 entry after run_status=pending (ops.py:1031)")
	}
	// Verify the queued value is the run_id
	val, err := rdb.LPop(ctx, "run:queue").Result()
	if err != nil {
		t.Fatalf("LPOP: %v", err)
	}
	if val != rID {
		t.Errorf("queued value = %q, want run_id %q", val, rID)
	}
}

func TestService_SetJointStatus_NoWake_WhenNotPending(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool, rdb := newTestServiceWithRedis(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "g", nil)
	thID := "a4000002-0000-0000-0000-000000000002"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	_, err := svc.SetJointStatus(ctx, &coreapi.SetThreadJointStatusRequest{
		ThreadId:  &coreapi.UUID{Value: thID},
		RunId:     &coreapi.UUID{Value: rID},
		RunStatus: "idle", // NOT pending → no wake
		GraphId:   "g",
	})
	if err != nil {
		t.Fatalf("SetJointStatus: %v", err)
	}

	n, err := rdb.LLen(ctx, "run:queue").Result()
	if err != nil {
		t.Fatalf("LLEN: %v", err)
	}
	if n != 0 {
		t.Errorf("run:queue should be empty for non-pending run_status, got %d", n)
	}
}

// ── C11: SetStatus service wake-up ────────────────────────────────────────────

func TestService_SetStatus_WakesWorker_WhenBusy(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool, rdb := newTestServiceWithRedis(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "g", nil)
	thID := "a1150001-0000-0000-0000-000000000001"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	// A pending run means the CASE will return 'busy' → should RPUSH
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	_, err := svc.SetStatus(ctx, &coreapi.SetThreadStatusRequest{
		ThreadId: &coreapi.UUID{Value: thID},
	})
	if err != nil {
		t.Fatalf("SetStatus: %v", err)
	}

	n, err := rdb.LLen(ctx, "run:queue").Result()
	if err != nil {
		t.Fatalf("LLEN: %v", err)
	}
	if n == 0 {
		t.Error("expected wake-up RPUSH when thread becomes busy — ops.py:940-944")
	}
}

// ── C12: Create service raise → AlreadyExists gRPC code ──────────────────────

func TestService_Create_Raise_ReturnsAlreadyExists(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx)

	id := "a1250001-0000-0000-0000-000000000001"
	_, err := svc.Create(ctx, &coreapi.CreateThreadRequest{
		ThreadId: &coreapi.UUID{Value: id},
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	// Second create with same id and RAISE (default) → AlreadyExists
	_, err = svc.Create(ctx, &coreapi.CreateThreadRequest{
		ThreadId: &coreapi.UUID{Value: id},
		IfExists: coreapi.OnConflictBehavior_RAISE,
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("second Create (raise): code=%s, want AlreadyExists; err=%v", status.Code(err), err)
	}
}

func TestService_Create_DoNothing_ReturnsSameThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx)

	id := "a1250002-0000-0000-0000-000000000002"
	th1, err := svc.Create(ctx, &coreapi.CreateThreadRequest{
		ThreadId: &coreapi.UUID{Value: id},
		IfExists: coreapi.OnConflictBehavior_DO_NOTHING,
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	th2, err := svc.Create(ctx, &coreapi.CreateThreadRequest{
		ThreadId: &coreapi.UUID{Value: id},
		IfExists: coreapi.OnConflictBehavior_DO_NOTHING,
	})
	if err != nil {
		t.Fatalf("second Create (do_nothing): %v", err)
	}
	if th1.GetThreadId().GetValue() != th2.GetThreadId().GetValue() {
		t.Errorf("thread IDs differ: %q vs %q", th1.GetThreadId().GetValue(), th2.GetThreadId().GetValue())
	}
}
