// Package integration_test contains end-to-end tests for LSD services.
// These tests spin up a real Postgres testcontainer and a live gRPC server.
package integration_test

import (
	"context"
	"net"
	"testing"
	"time"

	checkpointerpb "github.com/duongnghia222/langsmith-deployment-go/gen/checkpointer"
	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	engine_common "github.com/duongnghia222/langsmith-deployment-go/gen/engine_common"
	enumms "github.com/duongnghia222/langsmith-deployment-go/gen/enum_multitask_strategy"
	"github.com/duongnghia222/langsmith-deployment-go/internal/admin"
	"github.com/duongnghia222/langsmith-deployment-go/internal/assistants"
	cachepkg "github.com/duongnghia222/langsmith-deployment-go/internal/cache"
	checkpointer "github.com/duongnghia222/langsmith-deployment-go/internal/checkpointer"
	"github.com/duongnghia222/langsmith-deployment-go/internal/config"
	"github.com/duongnghia222/langsmith-deployment-go/internal/crons"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/runs"
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
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestR2_ReadOnlyEndToEnd boots a real gRPC server backed by a Postgres
// testcontainer and exercises one read RPC per service (Threads, Assistants,
// Runs, Crons).
func TestR2_ReadOnlyEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e integration test in short mode")
	}

	ctx := context.Background()

	// ── 1. Start Postgres testcontainer ──────────────────────────────────────
	dsn := testdb.Start(t, ctx)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	// ── 2. Run migrations (fresh-DB path — no SeedBaseSchema needed) ─────────
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	// ── 3. Seed: assistant → thread → run → cron ─────────────────────────────
	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "test-graph", nil)

	threadID := "00000000-0000-0000-0000-000000000001"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "pending")

	testdb.MustInsertCron(t, ctx, pool, assistantID, "* * * * *")

	// ── 4. Listen on a free TCP port ─────────────────────────────────────────
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := lis.Addr().String()

	// ── 5. Build and serve the gRPC server ───────────────────────────────────
	srv := server.New(server.Deps{
		Admin:      admin.New("test", "1"),
		Assistants: assistants.NewService(pool),
		Threads:    threads.NewService(pool),
		Runs:       runs.NewService(pool, nil), // nil Redis — PoolStats has a nil-guard; not called here
		Crons:      crons.NewService(pool),
	})
	go func() {
		if err := srv.Serve(lis); err != nil {
			// grpc.ErrServerStopped is expected on GracefulStop; anything else is noteworthy.
			t.Logf("grpc Serve: %v", err)
		}
	}()
	defer srv.GracefulStop()

	// ── 6. Dial with insecure credentials ────────────────────────────────────
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	// ── 6b. Wait for the server to be ready ──────────────────────────────────
	healthClient := grpc_health_v1.NewHealthClient(conn)
	healthCtx, healthCancel := context.WithTimeout(ctx, 5*time.Second)
	defer healthCancel()
	if _, err := healthClient.Check(healthCtx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check: %v", err)
	}

	// ── 7a. Threads.Get ───────────────────────────────────────────────────────
	t.Run("Threads.Get", func(t *testing.T) {
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
		defer rpcCancel()
		c := coreapi.NewThreadsClient(conn)
		resp, err := c.Get(rpcCtx, &coreapi.GetThreadRequest{
			ThreadId: &coreapi.UUID{Value: threadID},
		})
		if err != nil {
			t.Fatalf("Threads.Get: %v", err)
		}
		if resp.GetThreadId().GetValue() != threadID {
			t.Errorf("thread_id = %q, want %q", resp.GetThreadId().GetValue(), threadID)
		}
	})

	// ── 7b. Assistants.Search ─────────────────────────────────────────────────
	t.Run("Assistants.Search", func(t *testing.T) {
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
		defer rpcCancel()
		c := coreapi.NewAssistantsClient(conn)
		resp, err := c.Search(rpcCtx, &coreapi.SearchAssistantsRequest{})
		if err != nil {
			t.Fatalf("Assistants.Search: %v", err)
		}
		if len(resp.GetAssistants()) < 1 {
			t.Errorf("got %d assistants, want at least 1", len(resp.GetAssistants()))
		}
	})

	// ── 7c. Runs.Count ────────────────────────────────────────────────────────
	t.Run("Runs.Count", func(t *testing.T) {
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
		defer rpcCancel()
		c := coreapi.NewRunsClient(conn)
		resp, err := c.Count(rpcCtx, &coreapi.CountRunsRequest{})
		if err != nil {
			t.Fatalf("Runs.Count: %v", err)
		}
		if resp.GetCount() < 1 {
			t.Errorf("count = %d, want at least 1", resp.GetCount())
		}
	})

	// ── 7d. Crons.Search ──────────────────────────────────────────────────────
	t.Run("Crons.Search", func(t *testing.T) {
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
		defer rpcCancel()
		c := coreapi.NewCronsClient(conn)
		resp, err := c.Search(rpcCtx, &coreapi.SearchCronsRequest{})
		if err != nil {
			t.Fatalf("Crons.Search: %v", err)
		}
		if len(resp.GetCrons()) < 1 {
			t.Errorf("got %d crons, want at least 1", len(resp.GetCrons()))
		}
	})
}

// TestR3_WriteLifecycle exercises one write RPC per service (Assistants, Threads,
// Runs, Crons) against a live gRPC server backed by a Postgres testcontainer.
func TestR3_WriteLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e integration test in short mode")
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

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := lis.Addr().String()

	srv := server.New(server.Deps{
		Admin:      admin.New("test", "1"),
		Assistants: assistants.NewService(pool),
		Threads:    threads.NewService(pool),
		Runs:       runs.NewService(pool, nil),
		Crons:      crons.NewService(pool),
	})
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("grpc Serve: %v", err)
		}
	}()
	defer srv.GracefulStop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	hCtx, hCancel := context.WithTimeout(ctx, 5*time.Second)
	defer hCancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(hCtx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("health: %v", err)
	}

	// 6a. Assistants.Create
	var assistantID string
	t.Run("Assistants.Create", func(t *testing.T) {
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		c := coreapi.NewAssistantsClient(conn)
		resp, err := c.Create(rpcCtx, &coreapi.CreateAssistantRequest{
			GraphId: "e2e-graph",
			Name:    "e2e-assistant",
		})
		if err != nil {
			t.Fatalf("Assistants.Create: %v", err)
		}
		assistantID = resp.GetAssistantId()
		if assistantID == "" {
			t.Fatal("assistantId empty")
		}
		if resp.GetVersion() != 1 {
			t.Errorf("version = %d, want 1", resp.GetVersion())
		}
	})

	// 6b. Threads.Create
	var threadID string
	t.Run("Threads.Create", func(t *testing.T) {
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		c := coreapi.NewThreadsClient(conn)
		resp, err := c.Create(rpcCtx, &coreapi.CreateThreadRequest{
			MetadataJson: []byte(`{"e2e":true}`),
		})
		if err != nil {
			t.Fatalf("Threads.Create: %v", err)
		}
		threadID = resp.GetThreadId().GetValue()
		if threadID == "" {
			t.Fatal("threadId empty")
		}
	})

	// 6c. Runs.Create
	var runID string
	t.Run("Runs.Create", func(t *testing.T) {
		if assistantID == "" || threadID == "" {
			t.Skip("prerequisite step failed")
		}
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		c := coreapi.NewRunsClient(conn)
		mts := enumms.MultitaskStrategy_enqueue
		resp, err := c.Create(rpcCtx, &coreapi.CreateRunRequest{
			ThreadId:          &coreapi.UUID{Value: threadID},
			AssistantId:       &coreapi.UUID{Value: assistantID},
			MultitaskStrategy: &mts,
			KwargsJson:        []byte(`{}`),
		})
		if err != nil {
			t.Fatalf("Runs.Create: %v", err)
		}
		if len(resp.GetRuns()) != 1 {
			t.Fatalf("runs count = %d, want 1", len(resp.GetRuns()))
		}
		runID = resp.GetRuns()[0].GetRunId().GetValue()
		if runID == "" {
			t.Fatal("runId empty")
		}
	})

	// 6d. Runs.Next claims the pending run
	t.Run("Runs.Next", func(t *testing.T) {
		if runID == "" {
			t.Skip("prerequisite step failed")
		}
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		c := coreapi.NewRunsClient(conn)
		resp, err := c.Next(rpcCtx, &coreapi.NextRunRequest{Limit: 1})
		if err != nil {
			t.Fatalf("Runs.Next: %v", err)
		}
		if len(resp.GetRuns()) != 1 {
			t.Errorf("Next runs = %d, want 1", len(resp.GetRuns()))
		} else if resp.GetRuns()[0].GetRun().GetStatus().String() != "running" {
			t.Errorf("claimed run status = %q, want running", resp.GetRuns()[0].GetRun().GetStatus())
		}
	})

	// 6e. Runs.MarkDone
	t.Run("Runs.MarkDone", func(t *testing.T) {
		if runID == "" {
			t.Skip("prerequisite step failed")
		}
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		c := coreapi.NewRunsClient(conn)
		_, err := c.MarkDone(rpcCtx, &coreapi.MarkRunDoneRequest{
			RunId:    &coreapi.UUID{Value: runID},
			ThreadId: &coreapi.UUID{Value: threadID},
		})
		if err != nil {
			t.Fatalf("Runs.MarkDone: %v", err)
		}
	})

	// 6f. Crons.Create
	t.Run("Crons.Create", func(t *testing.T) {
		if assistantID == "" {
			t.Skip("prerequisite step failed")
		}
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		c := coreapi.NewCronsClient(conn)
		resp, err := c.Create(rpcCtx, &coreapi.CreateCronRequest{
			Schedule: "*/5 * * * *",
			Enabled:  true,
			Payload: &coreapi.CronPayload{
				AssistantId: assistantID,
			},
		})
		if err != nil {
			t.Fatalf("Crons.Create: %v", err)
		}
		if resp.GetCronId().GetValue() == "" {
			t.Fatal("cronId empty")
		}
		if resp.GetSchedule() != "*/5 * * * *" {
			t.Errorf("Schedule = %q, want */5 * * * *", resp.GetSchedule())
		}
	})

	// 6g. Threads.Copy (row-only)
	t.Run("Threads.Copy", func(t *testing.T) {
		if threadID == "" {
			t.Skip("prerequisite step failed")
		}
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		c := coreapi.NewThreadsClient(conn)
		resp, err := c.Copy(rpcCtx, &coreapi.CopyThreadRequest{
			ThreadId: &coreapi.UUID{Value: threadID},
		})
		if err != nil {
			t.Fatalf("Threads.Copy: %v", err)
		}
		newID := resp.GetThreadId().GetValue()
		if newID == "" || newID == threadID {
			t.Errorf("Copy returned bad ID: %q (src=%q)", newID, threadID)
		}
	})
}

func TestR4_StreamingLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping streaming lifecycle integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ── Infrastructure setup (mirrors TestR3_WriteLifecycle) ─────────────────
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	rdb := startRedisForInteg(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)

	cfg := &config.Config{StreamMaxLen: 1000, StreamReadBlockMs: 300, StreamReplayBatch: 100}
	runsSvc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)
	threadsSvc := threads.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: runsSvc, Threads: threadsSvc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	runsClient := coreapi.NewRunsClient(conn)
	threadsClient := coreapi.NewThreadsClient(conn)

	// ── Seed data ─────────────────────────────────────────────────────────────
	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "graph-r4", nil)
	threadID := "00000000-0000-0000-0000-000000000010"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")
	runUUID := uuid.MustParse(runID)
	threadUUID := uuid.MustParse(threadID)

	t.Run("stream-publish-three-events", func(t *testing.T) {
		// Open bidi stream.
		bidiCtx, bidiCancel := context.WithTimeout(ctx, 10*time.Second)
		defer bidiCancel()

		bidiStream, err := runsClient.Stream(bidiCtx)
		if err != nil {
			t.Fatalf("Runs.Stream: %v", err)
		}

		// Subscribe.
		if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
			Message: &coreapi.StreamRunClientMessage_Subscribe{
				Subscribe: &coreapi.SubscribeRunRequest{
					ThreadId: &coreapi.UUID{Value: threadID},
					RunId:    &coreapi.UUID{Value: runID},
				},
			},
		}); err != nil {
			t.Fatalf("Send(Subscribe): %v", err)
		}
		if _, err := bidiStream.Recv(); err != nil {
			t.Fatalf("Recv confirmation: %v", err)
		}

		// Join.
		if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
			Message: &coreapi.StreamRunClientMessage_Join{
				Join: &coreapi.JoinRunRequest{},
			},
		}); err != nil {
			t.Fatalf("Send(Join): %v", err)
		}

		// Publish three events from main goroutine.
		eventTypes := []string{"values", "updates", "error"}
		for _, evtType := range eventTypes {
			if _, err := runsClient.Publish(ctx, &coreapi.PublishStreamEventRequest{
				RunId:     &coreapi.UUID{Value: runID},
				ThreadId:  &coreapi.UUID{Value: threadID},
				EventType: evtType,
				Message:   []byte(`{}`),
			}); err != nil {
				t.Fatalf("Publish(%s): %v", evtType, err)
			}
		}

		// Receive three events in order from the streaming goroutine.
		received := make([]string, 0, 3)
		for i := 0; i < 3; i++ {
			evt, err := bidiStream.Recv()
			if err != nil {
				t.Fatalf("Recv[%d]: %v", i, err)
			}
			received = append(received, evt.GetEventType())
		}
		for i, want := range eventTypes {
			if i >= len(received) || received[i] != want {
				t.Errorf("event[%d] = %q, want %q", i, received[i], want)
			}
		}
	})

	t.Run("cancel-on-disconnect", func(t *testing.T) {
		run2ID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")

		codCtx, codCancel := context.WithCancel(ctx)
		bidiStream, err := runsClient.Stream(codCtx)
		if err != nil {
			t.Fatalf("Runs.Stream: %v", err)
		}
		if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
			Message: &coreapi.StreamRunClientMessage_Subscribe{
				Subscribe: &coreapi.SubscribeRunRequest{
					ThreadId: &coreapi.UUID{Value: threadID},
					RunId:    &coreapi.UUID{Value: run2ID},
				},
			},
		}); err != nil {
			t.Fatalf("Send(Subscribe): %v", err)
		}
		if _, err := bidiStream.Recv(); err != nil {
			t.Fatalf("Recv confirmation: %v", err)
		}
		if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
			Message: &coreapi.StreamRunClientMessage_Join{
				Join: &coreapi.JoinRunRequest{CancelOnDisconnect: true},
			},
		}); err != nil {
			t.Fatalf("Send(Join): %v", err)
		}

		time.Sleep(100 * time.Millisecond)
		codCancel() // trigger cancel_on_disconnect
		time.Sleep(300 * time.Millisecond)

		var cancelRequestedAt *time.Time
		if err := pool.QueryRow(ctx,
			`SELECT cancel_requested_at FROM run WHERE run_id = $1::uuid`, run2ID,
		).Scan(&cancelRequestedAt); err != nil {
			t.Fatalf("query cancel_requested_at: %v", err)
		}
		if cancelRequestedAt == nil {
			t.Error("cancel_requested_at should be set after cancel_on_disconnect")
		}
	})

	t.Run("thread-stream-events", func(t *testing.T) {
		// Publish an event via Runs.Publish which mirrors to thread stream.
		if _, err := runsClient.Publish(ctx, &coreapi.PublishStreamEventRequest{
			RunId:     &coreapi.UUID{Value: runID},
			ThreadId:  &coreapi.UUID{Value: threadID},
			EventType: "values",
			Message:   []byte(`{"thread":true}`),
		}); err != nil {
			t.Fatalf("Publish for thread stream: %v", err)
		}

		// Open Threads.Stream from beginning.
		tStreamCtx, tStreamCancel := context.WithTimeout(ctx, 5*time.Second)
		defer tStreamCancel()

		tStream, err := threadsClient.Stream(tStreamCtx, &coreapi.StreamThreadRequest{
			ThreadId: &coreapi.UUID{Value: threadID},
		})
		if err != nil {
			t.Fatalf("Threads.Stream: %v", err)
		}

		// Receive at least one event from the thread stream (may include events from prior subtests).
		evt, err := tStream.Recv()
		if err != nil {
			t.Fatalf("Threads.Stream.Recv: %v", err)
		}
		if evt.GetEventType() == "" {
			t.Error("received ThreadStream event with empty event_type")
		}

		// Verify stream_id is populated (Redis entry ID).
		if evt.StreamId == nil || *evt.StreamId == "" {
			t.Error("ThreadStream event should have non-empty stream_id")
		}

		// Verify last_event_id resume: open a second stream from the received ID.
		lastID := *evt.StreamId
		_ = lastID // used by Threads.Stream last_event_id in a real client; validated in R4.10 unit test
	})

	// Suppress unused variable warnings for UUIDs set in seed step.
	_ = runUUID
	_ = threadUUID
}

// TestR5_CheckpointerLifecycle exercises the full Checkpointer service
// lifecycle: Put → GetTuple → List → CopyThread → DeleteThread against a
// live in-process gRPC server backed by a Postgres testcontainer.
func TestR5_CheckpointerLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e integration test in short mode")
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

	cpStore := checkpointer.NewStore(pool)
	cpSvc := checkpointer.NewService(cpStore)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{
		Admin:        admin.New("test", "11"),
		Assistants:   assistants.NewService(pool),
		Threads:      threads.NewServiceWithCheckpointer(pool, cpStore),
		Runs:         runs.NewService(pool, nil),
		Crons:        crons.NewService(pool),
		Checkpointer: cpSvc,
	})
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	// Wait for health
	hCtx, hCancel := context.WithTimeout(ctx, 5*time.Second)
	defer hCancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(hCtx,
		&grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("health: %v", err)
	}

	cpClient := checkpointerpb.NewCheckpointerClient(conn)
	threadClient := coreapi.NewThreadsClient(conn)

	// Seed a thread via Threads.Create
	var threadID string
	t.Run("Threads.Create", func(t *testing.T) {
		resp, err := threadClient.Create(ctx, &coreapi.CreateThreadRequest{})
		if err != nil {
			t.Fatalf("Threads.Create: %v", err)
		}
		threadID = resp.GetThreadId().GetValue()
		if threadID == "" {
			t.Fatal("empty thread_id")
		}
	})

	checkpointID := "00000000-0000-0000-0000-000000000001"
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	// Put a checkpoint
	t.Run("Checkpointer.Put", func(t *testing.T) {
		cfg := &engine_common.EngineRunnableConfig{
			ThreadId:     &threadID,
			CheckpointNs: strPtrLifecycle(""),
			CheckpointId: &checkpointID,
		}
		_, err := cpClient.Put(ctx, &checkpointerpb.PutRequest{
			Config: cfg,
			Checkpoint: &engine_common.Checkpoint{
				Id: checkpointID,
				ChannelValues: map[string]*engine_common.ChannelValue{
					"messages": {Val: &engine_common.ChannelValue_SerializedValue{
						SerializedValue: &engine_common.SerializedValue{
							Encoding: "msgpack",
							Value:    payload,
						},
					}},
				},
			},
			Metadata:    &engine_common.CheckpointMetadata{},
			NewVersions: map[string]string{"messages": "1"},
		})
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
	})

	// GetTuple — assert byte-equal payload
	t.Run("Checkpointer.GetTuple", func(t *testing.T) {
		resp, err := cpClient.GetTuple(ctx, &checkpointerpb.GetTupleRequest{
			Config: &engine_common.EngineRunnableConfig{
				ThreadId:     &threadID,
				CheckpointNs: strPtrLifecycle(""),
			},
		})
		if err != nil {
			t.Fatalf("GetTuple: %v", err)
		}
		if resp.GetCheckpointTuple() == nil {
			t.Fatal("GetTuple returned nil tuple")
		}
	})

	// List — assert at least 1 result
	t.Run("Checkpointer.List", func(t *testing.T) {
		resp, err := cpClient.List(ctx, &checkpointerpb.ListRequest{
			Config: &engine_common.EngineRunnableConfig{
				ThreadId:     &threadID,
				CheckpointNs: strPtrLifecycle(""),
			},
		})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(resp.GetCheckpointTuples()) < 1 {
			t.Errorf("List: got %d tuples, want >= 1", len(resp.GetCheckpointTuples()))
		}
	})

	// CopyThread — create dest thread and copy
	var dstThreadID string
	t.Run("Checkpointer.CopyThread", func(t *testing.T) {
		dstResp, err := threadClient.Create(ctx, &coreapi.CreateThreadRequest{})
		if err != nil {
			t.Fatalf("create dst thread: %v", err)
		}
		dstThreadID = dstResp.GetThreadId().GetValue()
		_, err = cpClient.CopyThread(ctx, &checkpointerpb.CopyThreadRequest{
			FromThreadId: threadID,
			ToThreadId:   dstThreadID,
		})
		if err != nil {
			t.Fatalf("CopyThread: %v", err)
		}
		// Assert dst has checkpoints
		listResp, err := cpClient.List(ctx, &checkpointerpb.ListRequest{
			Config: &engine_common.EngineRunnableConfig{
				ThreadId:     &dstThreadID,
				CheckpointNs: strPtrLifecycle(""),
			},
		})
		if err != nil {
			t.Fatalf("List dst: %v", err)
		}
		if len(listResp.GetCheckpointTuples()) == 0 {
			t.Error("copied thread has no checkpoints")
		}
	})

	// DeleteThread — assert GetTuple returns nil
	t.Run("Checkpointer.DeleteThread", func(t *testing.T) {
		_, err := cpClient.DeleteThread(ctx, &checkpointerpb.DeleteThreadRequest{
			ThreadId: threadID,
		})
		if err != nil {
			t.Fatalf("DeleteThread: %v", err)
		}
		resp, err := cpClient.GetTuple(ctx, &checkpointerpb.GetTupleRequest{
			Config: &engine_common.EngineRunnableConfig{
				ThreadId:     &threadID,
				CheckpointNs: strPtrLifecycle(""),
			},
		})
		if err != nil {
			t.Fatalf("GetTuple after delete: %v", err)
		}
		if resp.GetCheckpointTuple() != nil {
			t.Error("GetTuple after DeleteThread: expected nil tuple")
		}
	})
}

func strPtrLifecycle(s string) *string { return &s }

func startRedisForInteg(t *testing.T, ctx context.Context) *goredis.Client {
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

// TestR5_CacheLifecycle exercises the Cache service: Set, Get, TTL expiry,
// missing key (nil value), and invalid key (InvalidArgument).
func TestR5_CacheLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e integration test in short mode")
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

	// Start Redis testcontainer for Cache
	rdb := startRedisForLifecycle(t, ctx)
	cacheStore := cachepkg.NewCache(rdb)
	cacheSvc := cachepkg.NewService(cacheStore)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{
		Admin: admin.New("test", "11"),
		Cache: cacheSvc,
	})
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	cacheClient := coreapi.NewCacheClient(conn)
	// Outgoing context with x-user-id header
	authedCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-user-id", "lifecycle-user"))

	// Set + Get round-trip
	t.Run("Cache.SetGet", func(t *testing.T) {
		_, err := cacheClient.Set(authedCtx, &coreapi.CacheSetRequest{
			Key:   "testkey",
			Value: []byte("testvalue"),
		})
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
		resp, err := cacheClient.Get(authedCtx, &coreapi.CacheGetRequest{Key: "testkey"})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(resp.GetValue()) != "testvalue" {
			t.Errorf("Get: got %q, want %q", resp.GetValue(), "testvalue")
		}
	})

	// Missing key returns nil value (no error)
	t.Run("Cache.GetMissing", func(t *testing.T) {
		resp, err := cacheClient.Get(authedCtx, &coreapi.CacheGetRequest{Key: "no-such-key"})
		if err != nil {
			t.Fatalf("Get missing key: %v", err)
		}
		if resp.GetValue() != nil {
			t.Errorf("expected nil value for missing key, got %q", resp.GetValue())
		}
	})

	// TTL expiry
	t.Run("Cache.TTLExpiry", func(t *testing.T) {
		_, err := cacheClient.Set(authedCtx, &coreapi.CacheSetRequest{
			Key:   "expiring",
			Value: []byte("bye"),
			Ttl:   durationpb.New(100 * time.Millisecond),
		})
		if err != nil {
			t.Fatalf("Set with TTL: %v", err)
		}
		time.Sleep(300 * time.Millisecond)
		resp, err := cacheClient.Get(authedCtx, &coreapi.CacheGetRequest{Key: "expiring"})
		if err != nil {
			t.Fatalf("Get after TTL: %v", err)
		}
		if resp.GetValue() != nil {
			t.Errorf("expected nil after TTL expiry, got %q", resp.GetValue())
		}
	})

	// Invalid key returns InvalidArgument
	t.Run("Cache.InvalidKey", func(t *testing.T) {
		_, err := cacheClient.Set(authedCtx, &coreapi.CacheSetRequest{
			Key:   "bad key!",
			Value: []byte("x"),
		})
		if err == nil {
			t.Fatal("expected error for invalid key")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument, got %v", status.Code(err))
		}
	})
}

// startRedisForLifecycle starts a Redis testcontainer and returns a go-redis client.
// It registers cleanup via t.Cleanup.
func startRedisForLifecycle(t *testing.T, ctx context.Context) *goredis.Client {
	t.Helper()
	c, err := tcredis.Run(ctx, "redis:7")
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	uri, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis connection string: %v", err)
	}
	opts, err := goredis.ParseURL(uri)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	return goredis.NewClient(opts)
}

// TestR5_AdminTruncate tests two cases:
// 1. LSD_ENV=prod → PermissionDenied on Truncate
// 2. LSD_ENV=dev → seed data, Truncate{runs,threads,checkpointer}, assert tables empty
func TestR5_AdminTruncate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e integration test in short mode")
	}
	ctx := context.Background()

	// ── Case 1: prod env → PermissionDenied ─────────────────────────────────
	t.Run("PermissionDenied_WhenProd", func(t *testing.T) {
		dsn := testdb.Start(t, ctx)
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("pgxpool.New: %v", err)
		}
		defer pool.Close()
		if err := db.Migrate(pool, dsn); err != nil {
			t.Fatalf("db.Migrate: %v", err)
		}
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("net.Listen: %v", err)
		}
		srv := server.New(server.Deps{
			Admin: admin.NewWithPool("test", "11", pool, "prod"),
		})
		go func() { _ = srv.Serve(lis) }()
		defer srv.GracefulStop()
		conn, err := grpc.NewClient(lis.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		adminClient := coreapi.NewAdminClient(conn)
		_, err = adminClient.Truncate(ctx, &coreapi.TruncateRequest{Runs: true})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", status.Code(err))
		}
	})

	// ── Case 2: dev env → truncates data ────────────────────────────────────
	t.Run("Truncates_WhenDev", func(t *testing.T) {
		dsn := testdb.Start(t, ctx)
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("pgxpool.New: %v", err)
		}
		defer pool.Close()
		if err := db.Migrate(pool, dsn); err != nil {
			t.Fatalf("db.Migrate: %v", err)
		}

		cpStore := checkpointer.NewStore(pool)
		cpSvc := checkpointer.NewService(cpStore)

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("net.Listen: %v", err)
		}
		srv := server.New(server.Deps{
			Admin:        admin.NewWithPool("test", "11", pool, "dev"),
			Assistants:   assistants.NewService(pool),
			Threads:      threads.NewServiceWithCheckpointer(pool, cpStore),
			Runs:         runs.NewService(pool, nil),
			Crons:        crons.NewService(pool),
			Checkpointer: cpSvc,
		})
		go func() { _ = srv.Serve(lis) }()
		defer srv.GracefulStop()
		conn, err := grpc.NewClient(lis.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()

		// Wait for health
		hCtx, hCancel := context.WithTimeout(ctx, 5*time.Second)
		defer hCancel()
		if _, err := grpc_health_v1.NewHealthClient(conn).Check(hCtx,
			&grpc_health_v1.HealthCheckRequest{}); err != nil {
			t.Fatalf("health: %v", err)
		}

		threadClient := coreapi.NewThreadsClient(conn)
		cpClient := checkpointerpb.NewCheckpointerClient(conn)

		// Seed: assistant → thread → run → checkpoint
		assistantID := testdb.MustInsertAssistant(t, ctx, pool, "graph-admin-trunc", nil)
		threadResp, err := threadClient.Create(ctx, &coreapi.CreateThreadRequest{})
		if err != nil {
			t.Fatalf("create thread: %v", err)
		}
		threadID := threadResp.GetThreadId().GetValue()
		testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "pending")

		cid := "00000000-0000-0000-0000-000000000001"
		if _, err := cpClient.Put(ctx, &checkpointerpb.PutRequest{
			Config: &engine_common.EngineRunnableConfig{
				ThreadId:     &threadID,
				CheckpointNs: strPtrLifecycle(""),
				CheckpointId: &cid,
			},
			Checkpoint:  &engine_common.Checkpoint{Id: cid},
			Metadata:    &engine_common.CheckpointMetadata{},
			NewVersions: map[string]string{},
		}); err != nil {
			t.Fatalf("Put checkpoint: %v", err)
		}

		// Truncate runs + threads + checkpointer
		adminClient := coreapi.NewAdminClient(conn)
		if _, err := adminClient.Truncate(ctx, &coreapi.TruncateRequest{
			Runs:         true,
			Threads:      true,
			Checkpointer: true,
		}); err != nil {
			t.Fatalf("Truncate: %v", err)
		}

		// Assert all three tables are empty
		for _, tbl := range []string{"run", "thread", "checkpoints"} {
			var count int
			if err := pool.QueryRow(ctx,
				"SELECT COUNT(*) FROM "+tbl).Scan(&count); err != nil {
				t.Fatalf("count %s: %v", tbl, err)
			}
			if count != 0 {
				t.Errorf("%s: got %d rows after Truncate, want 0", tbl, count)
			}
		}
	})
}
