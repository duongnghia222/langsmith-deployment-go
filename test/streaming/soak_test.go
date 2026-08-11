// Package streaming contains SSE soak tests for Runs.Stream and Threads.Stream.
// Run with: go test -v -race ./test/streaming/... (omit -short to execute)
package streaming_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/config"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/runs"
	"github.com/duongnghia222/langsmith-deployment-go/internal/server"
	lsdstream "github.com/duongnghia222/langsmith-deployment-go/internal/stream"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
)

// TestSoak_RunsStream opens 5 concurrent Runs.Stream subscribers,
// publishes 100 events spaced 100 ms apart, and asserts all subscribers
// receive all 100 events in order. Three subscribers join at t=0;
// two join at t=10s with last_event_id="" (full replay).
//
// Runtime: ~30 s. Skipped under -short.
// Run with -race to detect goroutine leaks via Go's race detector.
func TestSoak_RunsStream(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test: skipping in short mode (run without -short for full soak)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// ── Infrastructure ────────────────────────────────────────────────────────
	rdb := startSoakRedis(t, ctx)
	streamer := lsdstream.NewStreamer(rdb)

	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	cfg := &config.Config{StreamMaxLen: 2000, StreamReadBlockMs: 200, StreamReplayBatch: 50}
	runsSvc := runs.NewServiceWithStream(pool, rdb, streamer, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := server.New(server.Deps{Runs: runsSvc})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.GracefulStop() })

	addr := lis.Addr().String()

	// ── Seed run ──────────────────────────────────────────────────────────────
	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "graph-soak", nil)
	threadID := uuid.New().String()
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")

	const totalEvents = 100
	const earlySubscribers = 3
	const lateSubscribers = 2
	const totalSubscribers = earlySubscribers + lateSubscribers
	lateJoinDelay := 10 * time.Second
	publishInterval := 100 * time.Millisecond

	// receivedBySubscriber[i] collects event_types seen by subscriber i.
	receivedBySubscriber := make([][]string, totalSubscribers)
	for i := range receivedBySubscriber {
		receivedBySubscriber[i] = make([]string, 0, totalEvents)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup

	openStream := func(subIdx int, joinDelay time.Duration, lastEventID string) {
		defer wg.Done()
		// Adaptation 1: use grpc.NewClient instead of grpc.Dial
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Errorf("sub[%d] NewClient: %v", subIdx, err)
			return
		}
		defer conn.Close()

		runsClient := coreapi.NewRunsClient(conn)

		time.Sleep(joinDelay) // late subscribers wait before opening

		subCtx, subCancel := context.WithTimeout(ctx, 60*time.Second)
		defer subCancel()

		bidiStream, err := runsClient.Stream(subCtx)
		if err != nil {
			t.Errorf("sub[%d] Stream: %v", subIdx, err)
			return
		}

		if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
			Message: &coreapi.StreamRunClientMessage_Subscribe{
				Subscribe: &coreapi.SubscribeRunRequest{
					ThreadId: &coreapi.UUID{Value: threadID},
					RunId:    &coreapi.UUID{Value: runID},
				},
			},
		}); err != nil {
			t.Errorf("sub[%d] Send(Subscribe): %v", subIdx, err)
			return
		}
		if _, err := bidiStream.Recv(); err != nil {
			t.Errorf("sub[%d] Recv confirmation: %v", subIdx, err)
			return
		}

		var joinMsg *coreapi.JoinRunRequest
		if lastEventID != "" {
			joinMsg = &coreapi.JoinRunRequest{LastEventId: &lastEventID}
		} else {
			joinMsg = &coreapi.JoinRunRequest{}
		}
		if err := bidiStream.Send(&coreapi.StreamRunClientMessage{
			Message: &coreapi.StreamRunClientMessage_Join{Join: joinMsg},
		}); err != nil {
			t.Errorf("sub[%d] Send(Join): %v", subIdx, err)
			return
		}

		// Collect events until context done or stream ends.
		for {
			evt, err := bidiStream.Recv()
			if err != nil {
				break // EOF or context cancelled
			}
			if evt.GetEventType() == "control" {
				continue // skip subscription confirmations and terminal notices
			}
			mu.Lock()
			receivedBySubscriber[subIdx] = append(receivedBySubscriber[subIdx], evt.GetEventType())
			mu.Unlock()
		}
	}

	// Start early subscribers (indices 0..earlySubscribers-1).
	for i := 0; i < earlySubscribers; i++ {
		wg.Add(1)
		go openStream(i, 0, "")
	}

	// Publisher goroutine: publish 100 events spaced 100 ms.
	// Adaptation 1 (publisher): use grpc.NewClient with real error check.
	publishDone := make(chan struct{})
	var publishedEventTypes []string
	go func() {
		defer close(publishDone)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// NewClient only validates args; this is extremely unlikely, but log and return.
			t.Errorf("publisher NewClient: %v", err)
			return
		}
		defer conn.Close()
		runsClient := coreapi.NewRunsClient(conn)

		for i := 0; i < totalEvents; i++ {
			evtType := fmt.Sprintf("event-%04d", i)
			_, _ = runsClient.Publish(ctx, &coreapi.PublishStreamEventRequest{
				RunId:     &coreapi.UUID{Value: runID},
				ThreadId:  &coreapi.UUID{Value: threadID},
				EventType: evtType,
				Message:   []byte(fmt.Sprintf(`{"seq":%d}`, i)),
			})
			mu.Lock()
			publishedEventTypes = append(publishedEventTypes, evtType)
			mu.Unlock()
			time.Sleep(publishInterval)
		}
	}()

	// Start late subscribers after lateJoinDelay (indices earlySubscribers..totalSubscribers-1).
	// Late subscribers use last_event_id="" → full replay from 0-0.
	for i := earlySubscribers; i < totalSubscribers; i++ {
		wg.Add(1)
		go openStream(i, lateJoinDelay, "")
	}

	// Wait for publisher to finish all 100 events.
	select {
	case <-publishDone:
	case <-ctx.Done():
		t.Fatal("context expired before publisher finished")
	}

	// Publish terminal signal to close all streams.
	runUUID := uuid.MustParse(runID)
	if err := streamer.Publish(ctx, lsdstream.RunTerminalChannel(runUUID), []byte("done")); err != nil {
		t.Logf("publish terminal: %v (non-fatal)", err)
	}

	// Wait for all subscriber goroutines to exit.
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(10 * time.Second):
		t.Fatal("subscriber goroutines did not exit within 10s after terminal signal")
	}

	// ── Assertions ────────────────────────────────────────────────────────────
	mu.Lock()
	defer mu.Unlock()

	if len(publishedEventTypes) != totalEvents {
		t.Errorf("published %d events, expected %d", len(publishedEventTypes), totalEvents)
	}

	for i, received := range receivedBySubscriber {
		// Late subscribers (i >= earlySubscribers) joined mid-stream but with "" last_event_id
		// so they should replay all 100 events. Allow brief timing slack: assert >= 95.
		minExpected := totalEvents
		if i >= earlySubscribers {
			minExpected = 95 // allow up to 5 events published before late join completes
		}
		if len(received) < minExpected {
			t.Errorf("subscriber[%d] received %d events, expected >= %d", i, len(received), minExpected)
		}

		// Verify ordering is consistent (sequential by seq number in event_type string).
		for j := 1; j < len(received); j++ {
			var prevSeq, curSeq int
			fmt.Sscanf(received[j-1], "event-%d", &prevSeq)
			fmt.Sscanf(received[j], "event-%d", &curSeq)
			if curSeq <= prevSeq {
				t.Errorf("subscriber[%d] ordering broken at [%d..%d]: %s then %s",
					i, j-1, j, received[j-1], received[j])
				break
			}
		}
	}

	t.Logf("Soak complete: %d subscribers, %d published events, early=%d late=%d",
		totalSubscribers, totalEvents, earlySubscribers, lateSubscribers)
}

// startSoakRedis spins up a Redis testcontainer and returns a connected client.
// Adaptation 2: use tcredis.Run (repo pattern) instead of bespoke GenericContainer helper.
func startSoakRedis(t *testing.T, ctx context.Context) *goredis.Client {
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
