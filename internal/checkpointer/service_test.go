package checkpointer_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	checkpointerpb "github.com/duongnghia222/langsmith-deployment-go/gen/checkpointer"
	engine_common "github.com/duongnghia222/langsmith-deployment-go/gen/engine_common"
	"github.com/duongnghia222/langsmith-deployment-go/internal/checkpointer"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
)

// newTestService creates a fresh Store + in-process gRPC server, returning the
// CheckpointerClient to use in tests. The server and pool are cleaned up via
// t.Cleanup automatically.
func newTestService(t *testing.T, ctx context.Context) (checkpointerpb.CheckpointerClient, *pgxpool.Pool) {
	t.Helper()
	dsn := testdb.Start(t, ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(pool, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	store := checkpointer.NewStore(pool)
	svc := checkpointer.NewService(store)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	checkpointerpb.RegisterCheckpointerServer(grpcServer, svc)
	go grpcServer.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcServer.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return checkpointerpb.NewCheckpointerClient(conn), pool
}

func TestService_GetCapabilities(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	client, _ := newTestService(t, ctx)

	caps, err := client.GetCapabilities(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetCapabilities: %v", err)
	}
	if !caps.GetSupportsDeleteThread() {
		t.Error("SupportsDeleteThread should be true")
	}
	if !caps.GetSupportsPrune() {
		t.Error("SupportsPrune should be true")
	}
	if !caps.GetSupportsDeleteForRuns() {
		t.Error("SupportsDeleteForRuns should be true")
	}
	if !caps.GetSupportsCopyThread() {
		t.Error("SupportsCopyThread should be true")
	}
}

func TestService_Put_GetTuple_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	client, pool := newTestService(t, ctx)

	threadID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	checkpointID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

	threadIDStr := threadID
	nsStr := ""
	putResp, err := client.Put(ctx, &checkpointerpb.PutRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
		},
		Checkpoint: &engine_common.Checkpoint{
			Id: checkpointID,
		},
		Metadata: &engine_common.CheckpointMetadata{},
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if putResp.GetNextConfig() == nil {
		t.Fatal("Put response missing NextConfig")
	}

	cidStr := checkpointID
	tupleResp, err := client.GetTuple(ctx, &checkpointerpb.GetTupleRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
			CheckpointId: &cidStr,
		},
	})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if tupleResp.GetCheckpointTuple() == nil {
		t.Fatal("GetTuple returned nil tuple after Put")
	}
}

func TestService_PutWrites_SendsRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	client, pool := newTestService(t, ctx)

	threadID := "11111111-1111-1111-1111-111111111111"
	checkpointID := "22222222-2222-2222-2222-222222222222"
	taskID := "33333333-3333-3333-3333-333333333333"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	threadIDStr := threadID
	nsStr := ""
	cidStr := checkpointID

	if _, err := client.Put(ctx, &checkpointerpb.PutRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
		},
		Checkpoint: &engine_common.Checkpoint{Id: checkpointID},
		Metadata:   &engine_common.CheckpointMetadata{},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sendsValue := &engine_common.ChannelValue{
		Val: &engine_common.ChannelValue_Sends{
			Sends: &engine_common.Sends{
				Sends: []*engine_common.Send{
					{
						Node: "next_node",
						Arg: &engine_common.SerializedValue{
							Encoding: "json",
							Value:    []byte(`{"k":"v"}`),
						},
					},
				},
			},
		},
	}
	if _, err := client.PutWrites(ctx, &checkpointerpb.PutWritesRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
			CheckpointId: &cidStr,
		},
		TaskId:   taskID,
		TaskPath: "node/branch",
		Writes: []*engine_common.Write{
			{
				Channel: "out",
				Value: &engine_common.ChannelValue{
					Val: &engine_common.ChannelValue_SerializedValue{
						SerializedValue: &engine_common.SerializedValue{
							Encoding: "json",
							Value:    []byte(`"hello"`),
						},
					},
				},
			},
			{
				Channel: "__pregel_tasks",
				Value:   sendsValue,
			},
		},
	}); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	resp, err := client.GetTuple(ctx, &checkpointerpb.GetTupleRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
			CheckpointId: &cidStr,
		},
	})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	tuple := resp.GetCheckpointTuple()
	if tuple == nil {
		t.Fatal("GetTuple returned nil tuple")
	}
	if len(tuple.GetPendingWrites()) != 2 {
		t.Fatalf("expected 2 pending writes, got %d", len(tuple.GetPendingWrites()))
	}
	var got *engine_common.PendingWrite
	for _, pw := range tuple.GetPendingWrites() {
		if pw.GetChannel() == "__pregel_tasks" {
			got = pw
			break
		}
	}
	if got == nil {
		t.Fatal("missing __pregel_tasks pending write in round trip")
	}
	sends := got.GetValue().GetSends()
	if sends == nil {
		t.Fatalf("expected Sends oneof on round trip, got %T", got.GetValue().GetVal())
	}
	if len(sends.GetSends()) != 1 || sends.GetSends()[0].GetNode() != "next_node" {
		t.Fatalf("Sends payload not preserved: %+v", sends.GetSends())
	}
	arg := sends.GetSends()[0].GetArg()
	if arg.GetEncoding() != "json" || string(arg.GetValue()) != `{"k":"v"}` {
		t.Fatalf("Send.arg not preserved: enc=%q val=%q", arg.GetEncoding(), arg.GetValue())
	}
}

// TestService_PutWrites_EmptyBytesSerializedValue covers the production
// regression where HITL writes for control channels (e.g. branch:to:model,
// __no_writes__) hit a NOT NULL violation on checkpoint_writes.blob.
//
// Path: langgraph's JsonPlusSerializer.dumps_typed(None) returns ("null", b"")
// — a 4-char encoding string and EMPTY bytes. proto3's wire format omits
// default-valued bytes, so on the Go side sv.GetValue() returns a nil []byte.
// Without coercion, pgx writes that as SQL NULL → constraint violation.
//
// The fix in encodeWriteValue() coerces nil to []byte{}. This test exercises
// both the nil and explicitly-empty []byte{} inputs to lock the behavior in.
func TestService_PutWrites_EmptyBytesSerializedValue(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	client, pool := newTestService(t, ctx)

	threadID := "44444444-4444-4444-4444-444444444444"
	checkpointID := "55555555-5555-5555-5555-555555555555"
	taskID := "66666666-6666-6666-6666-666666666666"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	threadIDStr := threadID
	nsStr := ""
	cidStr := checkpointID

	if _, err := client.Put(ctx, &checkpointerpb.PutRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
		},
		Checkpoint: &engine_common.Checkpoint{Id: checkpointID},
		Metadata:   &engine_common.CheckpointMetadata{},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Mirror what JsonPlusSerializer.dumps_typed(None) sends on the wire for
	// the control-channel writes that broke HITL: encoding "null" with
	// nil-or-empty bytes. Cover both forms because proto3 collapses them on
	// decode.
	if _, err := client.PutWrites(ctx, &checkpointerpb.PutWritesRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
			CheckpointId: &cidStr,
		},
		TaskId:   taskID,
		TaskPath: "~__pregel_pull, __start__",
		Writes: []*engine_common.Write{
			{
				Channel: "branch:to:model",
				Value: &engine_common.ChannelValue{
					Val: &engine_common.ChannelValue_SerializedValue{
						SerializedValue: &engine_common.SerializedValue{
							Encoding: "null",
							Value:    nil,
						},
					},
				},
			},
			{
				Channel: "__no_writes__",
				Value: &engine_common.ChannelValue{
					Val: &engine_common.ChannelValue_SerializedValue{
						SerializedValue: &engine_common.SerializedValue{
							Encoding: "null",
							Value:    []byte{},
						},
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("PutWrites with empty-bytes SerializedValue must not fail (production regression): %v", err)
	}

	// Verify the rows actually exist with non-NULL blobs.
	var nullBlobs int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM checkpoint_writes
		   WHERE thread_id=$1 AND checkpoint_id=$2 AND blob IS NULL`,
		threadID, checkpointID,
	).Scan(&nullBlobs); err != nil {
		t.Fatalf("query null-blob count: %v", err)
	}
	if nullBlobs != 0 {
		t.Fatalf("expected zero rows with NULL blob, got %d", nullBlobs)
	}

	resp, err := client.GetTuple(ctx, &checkpointerpb.GetTupleRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
			CheckpointId: &cidStr,
		},
	})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	tuple := resp.GetCheckpointTuple()
	if tuple == nil {
		t.Fatal("GetTuple returned nil tuple")
	}
	if got, want := len(tuple.GetPendingWrites()), 2; got != want {
		t.Fatalf("pending writes: got %d, want %d", got, want)
	}
	for _, pw := range tuple.GetPendingWrites() {
		sv := pw.GetValue().GetSerializedValue()
		if sv == nil {
			t.Fatalf("channel %q: expected SerializedValue oneof, got %T",
				pw.GetChannel(), pw.GetValue().GetVal())
		}
		if sv.GetEncoding() != "null" {
			t.Errorf("channel %q: encoding=%q, want %q",
				pw.GetChannel(), sv.GetEncoding(), "null")
		}
	}
}

func TestService_AllNineRPCs_NoError(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	client, pool := newTestService(t, ctx)

	threadID := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	toThreadID := "dddddddd-dddd-dddd-dddd-dddddddddddd"
	checkpointID := "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
	taskID := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	testdb.MustInsertThread(t, ctx, pool, toThreadID, nil)

	threadIDStr := threadID
	toThreadIDStr := toThreadID
	nsStr := ""
	cidStr := checkpointID

	// 1. GetCapabilities
	if _, err := client.GetCapabilities(ctx, &emptypb.Empty{}); err != nil {
		t.Errorf("GetCapabilities: %v", err)
	}

	// 2. Put
	if _, err := client.Put(ctx, &checkpointerpb.PutRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
		},
		Checkpoint: &engine_common.Checkpoint{
			Id: checkpointID,
		},
		Metadata: &engine_common.CheckpointMetadata{},
	}); err != nil {
		t.Errorf("Put: %v", err)
	}

	// 3. PutWrites
	if _, err := client.PutWrites(ctx, &checkpointerpb.PutWritesRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
			CheckpointId: &cidStr,
		},
		TaskId:   taskID,
		TaskPath: "node/branch",
		Writes: []*engine_common.Write{
			{
				Channel: "out",
				Value: &engine_common.ChannelValue{
					Val: &engine_common.ChannelValue_SerializedValue{
						SerializedValue: &engine_common.SerializedValue{
							Encoding: "json",
							Value:    []byte(`"hello"`),
						},
					},
				},
			},
		},
	}); err != nil {
		t.Errorf("PutWrites: %v", err)
	}

	// 4. List
	if _, err := client.List(ctx, &checkpointerpb.ListRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
		},
	}); err != nil {
		t.Errorf("List: %v", err)
	}

	// 5. GetTuple
	if _, err := client.GetTuple(ctx, &checkpointerpb.GetTupleRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
		},
	}); err != nil {
		t.Errorf("GetTuple: %v", err)
	}

	// 6. DeleteThread
	if _, err := client.DeleteThread(ctx, &checkpointerpb.DeleteThreadRequest{
		ThreadId: threadID,
	}); err != nil {
		t.Errorf("DeleteThread: %v", err)
	}

	// 7. DeleteForRuns
	if _, err := client.DeleteForRuns(ctx, &checkpointerpb.DeleteForRunsRequest{
		RunIds: []string{},
	}); err != nil {
		t.Errorf("DeleteForRuns: %v", err)
	}

	// 8. CopyThread
	if _, err := client.CopyThread(ctx, &checkpointerpb.CopyThreadRequest{
		FromThreadId: threadID,
		ToThreadId:   toThreadIDStr,
	}); err != nil {
		t.Errorf("CopyThread: %v", err)
	}

	// 9. Prune
	if _, err := client.Prune(ctx, &checkpointerpb.PruneRequest{
		ThreadIds: []string{toThreadID},
		Strategy:  checkpointerpb.PruneRequest_DELETE_ALL,
	}); err != nil {
		t.Errorf("Prune: %v", err)
	}
}

// ── Task 1: checkpointer round-trip parity tests (service level) ─────────────

// TestService_FullRoundTrip_ChannelVersionsAndMetadata tests that channel_values,
// channel_versions, metadata (source/step/parents), and parent_config all survive
// a Put → GetTuple round trip (gaps C1, C2, C3, C4).
func TestService_FullRoundTrip_ChannelVersionsAndMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	client, pool := newTestService(t, ctx)

	threadID := "ffff0001-0001-0001-0001-000000000001"
	parentID := "ffff0001-0001-0001-0001-000000000002"
	checkpointID := "ffff0001-0001-0001-0001-000000000003"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	threadIDStr := threadID
	nsStr := ""
	parentIDStr := parentID

	// First put a "parent" checkpoint so the foreign key / reference is valid.
	if _, err := client.Put(ctx, &checkpointerpb.PutRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
		},
		Checkpoint: &engine_common.Checkpoint{
			Id:              parentID,
			V:               1,
			Ts:              "2024-01-01T00:00:00+00:00",
			ChannelVersions: map[string]string{"ch1": "v1"},
			VersionsSeen:    map[string]*engine_common.ChannelVersions{},
			UpdatedChannels: []string{"ch1"},
		},
		NewVersions: map[string]string{"ch1": "v1"},
		Metadata: &engine_common.CheckpointMetadata{
			Source:  engine_common.CheckpointMetadata_input,
			Step:    0,
			Parents: map[string]string{},
		},
	}); err != nil {
		t.Fatalf("Put parent: %v", err)
	}

	// Now put the main checkpoint with parent reference and channel blob
	if _, err := client.Put(ctx, &checkpointerpb.PutRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
			CheckpointId: &parentIDStr,
		},
		Checkpoint: &engine_common.Checkpoint{
			Id:              checkpointID,
			V:               1,
			Ts:              "2024-01-01T00:00:01+00:00",
			ChannelVersions: map[string]string{"messages": "00000000000000000000000000000001.abc"},
			VersionsSeen:    map[string]*engine_common.ChannelVersions{"node1": {ChannelVersions: map[string]string{"messages": "v0"}}},
			UpdatedChannels: []string{"messages"},
			ChannelValues: map[string]*engine_common.ChannelValue{
				"messages": {
					Val: &engine_common.ChannelValue_SerializedValue{
						SerializedValue: &engine_common.SerializedValue{
							Encoding: "json",
							Value:    []byte(`["hello"]`),
						},
					},
				},
			},
		},
		NewVersions: map[string]string{"messages": "00000000000000000000000000000001.abc"},
		Metadata: &engine_common.CheckpointMetadata{
			Source:  engine_common.CheckpointMetadata_loop,
			Step:    1,
			Parents: map[string]string{"": parentID},
		},
	}); err != nil {
		t.Fatalf("Put checkpoint: %v", err)
	}

	cidStr := checkpointID
	tupleResp, err := client.GetTuple(ctx, &checkpointerpb.GetTupleRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
			CheckpointId: &cidStr,
		},
	})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	tuple := tupleResp.GetCheckpointTuple()
	if tuple == nil {
		t.Fatal("GetTuple returned nil tuple")
	}

	cp := tuple.GetCheckpoint()
	if cp == nil {
		t.Fatal("checkpoint nil in tuple")
	}

	// C2: channel_versions round-trips
	if cv := cp.GetChannelVersions(); cv["messages"] != "00000000000000000000000000000001.abc" {
		t.Errorf("channel_versions[messages] = %q, want version string", cv["messages"])
	}

	// C2: versions_seen round-trips
	if vs := cp.GetVersionsSeen(); vs == nil {
		t.Error("versions_seen is nil")
	} else if inner := vs["node1"]; inner == nil || inner.ChannelVersions["messages"] != "v0" {
		t.Errorf("versions_seen[node1][messages] = %v, want 'v0'", inner)
	}

	// C2: ts round-trips
	if cp.GetTs() != "2024-01-01T00:00:01+00:00" {
		t.Errorf("ts = %q, want '2024-01-01T00:00:01+00:00'", cp.GetTs())
	}

	// C1: channel_values reconstructed from blobs
	chanVals := cp.GetChannelValues()
	if chanVals == nil {
		t.Fatal("channel_values is nil after round-trip")
	}
	msgVal := chanVals["messages"]
	if msgVal == nil {
		t.Fatal("channel_values[messages] missing after round-trip")
	}
	sv := msgVal.GetSerializedValue()
	if sv == nil || sv.GetEncoding() != "json" || string(sv.GetValue()) != `["hello"]` {
		t.Errorf("channel_values[messages] = %+v, want json/[\"hello\"]", msgVal)
	}

	// C3: parent_config populated
	if tuple.GetParentConfig() == nil {
		t.Error("parent_config is nil, want set to parentID")
	} else if got := tuple.GetParentConfig().GetCheckpointId(); got != parentID {
		t.Errorf("parent_config.checkpoint_id = %q, want %q", got, parentID)
	}

	// C4: metadata source, step, parents round-trip
	md := tuple.GetMetadata()
	if md == nil {
		t.Fatal("metadata is nil")
	}
	if md.GetSource() != engine_common.CheckpointMetadata_loop {
		t.Errorf("metadata.source = %v, want loop", md.GetSource())
	}
	if md.GetStep() != 1 {
		t.Errorf("metadata.step = %d, want 1", md.GetStep())
	}
	if md.GetParents()[""] != parentID {
		t.Errorf("metadata.parents[''] = %q, want %q", md.GetParents()[""], parentID)
	}
}

// TestService_WRITES_IDX_MAP verifies that sentinel channels (__error__, etc.)
// get their fixed negative idx values (gap C6).
func TestService_WRITES_IDX_MAP(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	client, pool := newTestService(t, ctx)

	threadID := "ffff0002-0002-0002-0002-000000000001"
	checkpointID := "ffff0002-0002-0002-0002-000000000002"
	taskID := "ffff0002-0002-0002-0002-000000000003"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	threadIDStr := threadID
	nsStr := ""
	cidStr := checkpointID

	if _, err := client.Put(ctx, &checkpointerpb.PutRequest{
		Config:     &engine_common.EngineRunnableConfig{ThreadId: &threadIDStr, CheckpointNs: &nsStr},
		Checkpoint: &engine_common.Checkpoint{Id: checkpointID},
		Metadata:   &engine_common.CheckpointMetadata{},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Write sentinel channels + one regular write
	if _, err := client.PutWrites(ctx, &checkpointerpb.PutWritesRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
			CheckpointId: &cidStr,
		},
		TaskId: taskID,
		Writes: []*engine_common.Write{
			{Channel: "__error__", Value: &engine_common.ChannelValue{Val: &engine_common.ChannelValue_SerializedValue{SerializedValue: &engine_common.SerializedValue{Encoding: "json", Value: []byte(`null`)}}}},
			{Channel: "__interrupt__", Value: &engine_common.ChannelValue{Val: &engine_common.ChannelValue_SerializedValue{SerializedValue: &engine_common.SerializedValue{Encoding: "json", Value: []byte(`null`)}}}},
			{Channel: "regular", Value: &engine_common.ChannelValue{Val: &engine_common.ChannelValue_SerializedValue{SerializedValue: &engine_common.SerializedValue{Encoding: "json", Value: []byte(`"val"`)}}}},
		},
	}); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	// Query idx values directly
	type idxRow struct {
		channel string
		idx     int32
	}
	rows, err := pool.Query(ctx,
		`SELECT channel, idx FROM checkpoint_writes WHERE thread_id=$1::uuid AND task_id=$2::uuid ORDER BY idx`,
		threadID, taskID,
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var got []idxRow
	for rows.Next() {
		var r idxRow
		if err := rows.Scan(&r.channel, &r.idx); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}

	// __error__ → -1, __interrupt__ → -3, regular → 2 (loop index)
	expected := map[string]int32{
		"__error__":     -1,
		"__interrupt__": -3,
		"regular":       2,
	}
	for _, r := range got {
		if want, ok := expected[r.channel]; ok {
			if r.idx != want {
				t.Errorf("channel %q: idx=%d, want %d", r.channel, r.idx, want)
			}
		}
	}
}

// TestService_List_WithFilterJSON tests that List respects filter_json (gap C7).
func TestService_List_WithFilterJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	client, pool := newTestService(t, ctx)

	threadID := "ffff0003-0003-0003-0003-000000000001"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	threadIDStr := threadID
	nsStr := ""

	for i, src := range []engine_common.CheckpointMetadata_CheckpointSource{
		engine_common.CheckpointMetadata_input,
		engine_common.CheckpointMetadata_loop,
		engine_common.CheckpointMetadata_loop,
	} {
		cid := fmt.Sprintf("ffff0003-0003-0003-0003-%012d", i+1)
		cidStr := cid
		if _, err := client.Put(ctx, &checkpointerpb.PutRequest{
			Config: &engine_common.EngineRunnableConfig{
				ThreadId:     &threadIDStr,
				CheckpointNs: &nsStr,
				CheckpointId: &cidStr,
			},
			Checkpoint: &engine_common.Checkpoint{Id: cid},
			Metadata:   &engine_common.CheckpointMetadata{Source: src, Step: int32(i)},
		}); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// filter for loop only
	listLimit := int64(10)
	resp, err := client.List(ctx, &checkpointerpb.ListRequest{
		Config: &engine_common.EngineRunnableConfig{
			ThreadId:     &threadIDStr,
			CheckpointNs: &nsStr,
		},
		FilterJson: []byte(`{"source":"loop"}`),
		Limit:      &listLimit,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got := len(resp.GetCheckpointTuples()); got != 2 {
		t.Errorf("List with filter source=loop: got %d tuples, want 2", got)
	}
}

// TestService_List_FullTuples verifies that List returns fully-loaded tuples
// (channel_values populated, not empty) — gap C8.
func TestService_List_FullTuples(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	client, pool := newTestService(t, ctx)

	threadID := "ffff0004-0004-0004-0004-000000000001"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	threadIDStr := threadID
	nsStr := ""
	cid := "ffff0004-0004-0004-0004-000000000002"

	if _, err := client.Put(ctx, &checkpointerpb.PutRequest{
		Config:     &engine_common.EngineRunnableConfig{ThreadId: &threadIDStr, CheckpointNs: &nsStr},
		Checkpoint: &engine_common.Checkpoint{
			Id:              cid,
			ChannelVersions: map[string]string{"ch": "v1"},
			ChannelValues: map[string]*engine_common.ChannelValue{
				"ch": {Val: &engine_common.ChannelValue_SerializedValue{SerializedValue: &engine_common.SerializedValue{Encoding: "json", Value: []byte(`42`)}}},
			},
		},
		NewVersions: map[string]string{"ch": "v1"},
		Metadata:    &engine_common.CheckpointMetadata{},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	listLimit2 := int64(10)
	resp, err := client.List(ctx, &checkpointerpb.ListRequest{
		Config: &engine_common.EngineRunnableConfig{ThreadId: &threadIDStr, CheckpointNs: &nsStr},
		Limit:  &listLimit2,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	tuples := resp.GetCheckpointTuples()
	if len(tuples) == 0 {
		t.Fatal("List returned no tuples")
	}
	cp := tuples[0].GetCheckpoint()
	if cp == nil {
		t.Fatal("checkpoint is nil in listed tuple")
	}
	chanVals := cp.GetChannelValues()
	if chanVals == nil || chanVals["ch"] == nil {
		t.Error("channel_values missing from listed tuple (gap C8 not fixed)")
	}
}
