package checkpointer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duongnghia222/langsmith-deployment-go/internal/checkpointer"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
)

func newTestStore(t *testing.T, ctx context.Context) (*checkpointer.Store, *pgxpool.Pool) {
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
	return checkpointer.NewStore(pool), pool
}

func TestStore_Put_GetTuple_ByteEqualRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	threadID := "11111111-1111-1111-1111-111111111111"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	checkpointID := "00000000-0000-0000-0000-000000000001"
	encoding := "msgpack"
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03}

	if err := store.Put(ctx, checkpointer.PutInput{
		ThreadID:       threadID,
		CheckpointNS:   "",
		CheckpointID:   checkpointID,
		CheckpointJSON: []byte(`{"v":1,"id":"` + checkpointID + `","channel_values":{}}`),
		MetadataJSON:   []byte(`{"source":"input","step":0,"parents":{}}`),
		Blobs: []checkpointer.BlobInput{
			{Channel: "messages", Version: "1", Encoding: encoding, Blob: payload},
		},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	tuple, err := store.GetTuple(ctx, threadID, "", "")
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if tuple == nil {
		t.Fatal("GetTuple returned nil tuple")
	}
	if tuple.CheckpointID != checkpointID {
		t.Errorf("CheckpointID = %q, want %q", tuple.CheckpointID, checkpointID)
	}
	if len(tuple.Blobs) == 0 {
		t.Fatal("no blobs in returned tuple")
	}
	found := false
	for _, b := range tuple.Blobs {
		if b.Channel == "messages" && b.Version == "1" {
			if b.Encoding != encoding {
				t.Errorf("encoding: got %q want %q", b.Encoding, encoding)
			}
			if !bytes.Equal(b.Blob, payload) {
				t.Errorf("blob bytes not equal: got %v want %v", b.Blob, payload)
			}
			found = true
		}
	}
	if !found {
		t.Error("messages/v1 blob not found in GetTuple response")
	}
}

func TestStore_GetTuple_NoCheckpoint_ReturnsNil(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	threadID := "22222222-2222-2222-2222-222222222222"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	tuple, err := store.GetTuple(ctx, threadID, "", "")
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if tuple != nil {
		t.Errorf("expected nil tuple for thread with no checkpoints, got %+v", tuple)
	}
}

func TestStore_List_Pagination(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	threadID := "33333333-3333-3333-3333-333333333333"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	for i := 1; i <= 3; i++ {
		cid := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
		if err := store.Put(ctx, checkpointer.PutInput{
			ThreadID:       threadID,
			CheckpointNS:   "",
			CheckpointID:   cid,
			CheckpointJSON: []byte(`{"v":1,"id":"` + cid + `"}`),
			MetadataJSON:   []byte(`{}`),
		}); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	limit := int64(2)
	tuples, err := store.List(ctx, threadID, "", "", &limit, nil)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(tuples) != 2 {
		t.Errorf("List with limit=2: got %d results want 2", len(tuples))
	}
	// Descending order: latest checkpoint_id first.
	if tuples[0].CheckpointID < tuples[1].CheckpointID {
		t.Errorf("List not in DESC order: %q < %q", tuples[0].CheckpointID, tuples[1].CheckpointID)
	}
}

func TestStore_PutWrites_AppearOnGetTuple(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	threadID := "44444444-4444-4444-4444-444444444444"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	cid := "00000000-0000-0000-0000-000000000001"
	if err := store.Put(ctx, checkpointer.PutInput{
		ThreadID:       threadID,
		CheckpointID:   cid,
		CheckpointJSON: []byte(`{"v":1}`),
		MetadataJSON:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	taskID := "55555555-5555-5555-5555-555555555555"
	if err := store.PutWrites(ctx, checkpointer.PutWritesInput{
		ThreadID:     threadID,
		CheckpointID: cid,
		TaskID:       taskID,
		TaskPath:     "node/branch",
		Writes: []checkpointer.WriteInput{
			{Idx: 0, Channel: "out", Encoding: "json", Blob: []byte{0x01, 0x02}},
			{Idx: 1, Channel: "out", Encoding: "json", Blob: []byte{0x03, 0x04}},
		},
	}); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	tuple, err := store.GetTuple(ctx, threadID, "", "")
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if len(tuple.Writes) != 2 {
		t.Fatalf("Writes len = %d, want 2", len(tuple.Writes))
	}
	for _, w := range tuple.Writes {
		if w.TaskPath != "node/branch" {
			t.Errorf("TaskPath = %q, want %q", w.TaskPath, "node/branch")
		}
	}
}

func TestStore_CopyThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	srcID := "66666666-6666-6666-6666-666666666661"
	dstID := "66666666-6666-6666-6666-666666666662"
	testdb.MustInsertThread(t, ctx, pool, srcID, nil)
	testdb.MustInsertThread(t, ctx, pool, dstID, nil)

	payloads := [][]byte{{0x01, 0x02}, {0x03, 0x04}}
	for i, payload := range payloads {
		cid := fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1)
		if err := store.Put(ctx, checkpointer.PutInput{
			ThreadID:       srcID,
			CheckpointID:   cid,
			CheckpointJSON: []byte(`{"v":1,"id":"` + cid + `"}`),
			MetadataJSON:   []byte(`{}`),
			Blobs: []checkpointer.BlobInput{
				{Channel: "ch", Version: fmt.Sprintf("%d", i+1), Encoding: "json", Blob: payload},
			},
		}); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if err := store.CopyThread(ctx, srcID, dstID); err != nil {
		t.Fatalf("CopyThread: %v", err)
	}

	limit := int64(10)
	tuples, err := store.List(ctx, dstID, "", "", &limit, nil)
	if err != nil {
		t.Fatalf("List dst: %v", err)
	}
	if len(tuples) != 2 {
		t.Errorf("expected 2 checkpoints in dst after copy, got %d", len(tuples))
	}

	tuple, err := store.GetTuple(ctx, dstID, "", "")
	if err != nil {
		t.Fatalf("GetTuple dst: %v", err)
	}
	if len(tuple.Blobs) == 0 {
		t.Error("no blobs in copied tuple")
	}
}

func TestStore_DeleteThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	threadID := "77777777-7777-7777-7777-777777777777"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	cid := "00000000-0000-0000-0000-000000000001"
	if err := store.Put(ctx, checkpointer.PutInput{
		ThreadID:       threadID,
		CheckpointID:   cid,
		CheckpointJSON: []byte(`{"v":1}`),
		MetadataJSON:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := store.DeleteThread(ctx, threadID); err != nil {
		t.Fatalf("DeleteThread: %v", err)
	}

	limit := int64(10)
	tuples, err := store.List(ctx, threadID, "", "", &limit, nil)
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	if len(tuples) != 0 {
		t.Errorf("expected 0 tuples after DeleteThread, got %d", len(tuples))
	}
}

// ── Task 6: state_updated_at bump ─────────────────────────────────────────

func TestPut_BumpsThreadStateUpdatedAt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	threadID := "99999999-9999-9999-9999-999999999901"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	// Freshly inserted thread should have state_updated_at = NULL.
	var stateUpdatedAt *string
	if err := pool.QueryRow(ctx,
		`SELECT state_updated_at::text FROM thread WHERE thread_id = $1::uuid`,
		threadID,
	).Scan(&stateUpdatedAt); err != nil {
		t.Fatalf("initial query: %v", err)
	}
	if stateUpdatedAt != nil {
		t.Fatalf("state_updated_at before Put = %v, want nil", *stateUpdatedAt)
	}

	cid := "00000000-0000-0000-0000-999999999901"
	if err := store.Put(ctx, checkpointer.PutInput{
		ThreadID:       threadID,
		CheckpointNS:   "",
		CheckpointID:   cid,
		CheckpointJSON: []byte(`{"v":1,"id":"` + cid + `"}`),
		MetadataJSON:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// After Put, state_updated_at should be set.
	if err := pool.QueryRow(ctx,
		`SELECT state_updated_at::text FROM thread WHERE thread_id = $1::uuid`,
		threadID,
	).Scan(&stateUpdatedAt); err != nil {
		t.Fatalf("post-Put query: %v", err)
	}
	if stateUpdatedAt == nil {
		t.Fatal("state_updated_at is still nil after Put, want non-nil")
	}
}

func TestPutWrites_BumpsThreadStateUpdatedAt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	threadID := "99999999-9999-9999-9999-999999999902"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	// Seed a checkpoint first (PutWrites requires an existing checkpoint row).
	cid := "00000000-0000-0000-0000-999999999902"
	if err := store.Put(ctx, checkpointer.PutInput{
		ThreadID:       threadID,
		CheckpointNS:   "",
		CheckpointID:   cid,
		CheckpointJSON: []byte(`{"v":1}`),
		MetadataJSON:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Put (seed): %v", err)
	}

	// Clear state_updated_at so we can re-check it after PutWrites.
	if _, err := pool.Exec(ctx,
		`UPDATE thread SET state_updated_at = NULL WHERE thread_id = $1::uuid`,
		threadID,
	); err != nil {
		t.Fatalf("clear state_updated_at: %v", err)
	}

	taskID := "55555555-5555-5555-5555-999999999902"
	if err := store.PutWrites(ctx, checkpointer.PutWritesInput{
		ThreadID:     threadID,
		CheckpointID: cid,
		TaskID:       taskID,
		TaskPath:     "node/test",
		Writes: []checkpointer.WriteInput{
			{Idx: 0, Channel: "out", Encoding: "json", Blob: []byte(`"hello"`)},
		},
	}); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	var stateUpdatedAt *string
	if err := pool.QueryRow(ctx,
		`SELECT state_updated_at::text FROM thread WHERE thread_id = $1::uuid`,
		threadID,
	).Scan(&stateUpdatedAt); err != nil {
		t.Fatalf("post-PutWrites query: %v", err)
	}
	if stateUpdatedAt == nil {
		t.Fatal("state_updated_at is nil after PutWrites, want non-nil")
	}
}

func TestCopyThread_BumpsDestinationStateUpdatedAt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	srcID := "99999999-9999-9999-9999-999999999903"
	dstID := "99999999-9999-9999-9999-999999999904"
	testdb.MustInsertThread(t, ctx, pool, srcID, nil)
	testdb.MustInsertThread(t, ctx, pool, dstID, nil)

	// Seed a checkpoint on the source thread.
	cid := "00000000-0000-0000-0000-999999999903"
	if err := store.Put(ctx, checkpointer.PutInput{
		ThreadID:       srcID,
		CheckpointNS:   "",
		CheckpointID:   cid,
		CheckpointJSON: []byte(`{"v":1}`),
		MetadataJSON:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Put (seed): %v", err)
	}

	// Clear state_updated_at on destination before the copy.
	if _, err := pool.Exec(ctx,
		`UPDATE thread SET state_updated_at = NULL WHERE thread_id = $1::uuid`,
		dstID,
	); err != nil {
		t.Fatalf("clear state_updated_at: %v", err)
	}

	if err := store.CopyThread(ctx, srcID, dstID); err != nil {
		t.Fatalf("CopyThread: %v", err)
	}

	var stateUpdatedAt *string
	if err := pool.QueryRow(ctx,
		`SELECT state_updated_at::text FROM thread WHERE thread_id = $1::uuid`,
		dstID,
	).Scan(&stateUpdatedAt); err != nil {
		t.Fatalf("post-CopyThread query: %v", err)
	}
	if stateUpdatedAt == nil {
		t.Fatal("state_updated_at is nil on destination after CopyThread, want non-nil")
	}
}

func TestStore_Prune_KeepLatest(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	threadID := "88888888-8888-8888-8888-888888888888"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	for i := 1; i <= 3; i++ {
		cid := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
		if err := store.Put(ctx, checkpointer.PutInput{
			ThreadID:       threadID,
			CheckpointID:   cid,
			CheckpointJSON: []byte(`{"v":1}`),
			MetadataJSON:   []byte(`{}`),
		}); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if err := store.Prune(ctx, []string{threadID}, checkpointer.PruneStrategyKeepLatest); err != nil {
		t.Fatalf("Prune: %v", err)
	}

	limit := int64(10)
	tuples, err := store.List(ctx, threadID, "", "", &limit, nil)
	if err != nil {
		t.Fatalf("List after prune: %v", err)
	}
	if len(tuples) != 1 {
		t.Errorf("expected 1 checkpoint after KEEP_LATEST prune, got %d", len(tuples))
	}
}

// ── Task 1: checkpointer round-trip parity tests ────────────────────────────

// TestStore_Put_ParentAndRunID verifies that parent_checkpoint_id and run_id
// are stored and returned correctly (gaps C4+C5).
func TestStore_Put_ParentAndRunID(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	threadID := "aaaabbbb-0001-0001-0001-000000000001"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	// run_id has a FK to the run table; create a real run first.
	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "test_graph", nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")

	parentID := "aaaabbbb-0001-0001-0001-000000000002"
	cid := "aaaabbbb-0001-0001-0001-000000000003"

	if err := store.Put(ctx, checkpointer.PutInput{
		ThreadID:           threadID,
		CheckpointNS:       "",
		CheckpointID:       cid,
		ParentCheckpointID: parentID,
		RunID:              runID,
		CheckpointJSON:     []byte(`{"v":1,"id":"` + cid + `","channel_versions":{},"versions_seen":{},"updated_channels":[]}`),
		MetadataJSON:       []byte(`{"source":"loop","step":1,"parents":{}}`),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	var gotParent, gotRun *string
	if err := pool.QueryRow(ctx,
		`SELECT parent_checkpoint_id::text, run_id::text FROM checkpoints WHERE checkpoint_id = $1::uuid`,
		cid,
	).Scan(&gotParent, &gotRun); err != nil {
		t.Fatalf("query: %v", err)
	}
	if gotParent == nil || *gotParent != parentID {
		t.Errorf("parent_checkpoint_id = %v, want %q", gotParent, parentID)
	}
	if gotRun == nil || *gotRun != runID {
		t.Errorf("run_id = %v, want %q", gotRun, runID)
	}
}

// TestStore_DeleteForRuns_FindsWrittenRunID tests that DeleteForRuns deletes
// a checkpoint whose run_id was properly written by Put (gap C5).
func TestStore_DeleteForRuns_FindsWrittenRunID(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	threadID := "aaaabbbb-0002-0002-0002-000000000001"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	// run_id has a FK to the run table; create a real run first.
	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "test_graph", nil)
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")

	cid := "aaaabbbb-0002-0002-0002-000000000002"

	if err := store.Put(ctx, checkpointer.PutInput{
		ThreadID:       threadID,
		CheckpointID:   cid,
		RunID:          runID,
		CheckpointJSON: []byte(`{"v":1}`),
		MetadataJSON:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := store.DeleteForRuns(ctx, []string{runID}); err != nil {
		t.Fatalf("DeleteForRuns: %v", err)
	}

	limit := int64(10)
	tuples, err := store.List(ctx, threadID, "", "", &limit, nil)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(tuples) != 0 {
		t.Errorf("expected 0 checkpoints after DeleteForRuns, got %d", len(tuples))
	}
}

// TestStore_List_FilterJSON tests that metadata @> filter works (gap C7).
func TestStore_List_FilterJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	threadID := "aaaabbbb-0003-0003-0003-000000000001"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	for i, meta := range []string{
		`{"source":"input","step":0,"parents":{}}`,
		`{"source":"loop","step":1,"parents":{}}`,
		`{"source":"loop","step":2,"parents":{}}`,
	} {
		cid := fmt.Sprintf("aaaabbbb-0003-0003-0003-%012d", i+1)
		if err := store.Put(ctx, checkpointer.PutInput{
			ThreadID:       threadID,
			CheckpointID:   cid,
			CheckpointJSON: []byte(`{"v":1}`),
			MetadataJSON:   []byte(meta),
		}); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// filter to "source":"loop" only → should return 2
	limit := int64(10)
	tuples, err := store.List(ctx, threadID, "", "", &limit, []byte(`{"source":"loop"}`))
	if err != nil {
		t.Fatalf("List with filter: %v", err)
	}
	if len(tuples) != 2 {
		t.Errorf("expected 2 tuples with source=loop filter, got %d", len(tuples))
	}

	// filter to "source":"input" → should return 1
	tuples2, err := store.List(ctx, threadID, "", "", &limit, []byte(`{"source":"input"}`))
	if err != nil {
		t.Fatalf("List with input filter: %v", err)
	}
	if len(tuples2) != 1 {
		t.Errorf("expected 1 tuple with source=input filter, got %d", len(tuples2))
	}
}

// TestStore_CopyThread_MetadataThreadIDPatched tests that CopyThread patches
// metadata.thread_id and preserves run_id (gap C11).
func TestStore_CopyThread_MetadataThreadIDPatched(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	srcID := "aaaabbbb-0004-0004-0004-000000000001"
	dstID := "aaaabbbb-0004-0004-0004-000000000002"
	testdb.MustInsertThread(t, ctx, pool, srcID, nil)
	testdb.MustInsertThread(t, ctx, pool, dstID, nil)

	// run_id has a FK to the run table; create a real run first.
	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "test_graph", nil)
	runID := testdb.MustInsertRun(t, ctx, pool, srcID, assistantID, "running")
	cid := "aaaabbbb-0004-0004-0004-000000000004"

	if err := store.Put(ctx, checkpointer.PutInput{
		ThreadID:       srcID,
		CheckpointID:   cid,
		RunID:          runID,
		CheckpointJSON: []byte(`{"v":1}`),
		MetadataJSON:   []byte(`{"thread_id":"` + srcID + `","source":"loop","step":1,"parents":{}}`),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := store.CopyThread(ctx, srcID, dstID); err != nil {
		t.Fatalf("CopyThread: %v", err)
	}

	// Check metadata was patched and run_id preserved on copied checkpoint
	var metaJSON string
	var copiedRunID *string
	if err := pool.QueryRow(ctx,
		`SELECT metadata::text, run_id::text FROM checkpoints WHERE thread_id = $1::uuid AND checkpoint_id = $2::uuid`,
		dstID, cid,
	).Scan(&metaJSON, &copiedRunID); err != nil {
		t.Fatalf("query copied checkpoint: %v", err)
	}

	// Verify run_id preserved
	if copiedRunID == nil || *copiedRunID != runID {
		t.Errorf("run_id = %v, want %q", copiedRunID, runID)
	}

	// Verify metadata.thread_id patched to dstID
	var meta map[string]any
	if err := json.Unmarshal([]byte(metaJSON), &meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if tidVal, ok := meta["thread_id"]; !ok {
		t.Error("metadata.thread_id missing after CopyThread")
	} else if tidStr, ok := tidVal.(string); !ok || tidStr != dstID {
		t.Errorf("metadata.thread_id = %v, want %q", tidVal, dstID)
	}
}
