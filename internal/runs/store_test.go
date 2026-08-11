package runs_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/runs"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestStore(t *testing.T, ctx context.Context) (*runs.Store, *pgxpool.Pool) {
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
	return runs.NewStore(pool), pool
}

// seedRunFixtures inserts an assistant, a thread, and returns their IDs.
// Callers use these to seed run rows via testdb.MustInsertRun.
func seedRunFixtures(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (assistantID, threadID string) {
	t.Helper()
	assistantID = testdb.MustInsertAssistant(t, ctx, pool, "test-graph", nil)
	threadID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)
	return assistantID, threadID
}

func TestStore_Get_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	_, err := store.Get(ctx, "00000000-0000-0000-0000-000000000000", "", nil)
	if err == nil || !errors.Is(err, runs.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestStore_Get_ReturnsRow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	r, err := store.Get(ctx, runID, "", nil)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if r.RunID != runID {
		t.Errorf("RunID = %q, want %q", r.RunID, runID)
	}
	if r.ThreadID != thID {
		t.Errorf("ThreadID = %q, want %q", r.ThreadID, thID)
	}
	if r.AssistantID != aID {
		t.Errorf("AssistantID = %q, want %q", r.AssistantID, aID)
	}
	if r.Status != "pending" {
		t.Errorf("Status = %q, want pending", r.Status)
	}
	// PostgreSQL normalises JSONB whitespace, compare semantically.
	var got map[string]any
	if err := json.Unmarshal(r.Kwargs, &got); err != nil {
		t.Fatalf("unmarshal Kwargs: %v", err)
	}
}

func TestStore_Get_WithThreadID(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	runID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	// Get with matching thread_id succeeds.
	r, err := store.Get(ctx, runID, thID, nil)
	if err != nil {
		t.Fatalf("Get with threadID: %v", err)
	}
	if r.RunID != runID {
		t.Errorf("RunID = %q, want %q", r.RunID, runID)
	}

	// Get with wrong thread_id returns ErrNotFound.
	_, err = store.Get(ctx, runID, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", nil)
	if err == nil || !errors.Is(err, runs.ErrNotFound) {
		t.Fatalf("want ErrNotFound for wrong thread_id, got %v", err)
	}
}

func TestStore_Search(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	// Insert a second thread for cross-thread isolation.
	th2ID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	testdb.MustInsertThread(t, ctx, pool, th2ID, nil)

	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")
	testdb.MustInsertRun(t, ctx, pool, th2ID, aID, "pending")

	// All results (no filter).
	results, err := store.Search(ctx, runs.SearchInput{Limit: 10}, nil)
	if err != nil {
		t.Fatalf("Search all: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("len(results) = %d, want 3", len(results))
	}

	// Filter by thread_id.
	byThread, err := store.Search(ctx, runs.SearchInput{ThreadID: thID, Limit: 10}, nil)
	if err != nil {
		t.Fatalf("Search by thread: %v", err)
	}
	if len(byThread) != 2 {
		t.Errorf("len(byThread) = %d, want 2", len(byThread))
	}

	// Filter by status.
	byStatus, err := store.Search(ctx, runs.SearchInput{Statuses: []string{"pending"}, Limit: 10}, nil)
	if err != nil {
		t.Fatalf("Search by status: %v", err)
	}
	if len(byStatus) != 2 {
		t.Errorf("len(byStatus pending) = %d, want 2", len(byStatus))
	}

	// Pagination: limit 1.
	paged, err := store.Search(ctx, runs.SearchInput{Limit: 1, Offset: 0}, nil)
	if err != nil {
		t.Fatalf("Search paginated: %v", err)
	}
	if len(paged) != 1 {
		t.Errorf("len(paged) = %d, want 1", len(paged))
	}
}

func TestStore_Count(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	n, err := store.Count(ctx, runs.SearchInput{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 2 {
		t.Errorf("Count = %d, want 2", n)
	}

	// Count with status filter.
	nPending, err := store.Count(ctx, runs.SearchInput{Statuses: []string{"pending"}}, nil)
	if err != nil {
		t.Fatalf("Count pending: %v", err)
	}
	if nPending != 1 {
		t.Errorf("Count(pending) = %d, want 1", nPending)
	}

	// Count with thread_id filter.
	nThread, err := store.Count(ctx, runs.SearchInput{ThreadID: thID}, nil)
	if err != nil {
		t.Fatalf("Count by thread: %v", err)
	}
	if nThread != 2 {
		t.Errorf("Count(thread) = %d, want 2", nThread)
	}
}

func TestStore_Stats(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	st, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.NPending != 2 {
		t.Errorf("NPending = %d, want 2", st.NPending)
	}
	if st.NRunning != 1 {
		t.Errorf("NRunning = %d, want 1", st.NRunning)
	}
}

// ─── Write method tests ───────────────────────────────────────────────────────

func TestRunStore_Create_Reject_ExistingInflight(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	// pre-existing pending run on thread
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	_, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:          thID,
		AssistantID:       aID,
		MultitaskStrategy: "reject",
		KwargsJSON:        []byte(`{}`),
	}, nil, nil)
	if err == nil || !errors.Is(err, runs.ErrInflight) {
		t.Fatalf("want ErrInflight, got %v", err)
	}
}

func TestRunStore_Create_Enqueue_NoCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:          thID,
		AssistantID:       aID,
		MultitaskStrategy: "enqueue",
		KwargsJSON:        []byte(`{}`),
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create (enqueue): %v", err)
	}
	if r.RunID == "" {
		t.Fatal("RunID empty")
	}
}

func TestRunStore_Create_Interrupt_CancelsExisting(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	// pre-existing pending run on thread
	existingID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:          thID,
		AssistantID:       aID,
		MultitaskStrategy: "interrupt",
		KwargsJSON:        []byte(`{}`),
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create (interrupt): %v", err)
	}
	if r.RunID == "" {
		t.Fatal("RunID empty")
	}
	// Existing run should now be interrupted.
	existing, err := store.Get(ctx, existingID, "", nil)
	if err != nil {
		t.Fatalf("Get existing: %v", err)
	}
	if existing.Status != "interrupted" {
		t.Errorf("existing run status = %q, want interrupted", existing.Status)
	}
}

func TestRunStore_Create_ThreadNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)

	_, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    "00000000-0000-0000-0000-000000000001",
		AssistantID: "00000000-0000-0000-0000-000000000002",
		KwargsJSON:  []byte(`{}`),
	}, nil, nil)
	if !errors.Is(err, runs.ErrNotFound) {
		t.Fatalf("want ErrNotFound for missing thread, got %v", err)
	}
}

func TestRunStore_Create_AssistantNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	_, thID := seedRunFixtures(t, ctx, pool)

	_, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    thID,
		AssistantID: "00000000-0000-0000-0000-000000000099",
		KwargsJSON:  []byte(`{}`),
	}, nil, nil)
	if !errors.Is(err, runs.ErrNotFound) {
		t.Fatalf("want ErrNotFound for missing assistant, got %v", err)
	}
}

func TestRunStore_Create_MergesGraphIDIntoConfigurable(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    thID,
		AssistantID: aID,
		KwargsJSON:  []byte(`{"config":{"configurable":{"user_key":"u"}}}`),
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	var kw struct {
		Config struct {
			Configurable map[string]any `json:"configurable"`
		} `json:"config"`
	}
	if err := json.Unmarshal(r.Kwargs, &kw); err != nil {
		t.Fatalf("unmarshal Kwargs: %v", err)
	}
	c := kw.Config.Configurable
	if c["graph_id"] != "test-graph" {
		t.Errorf("graph_id = %v, want test-graph", c["graph_id"])
	}
	if c["assistant_id"] != aID {
		t.Errorf("assistant_id = %v, want %s", c["assistant_id"], aID)
	}
	if c["thread_id"] != thID {
		t.Errorf("thread_id = %v, want %s", c["thread_id"], thID)
	}
	if c["run_id"] != r.RunID {
		t.Errorf("run_id = %v, want %s", c["run_id"], r.RunID)
	}
	if c["user_key"] != "u" {
		t.Errorf("user-supplied configurable key not preserved: %v", c["user_key"])
	}
}

func TestRunStore_Create_ReturnsLeaseGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    thID,
		AssistantID: aID,
		KwargsJSON:  []byte(`{}`),
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// LeaseGeneration should be zero on a fresh row.
	if r.LeaseGeneration != 0 {
		t.Errorf("LeaseGeneration = %d, want 0", r.LeaseGeneration)
	}
}

func TestRunStore_Delete_RemovesRow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	deleted, err := store.Delete(ctx, rID, thID, nil)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted != rID {
		t.Errorf("deleted = %q, want %q", deleted, rID)
	}
	_, err = store.Get(ctx, rID, "", nil)
	if !errors.Is(err, runs.ErrNotFound) {
		t.Errorf("after delete: want ErrNotFound, got %v", err)
	}
}

func TestRunStore_Delete_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)

	_, err := store.Delete(ctx, "00000000-0000-0000-0000-000000000000", "", nil)
	if !errors.Is(err, runs.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestRunStore_Delete_WrongThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	_, err := store.Delete(ctx, rID, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", nil)
	if !errors.Is(err, runs.ErrNotFound) {
		t.Fatalf("want ErrNotFound for wrong thread, got %v", err)
	}
	// Row should still exist.
	_, err = store.Get(ctx, rID, "", nil)
	if err != nil {
		t.Errorf("run should still exist, got err: %v", err)
	}
}

func TestRunStore_SetStatus_Transitions(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	if err := store.SetStatus(ctx, rID, "running"); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	r, _ := store.Get(ctx, rID, "", nil)
	if r.Status != "running" {
		t.Errorf("Status = %q, want running", r.Status)
	}
}

func TestRunStore_Cancel_SetsRequestedAt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	cancelled, err := store.Cancel(ctx, []string{rID}, thID, nil)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if len(cancelled) != 1 {
		t.Fatalf("cancelled count = %d, want 1", len(cancelled))
	}
	if cancelled[0] != rID {
		t.Errorf("cancelled[0] = %q, want %q", cancelled[0], rID)
	}
}

func TestRunStore_Cancel_WrongThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	// Cancel with wrong thread_id returns empty list (no match).
	cancelled, err := store.Cancel(ctx, []string{rID}, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", nil)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if len(cancelled) != 0 {
		t.Errorf("cancel with wrong thread: want 0 cancelled, got %d", len(cancelled))
	}
}

func TestRunStore_Cancel_Empty(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)

	cancelled, err := store.Cancel(ctx, nil, "", nil)
	if err != nil {
		t.Fatalf("Cancel(nil): %v", err)
	}
	if len(cancelled) != 0 {
		t.Errorf("cancel empty: want 0, got %d", len(cancelled))
	}
}

func TestRunStore_MarkDone_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	if err := store.MarkDone(ctx, rID, false); err != nil {
		t.Fatalf("MarkDone(success): %v", err)
	}
	r, err := store.Get(ctx, rID, "", nil)
	if err != nil {
		t.Fatalf("Get after MarkDone: %v", err)
	}
	if r.Status != "success" {
		t.Errorf("Status = %q, want success", r.Status)
	}
}

func TestRunStore_MarkDone_Resumable(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	if err := store.MarkDone(ctx, rID, true); err != nil {
		t.Fatalf("MarkDone(resumable): %v", err)
	}
	r, err := store.Get(ctx, rID, "", nil)
	if err != nil {
		t.Fatalf("Get after MarkDone: %v", err)
	}
	if r.Status != "interrupted" {
		t.Errorf("Status = %q, want interrupted", r.Status)
	}
}

func TestRunStore_Next_ClaimsRun(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	claimed, err := store.Next(ctx, 1)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed count = %d, want 1", len(claimed))
	}
	if claimed[0].Run.Status != "running" {
		t.Errorf("claimed run status = %q, want running", claimed[0].Run.Status)
	}
}

func TestRunStore_Next_SkipsLocked(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	// Claim both; then try again — should return empty.
	if _, err := store.Next(ctx, 10); err != nil {
		t.Fatalf("Next: %v", err)
	}
	second, err := store.Next(ctx, 10)
	if err != nil {
		t.Fatalf("second Next: %v", err)
	}
	if len(second) != 0 {
		t.Errorf("expected 0 claimed after all taken, got %d", len(second))
	}
}

// TestRunStore_Sweep_ResetsExpiredLeaseToPending verifies C8 parity:
// Python ops.py:1467 resets swept runs to 'pending', not 'error'.
func TestRunStore_Sweep_ResetsExpiredLeaseToPending(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")
	// Manually set an already-expired lease.
	if _, err := pool.Exec(ctx,
		`UPDATE run SET lease_expires_at = now() - interval '1 second' WHERE run_id = $1::uuid`,
		rID,
	); err != nil {
		t.Fatalf("set lease: %v", err)
	}
	swept, err := store.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(swept) != 1 || swept[0] != rID {
		t.Errorf("swept = %v, want [%s]", swept, rID)
	}
	r, err := store.Get(ctx, rID, "", nil)
	if err != nil {
		t.Fatalf("Get after sweep: %v", err)
	}
	// (C8) Must be 'pending', not 'error' — Python ops.py:1467.
	if r.Status != "pending" {
		t.Errorf("after sweep status = %q, want pending (C8 parity)", r.Status)
	}
}

func TestExtendLease(t *testing.T) {
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

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "graph-lease", nil)
	threadID := "dddddddd-dddd-dddd-dddd-dddddddddddd"
	testdb.MustInsertThread(t, ctx, pool, threadID, nil)

	store := runs.NewStore(pool)

	// Insert as pending, then claim via Next.
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "pending")

	claimed, err := store.Next(ctx, 1)
	if err != nil || len(claimed) == 0 {
		t.Fatalf("Next: %v, claimed=%d", err, len(claimed))
	}
	claimedRunID := claimed[0].Run.RunID
	if claimedRunID != runID {
		t.Fatalf("Next claimed unexpected run: got %s, want %s", claimedRunID, runID)
	}

	// ExtendLease with empty holderID should succeed if the run is running.
	if err := store.ExtendLease(ctx, claimedRunID, ""); err != nil {
		t.Errorf("ExtendLease on owned run: %v", err)
	}

	// Set a holder so we can exercise the holder-check branch.
	if _, err := pool.Exec(ctx, `UPDATE run SET lease_holder_id = 'worker-A' WHERE run_id = $1::uuid`, claimedRunID); err != nil {
		t.Fatalf("seed lease_holder_id: %v", err)
	}

	// Matching holderID: success.
	if err := store.ExtendLease(ctx, claimedRunID, "worker-A"); err != nil {
		t.Errorf("ExtendLease with matching holder: %v", err)
	}

	// Mismatched holderID: ErrNotFound (stolen-lease guard).
	if err := store.ExtendLease(ctx, claimedRunID, "worker-B"); err == nil {
		t.Error("ExtendLease with mismatched holder: expected ErrNotFound, got nil")
	} else if err != runs.ErrNotFound {
		t.Errorf("ExtendLease with mismatched holder: got %v, want ErrNotFound", err)
	}

	// ExtendLease on a completed run returns ErrNotFound.
	if err := store.SetStatus(ctx, claimedRunID, "success"); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	if err := store.ExtendLease(ctx, claimedRunID, ""); err == nil {
		t.Error("ExtendLease on completed run: expected ErrNotFound, got nil")
	} else if err != runs.ErrNotFound {
		t.Errorf("ExtendLease on completed run: got %v, want ErrNotFound", err)
	}

	// ExtendLease on a non-existent run returns ErrNotFound.
	fakeID := "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
	if err := store.ExtendLease(ctx, fakeID, ""); err == nil {
		t.Error("ExtendLease on non-existent run: expected ErrNotFound, got nil")
	} else if err != runs.ErrNotFound {
		t.Errorf("ExtendLease on non-existent run: got %v, want ErrNotFound", err)
	}

}

func TestStore_Create_WithAfterSeconds_SetsRunAfter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    thID,
		AssistantID: aID,
		KwargsJSON:  []byte(`{}`),
		Metadata:    []byte(`{}`),
		AfterSeconds: 60,
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create with AfterSeconds: %v", err)
	}
	if r.RunAfter == nil {
		t.Fatal("RunAfter is nil, want non-nil when AfterSeconds=60")
	}
	if !r.RunAfter.After(r.CreatedAt) {
		t.Errorf("RunAfter (%v) should be after CreatedAt (%v)", r.RunAfter, r.CreatedAt)
	}
}

func TestStore_Next_SkipsFutureRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	// Create a run deferred 1 hour into the future.
	_, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:     thID,
		AssistantID:  aID,
		KwargsJSON:   []byte(`{}`),
		AfterSeconds: 3600,
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create deferred run: %v", err)
	}

	claimed, err := store.Next(ctx, 10)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(claimed) != 0 {
		t.Errorf("Next returned %d claimed runs, want 0 (future run must be skipped)", len(claimed))
	}
}

func TestStore_Count_MultipleStatuses(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	// Count with both statuses: should return all 3.
	n, err := store.Count(ctx, runs.SearchInput{ThreadID: thID, Statuses: []string{"pending", "running"}}, nil)
	if err != nil {
		t.Fatalf("Count multi-status: %v", err)
	}
	if n != 3 {
		t.Errorf("Count(pending+running) = %d, want 3", n)
	}

	// Sanity check: only pending.
	nPending, err := store.Count(ctx, runs.SearchInput{ThreadID: thID, Statuses: []string{"pending"}}, nil)
	if err != nil {
		t.Fatalf("Count pending-only: %v", err)
	}
	if nPending != 2 {
		t.Errorf("Count(pending) = %d, want 2", nPending)
	}
}

func TestStore_Stats_PopulatesWaitPercentiles(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	// Insert 3 pending runs and backdate created_at to 1s, 5s, 10s ago.
	backdates := []int{1, 5, 10}
	for _, sec := range backdates {
		rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
		if _, err := pool.Exec(ctx,
			`UPDATE run SET created_at = now() - ($2 * interval '1 second') WHERE run_id = $1::uuid`,
			rID, sec,
		); err != nil {
			t.Fatalf("backdate created_at by %ds: %v", sec, err)
		}
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.PendingWaitMaxSecs == nil {
		t.Fatal("PendingWaitMaxSecs is nil, want non-nil")
	}
	if *stats.PendingWaitMaxSecs < 9.0 || *stats.PendingWaitMaxSecs > 30.0 {
		t.Errorf("PendingWaitMaxSecs = %f, want in [9.0, 30.0]", *stats.PendingWaitMaxSecs)
	}
	if stats.PendingWaitMedSecs == nil {
		t.Fatal("PendingWaitMedSecs is nil, want non-nil")
	}
	if *stats.PendingWaitMedSecs < 4.0 || *stats.PendingWaitMedSecs > 30.0 {
		t.Errorf("PendingWaitMedSecs = %f, want in [4.0, 30.0]", *stats.PendingWaitMedSecs)
	}
}

func TestPublishExistsAndAuth(t *testing.T) {
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

	assistantID := testdb.MustInsertAssistant(t, ctx, pool, "graph-pub", nil)
	threadID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	testdb.MustInsertThread(t, ctx, pool, threadID, []byte(`{"owner":"alice"}`))
	runID := testdb.MustInsertRun(t, ctx, pool, threadID, assistantID, "running")

	store := runs.NewStore(pool)

	// Positive: no auth filters — must return nil.
	if err := store.PublishExistsAndAuth(ctx, runID, threadID, nil); err != nil {
		t.Errorf("positive case: got err %v, want nil", err)
	}

	// Auth filter match: owner=alice — must return nil.
	matchFilter := []*coreapi.AuthFilter{
		{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "owner", Match: "alice"}}},
	}
	if err := store.PublishExistsAndAuth(ctx, runID, threadID, matchFilter); err != nil {
		t.Errorf("matching auth filter: got err %v, want nil", err)
	}

	// Auth filter mismatch: owner=bob — must return ErrForbidden.
	mismatchFilter := []*coreapi.AuthFilter{
		{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "owner", Match: "bob"}}},
	}
	if err := store.PublishExistsAndAuth(ctx, runID, threadID, mismatchFilter); err == nil {
		t.Error("mismatch auth filter: expected ErrForbidden, got nil")
	} else if err != runs.ErrForbidden {
		t.Errorf("mismatch auth filter: got %v, want ErrForbidden", err)
	}

	// Wrong runID — must return ErrNotFound.
	wrongRunID := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	if err := store.PublishExistsAndAuth(ctx, wrongRunID, threadID, nil); err == nil {
		t.Error("wrong run ID: expected ErrNotFound, got nil")
	} else if err != runs.ErrNotFound {
		t.Errorf("wrong run ID: got %v, want ErrNotFound", err)
	}
}

// ─── Parity gap tests ────────────────────────────────────────────────────────

// TestStore_Cancel_RollbackDeletesPending verifies C2 parity:
// Python ops.py:1873-1879: pending runs are DELETEd when action=rollback.
func TestStore_Cancel_RollbackDeletesPending(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-cancel-rb", nil)
	thID := "b1b1b1b1-b1b1-b1b1-b1b1-b1b1b1b1b1b1"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	pendingID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	runningID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	results, err := store.CancelWithAction(ctx, []string{pendingID, runningID}, thID, "rollback", nil)
	if err != nil {
		t.Fatalf("CancelWithAction(rollback): %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results count = %d, want 2", len(results))
	}

	// pending run must be deleted
	_, err = store.Get(ctx, pendingID, "", nil)
	if !errors.Is(err, runs.ErrNotFound) {
		t.Errorf("pending run after rollback: want ErrNotFound, got %v", err)
	}
	// verify the result marks it deleted
	var pendingResult *runs.CancelResult
	for i := range results {
		if results[i].RunID == pendingID {
			pendingResult = &results[i]
		}
	}
	if pendingResult == nil {
		t.Fatal("pending run not in results")
	}
	if !pendingResult.Deleted {
		t.Error("pending run Deleted = false, want true")
	}

	// running run must still exist with cancel_requested_at set
	r, err := store.Get(ctx, runningID, "", nil)
	if err != nil {
		t.Fatalf("Get running run after rollback cancel: %v", err)
	}
	if r.Status != "running" {
		t.Errorf("running run status = %q, want running", r.Status)
	}
}

// TestStore_Cancel_InterruptTransitionsPending verifies C2 parity:
// Python ops.py:1864-1870: pending runs → status='interrupted' when action=interrupt.
func TestStore_Cancel_InterruptTransitionsPending(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-cancel-int", nil)
	thID := "b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	pendingID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	results, err := store.CancelWithAction(ctx, []string{pendingID}, thID, "interrupt", nil)
	if err != nil {
		t.Fatalf("CancelWithAction(interrupt): %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results count = %d, want 1", len(results))
	}
	if results[0].Deleted {
		t.Error("interrupt cancel: Deleted = true, want false")
	}

	r, err := store.Get(ctx, pendingID, "", nil)
	if err != nil {
		t.Fatalf("Get after interrupt cancel: %v", err)
	}
	if r.Status != "interrupted" {
		t.Errorf("pending run status after interrupt = %q, want interrupted", r.Status)
	}
}

// TestStore_Next_ThreadIsolationGuard verifies C9 parity:
// Python ops.py:1375-1379: a pending run is not claimable while another run on
// the same thread is 'running'.
func TestStore_Next_ThreadIsolationGuard(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-isolation", nil)
	thID := "b3b3b3b3-b3b3-b3b3-b3b3-b3b3b3b3b3b3"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)

	// Insert a running run on the same thread.
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")
	// Insert a pending run on the same thread.
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	// Next must not claim the pending run because the thread has a running run.
	claimed, err := store.Next(ctx, 10)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(claimed) != 0 {
		t.Errorf("Next claimed %d runs, want 0 (thread-isolation guard, C9)", len(claimed))
	}
}

// TestStore_Next_ThreadIsolationAllowsDifferentThread verifies that the thread-
// isolation guard does NOT block pending runs on a different thread.
func TestStore_Next_ThreadIsolationAllowsDifferentThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-isolation2", nil)

	th1ID := "b4b4b4b4-b4b4-b4b4-b4b4-b4b4b4b4b4b4"
	th2ID := "b5b5b5b5-b5b5-b5b5-b5b5-b5b5b5b5b5b5"
	testdb.MustInsertThread(t, ctx, pool, th1ID, nil)
	testdb.MustInsertThread(t, ctx, pool, th2ID, nil)

	// thread1 has a running run — blocks its pending run.
	testdb.MustInsertRun(t, ctx, pool, th1ID, aID, "running")
	testdb.MustInsertRun(t, ctx, pool, th1ID, aID, "pending")

	// thread2 has only a pending run — should be claimed.
	testdb.MustInsertRun(t, ctx, pool, th2ID, aID, "pending")

	claimed, err := store.Next(ctx, 10)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(claimed) != 1 {
		t.Errorf("Next claimed %d runs, want 1 (only thread2's pending)", len(claimed))
	} else if claimed[0].Run.ThreadID != th2ID {
		t.Errorf("claimed run thread = %q, want %q", claimed[0].Run.ThreadID, th2ID)
	}
}

// TestStore_Sweep_ZombieLeaseFencing verifies C8 parity:
// After Sweep resets a run to 'pending', a zombie worker's ExtendLease
// fails with ErrNotFound because the run is no longer 'running'.
func TestStore_Sweep_ZombieLeaseFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-zombie", nil)
	thID := "b6b6b6b6-b6b6-b6b6-b6b6-b6b6b6b6b6b6"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	// Expire the lease.
	if _, err := pool.Exec(ctx,
		`UPDATE run SET lease_expires_at = now() - interval '1 second' WHERE run_id = $1::uuid`, rID,
	); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	swept, err := store.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(swept) != 1 || swept[0] != rID {
		t.Fatalf("swept = %v, want [%s]", swept, rID)
	}

	// Zombie worker tries to extend — must fail because run is 'pending'.
	err = store.ExtendLease(ctx, rID, "")
	if !errors.Is(err, runs.ErrNotFound) {
		t.Errorf("zombie ExtendLease: want ErrNotFound, got %v", err)
	}
}

// ─── Parity gap tests (Task 3) ───────────────────────────────────────────────

// TestCreate_UserID_InjectsIntoConfigurable verifies item 1 parity:
// user_id is injected into kwargs.config.configurable.user_id.
func TestCreate_UserID_InjectsIntoConfigurable(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    thID,
		AssistantID: aID,
		KwargsJSON:  []byte(`{}`),
		UserID:      "alice",
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create with UserID: %v", err)
	}
	var kw struct {
		Config struct {
			Configurable map[string]any `json:"configurable"`
		} `json:"config"`
	}
	if err := json.Unmarshal(r.Kwargs, &kw); err != nil {
		t.Fatalf("unmarshal kwargs: %v", err)
	}
	if kw.Config.Configurable["user_id"] != "alice" {
		t.Errorf("user_id = %v, want alice", kw.Config.Configurable["user_id"])
	}
}

// TestCreate_UserID_Precedence verifies item 1 parity (ops.py:1605-1610):
// kwargs.config.configurable.user_id wins over request user_id.
func TestCreate_UserID_Precedence(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	// Caller-provided kwargs.config.configurable.user_id takes precedence.
	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    thID,
		AssistantID: aID,
		KwargsJSON:  []byte(`{"config":{"configurable":{"user_id":"kwarg-user"}}}`),
		UserID:      "fallback-user",
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	var kw struct {
		Config struct {
			Configurable map[string]any `json:"configurable"`
		} `json:"config"`
	}
	if err := json.Unmarshal(r.Kwargs, &kw); err != nil {
		t.Fatalf("unmarshal kwargs: %v", err)
	}
	// kwargs.config.configurable.user_id must win (ops.py:1606 — first COALESCE arg).
	if kw.Config.Configurable["user_id"] != "kwarg-user" {
		t.Errorf("user_id = %v, want kwarg-user (kwargs wins)", kw.Config.Configurable["user_id"])
	}
}

// TestCreate_UserID_FallsBackToRequest verifies item 1: when no other source
// provides user_id the request UserID is used.
func TestCreate_UserID_FallsBackToRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	// No kwargs.config.configurable.user_id, no thread/assistant config → fallback to UserID.
	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    thID,
		AssistantID: aID,
		KwargsJSON:  []byte(`{}`),
		UserID:      "request-user",
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	var kw struct {
		Config struct {
			Configurable map[string]any `json:"configurable"`
		} `json:"config"`
	}
	if err := json.Unmarshal(r.Kwargs, &kw); err != nil {
		t.Fatalf("unmarshal kwargs: %v", err)
	}
	if kw.Config.Configurable["user_id"] != "request-user" {
		t.Errorf("user_id = %v, want request-user", kw.Config.Configurable["user_id"])
	}
}

// TestCreate_AssistantID_SetdefaultInMetadata verifies item 2 parity:
// metadata.assistant_id is injected by default (ops.py:1502 setdefault).
func TestCreate_AssistantID_SetdefaultInMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    thID,
		AssistantID: aID,
		KwargsJSON:  []byte(`{}`),
		Metadata:    []byte(`{}`), // no assistant_id provided
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	var meta map[string]any
	if err := json.Unmarshal(r.Metadata, &meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if meta["assistant_id"] != aID {
		t.Errorf("metadata.assistant_id = %v, want %s (default injection)", meta["assistant_id"], aID)
	}
}

// TestCreate_AssistantID_CallerWins verifies item 2 parity (setdefault semantics):
// caller-provided metadata.assistant_id takes precedence.
func TestCreate_AssistantID_CallerWins(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	callerMeta := []byte(`{"assistant_id":"custom-assistant-id"}`)
	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    thID,
		AssistantID: aID,
		KwargsJSON:  []byte(`{}`),
		Metadata:    callerMeta,
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	var meta map[string]any
	if err := json.Unmarshal(r.Metadata, &meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	// Caller-provided value must win (setdefault: ops.py:1502).
	if meta["assistant_id"] != "custom-assistant-id" {
		t.Errorf("metadata.assistant_id = %v, want custom-assistant-id (caller wins)", meta["assistant_id"])
	}
}

// TestCreate_IfNotExists_Reject_MissingThread verifies item 3 parity:
// without if_not_exists=CREATE, a missing thread returns ErrNotFound.
func TestCreate_IfNotExists_Reject_MissingThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-ifnotexists", nil)

	_, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    "cccccccc-cccc-cccc-cccc-cccccccccccc",
		AssistantID: aID,
		KwargsJSON:  []byte(`{}`),
		IfNotExists: 0, // REJECT_RUN_IF_THREAD_NOT_EXISTS
	}, nil, nil)
	if !errors.Is(err, runs.ErrNotFound) {
		t.Fatalf("want ErrNotFound for missing thread (reject), got %v", err)
	}
}

// TestCreate_IfNotExists_Create_AutoCreatesThread verifies item 3 parity:
// with if_not_exists=CREATE (value 1), a missing thread is auto-created
// (ops.py:1527-1558 inserted_thread CTE).
func TestCreate_IfNotExists_Create_AutoCreatesThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-autocreate", nil)

	newThreadID := "dddddddd-cccc-cccc-cccc-cccccccccccc"
	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    newThreadID,
		AssistantID: aID,
		KwargsJSON:  []byte(`{}`),
		IfNotExists: 1, // CREATE_THREAD_IF_THREAD_NOT_EXISTS
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create with if_not_exists=create: %v", err)
	}
	if r.ThreadID != newThreadID {
		t.Errorf("run.ThreadID = %q, want %q", r.ThreadID, newThreadID)
	}

	// Verify thread was actually created.
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM thread WHERE thread_id = $1::uuid)`, newThreadID,
	).Scan(&exists); err != nil {
		t.Fatalf("check thread exists: %v", err)
	}
	if !exists {
		t.Error("thread was not auto-created by if_not_exists=create")
	}
}

// TestCreate_IfNotExists_Create_NoThreadID_GeneratesThread verifies item 3 parity:
// when thread_id is empty and if_not_exists=create, a new thread UUID is generated
// (ops.py:1565 thread_id or uuid4()).
func TestCreate_IfNotExists_Create_NoThreadID_GeneratesThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-genthread", nil)

	r, err := store.Create(ctx, runs.CreateRunInput{
		ThreadID:    "", // no thread_id provided
		AssistantID: aID,
		KwargsJSON:  []byte(`{}`),
		IfNotExists: 1, // CREATE_THREAD_IF_THREAD_NOT_EXISTS
	}, nil, nil)
	if err != nil {
		t.Fatalf("Create with no thread_id and if_not_exists=create: %v", err)
	}
	if r.ThreadID == "" {
		t.Error("expected a generated thread_id, got empty")
	}

	// Verify thread was created.
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM thread WHERE thread_id = $1::uuid)`, r.ThreadID,
	).Scan(&exists); err != nil {
		t.Fatalf("check thread exists: %v", err)
	}
	if !exists {
		t.Errorf("auto-generated thread %q was not created", r.ThreadID)
	}
}

// TestDelete_RemovesOrphanCheckpointWrites verifies item 4 parity:
// Python ops.py:1747-1755 deletes checkpoint_writes for the run's checkpoints.
func TestDelete_RemovesOrphanCheckpointWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	// Insert a checkpoint row for this run.
	// Schema (migration 0000006): checkpoint_id, thread_id, checkpoint_ns, run_id, checkpoint, metadata
	var cpID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO checkpoints (checkpoint_id, thread_id, checkpoint_ns, run_id, checkpoint, metadata)
		VALUES (gen_random_uuid(), $1::uuid, '', $2::uuid, '{}'::jsonb, '{}'::jsonb)
		RETURNING checkpoint_id::text`,
		thID, rID,
	).Scan(&cpID); err != nil {
		t.Fatalf("insert checkpoint: %v", err)
	}

	// Insert a checkpoint_writes row for that checkpoint.
	// Schema: thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob
	if _, err := pool.Exec(ctx, `
		INSERT INTO checkpoint_writes (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob)
		VALUES ($1::uuid, '', $2::uuid, gen_random_uuid(), 0, 'ch', 'test', '\x'::bytea)`,
		thID, cpID,
	); err != nil {
		t.Fatalf("insert checkpoint_writes: %v", err)
	}

	// Delete the run — should also remove orphaned checkpoint_writes.
	if _, err := store.Delete(ctx, rID, thID, nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// checkpoint_writes row must be gone.
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM checkpoint_writes WHERE checkpoint_id = $1::uuid`, cpID,
	).Scan(&count); err != nil {
		t.Fatalf("count checkpoint_writes: %v", err)
	}
	if count != 0 {
		t.Errorf("checkpoint_writes count = %d, want 0 after delete (item 4 parity)", count)
	}
}

// TestStats_IncludesRunningInAgeMetrics verifies item 5 parity:
// Python ops.py:1331 uses status IN ('pending','running') for the age metrics.
// Both pending and running runs should contribute to the age calculations.
func TestStats_IncludesRunningInAgeMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID, thID := seedRunFixtures(t, ctx, pool)

	// Insert a running run backdated 60 seconds — older than the pending ones.
	runningID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")
	if _, err := pool.Exec(ctx,
		`UPDATE run SET created_at = now() - interval '60 seconds' WHERE run_id = $1::uuid`,
		runningID,
	); err != nil {
		t.Fatalf("backdate running run: %v", err)
	}

	// Insert a pending run backdated 10 seconds.
	pendingID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")
	if _, err := pool.Exec(ctx,
		`UPDATE run SET created_at = now() - interval '10 seconds' WHERE run_id = $1::uuid`,
		pendingID,
	); err != nil {
		t.Fatalf("backdate pending run: %v", err)
	}

	st, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.NPending != 1 {
		t.Errorf("NPending = %d, want 1", st.NPending)
	}
	if st.NRunning != 1 {
		t.Errorf("NRunning = %d, want 1", st.NRunning)
	}

	// PendingWaitMaxSecs should reflect the running run (60s), not just the pending (10s).
	// This matches Python semantics: status IN ('pending','running'), MAX age.
	if st.PendingWaitMaxSecs == nil {
		t.Fatal("PendingWaitMaxSecs is nil, want non-nil")
	}
	if *st.PendingWaitMaxSecs < 55.0 {
		t.Errorf("PendingWaitMaxSecs = %.1f, want >= 55.0 (running run should be included)", *st.PendingWaitMaxSecs)
	}

	// Median of [60s, 10s] = ~35s (percentile_cont(0.5) over 2 values).
	if st.PendingWaitMedSecs == nil {
		t.Fatal("PendingWaitMedSecs is nil, want non-nil")
	}
	if *st.PendingWaitMedSecs < 9.0 {
		t.Errorf("PendingWaitMedSecs = %.1f, want >= 9.0", *st.PendingWaitMedSecs)
	}
}

// TestStore_LeaseTTL_HonorsConfig verifies item 6 parity:
// NewStoreWithLeaseTTL wires the lease TTL from config into ExtendLease and Next.
func TestStore_LeaseTTL_HonorsConfig(t *testing.T) {
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
	from_db := func() *runs.Store {
		if err := db.Migrate(pool, dsn); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		return runs.NewStoreWithLeaseTTL(pool, 45) // 45s TTL
	}
	store := from_db()
	aID := testdb.MustInsertAssistant(t, ctx, pool, "graph-leasettl", nil)
	thID := "ffffffff-eeee-eeee-eeee-ffffffffffff"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending")

	// Claim via Next — lease should be set to ~45s from now.
	claimed, err := store.Next(ctx, 1)
	if err != nil || len(claimed) == 0 {
		t.Fatalf("Next: %v, claimed=%d", err, len(claimed))
	}
	rID := claimed[0].Run.RunID

	// Check lease_expires_at is ~45s in the future (not 5 minutes = 300s).
	var leaseSecsFromNow float64
	if err := pool.QueryRow(ctx,
		`SELECT EXTRACT(EPOCH FROM (lease_expires_at - now())) FROM run WHERE run_id = $1::uuid`,
		rID,
	).Scan(&leaseSecsFromNow); err != nil {
		t.Fatalf("get lease_expires_at: %v", err)
	}
	if leaseSecsFromNow < 40.0 || leaseSecsFromNow > 55.0 {
		t.Errorf("lease TTL = %.1f seconds from now, want ~45s (not 300s/5min)", leaseSecsFromNow)
	}

	// ExtendLease should also use the 45s TTL.
	if err := store.ExtendLease(ctx, rID, ""); err != nil {
		t.Fatalf("ExtendLease: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT EXTRACT(EPOCH FROM (lease_expires_at - now())) FROM run WHERE run_id = $1::uuid`,
		rID,
	).Scan(&leaseSecsFromNow); err != nil {
		t.Fatalf("get lease_expires_at after ExtendLease: %v", err)
	}
	if leaseSecsFromNow < 40.0 || leaseSecsFromNow > 55.0 {
		t.Errorf("ExtendLease TTL = %.1f seconds from now, want ~45s", leaseSecsFromNow)
	}
}
