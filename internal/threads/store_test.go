package threads_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/duongnghia222/langsmith-deployment-go/internal/threads"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestStore(t *testing.T, ctx context.Context) (*threads.Store, *pgxpool.Pool) {
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
	return threads.NewStore(pool), pool
}

func TestStore_Get_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	_, err := store.Get(ctx, "00000000-0000-0000-0000-000000000000", nil)
	if err == nil || !errors.Is(err, threads.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestStore_Get_ReturnsRow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := "11111111-1111-1111-1111-111111111111"
	testdb.MustInsertThread(t, ctx, pool, id, []byte(`{"owner":"alice"}`))

	th, err := store.Get(ctx, id, nil)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if th.ThreadID != id {
		t.Errorf("ThreadID = %q, want %q", th.ThreadID, id)
	}
	// PostgreSQL normalizes JSONB (e.g. adds spaces), so compare semantically.
	var got, want map[string]any
	if err := json.Unmarshal(th.Metadata, &got); err != nil {
		t.Fatalf("unmarshal Metadata: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"owner":"alice"}`), &want); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if got["owner"] != want["owner"] {
		t.Errorf("Metadata owner = %v, want alice", got["owner"])
	}
}

func TestStore_Search_FiltersAndPaginates(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	for i, id := range []string{
		"22222222-2222-2222-2222-222222222221",
		"22222222-2222-2222-2222-222222222222",
		"22222222-2222-2222-2222-222222222223",
	} {
		owner := "bob"
		if i == 1 {
			owner = "alice"
		}
		testdb.MustInsertThread(t, ctx, pool, id, []byte(`{"owner":"`+owner+`"}`))
	}

	results, err := store.Search(ctx, threads.SearchInput{Limit: 10, Offset: 0}, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) < 3 {
		t.Errorf("len(results) = %d, want >=3", len(results))
	}

	// Pagination: limit 1
	results, err = store.Search(ctx, threads.SearchInput{Limit: 1, Offset: 0}, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("len(results) = %d, want 1", len(results))
	}
}

func TestStore_Count(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	for _, id := range []string{
		"33333333-3333-3333-3333-333333333331",
		"33333333-3333-3333-3333-333333333332",
	} {
		testdb.MustInsertThread(t, ctx, pool, id, nil)
	}
	n, err := store.Count(ctx, threads.SearchInput{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n < 2 {
		t.Errorf("Count = %d, want >=2", n)
	}
}

func TestStore_GetGraphID(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := "44444444-4444-4444-4444-444444444444"
	// Seed thread with config containing graph_id
	if _, err := pool.Exec(ctx,
		`INSERT INTO thread (thread_id, config, status, created_at, updated_at)
		 VALUES ($1::uuid, $2::jsonb, 'idle', now(), now())`,
		id, `{"configurable":{"graph_id":"my-graph"}}`,
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	gid, err := store.GetGraphID(ctx, id, nil)
	if err != nil {
		t.Fatalf("GetGraphID: %v", err)
	}
	if gid != "my-graph" {
		t.Errorf("graph_id = %q, want my-graph", gid)
	}
}

func TestThreadStore_Create_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	th, err := store.Create(ctx, threads.CreateThreadInput{
		Metadata: []byte(`{"env":"test"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if th.ThreadID == "" {
		t.Fatal("ThreadID empty")
	}
	if th.Status != "idle" {
		t.Errorf("Status = %q, want idle", th.Status)
	}
}

func TestThreadStore_Patch_UpdatesMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	testdb.MustInsertThread(t, ctx, pool, id, []byte(`{"a":1}`))
	th, err := store.Patch(ctx, id, threads.PatchThreadInput{
		Metadata: []byte(`{"a":2}`),
	}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if string(th.Metadata) != `{"a": 2}` && string(th.Metadata) != `{"a":2}` {
		t.Errorf("Metadata = %s, want a:2", th.Metadata)
	}
}

func TestThreadStore_Delete_RemovesRow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	testdb.MustInsertThread(t, ctx, pool, id, nil)
	deleted, err := store.Delete(ctx, id, nil)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted != id {
		t.Errorf("deleted = %q, want %q", deleted, id)
	}
	_, err = store.Get(ctx, id, nil)
	if !errors.Is(err, threads.ErrNotFound) {
		t.Errorf("after delete: %v, want ErrNotFound", err)
	}
}

func TestThreadStore_SetStatus_UpdatesStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := "dddddddd-dddd-dddd-dddd-dddddddddddd"
	testdb.MustInsertThread(t, ctx, pool, id, nil)
	// SetStatus with no pending/running runs → always gets the requested status.
	_, err := store.SetStatus(ctx, threads.SetStatusInput{
		ThreadID:   id,
		StatusText: "idle",
	})
	if err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	th, err := store.Get(ctx, id, nil)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if th.Status != "idle" {
		t.Errorf("Status = %q, want idle", th.Status)
	}
}

func TestThreadStore_Copy_CreatesNewRow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	srcID := "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
	testdb.MustInsertThread(t, ctx, pool, srcID, []byte(`{"src":true}`))
	newTh, err := store.Copy(ctx, srcID, nil)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if newTh.ThreadID == srcID {
		t.Fatal("Copy returned same thread_id as source")
	}
	if newTh.ThreadID == "" {
		t.Fatal("new ThreadID empty")
	}
}

func TestStore_SetJointStatus_UpdatesBothRunAndThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g", nil)
	thID := "f1000001-0000-0000-0000-000000000001"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	if err := store.SetJointStatus(ctx, threads.SetJointStatusInput{
		ThreadID:  thID,
		RunID:     rID,
		RunStatus: "idle",
		GraphID:   "g-after",
	}); err != nil {
		t.Fatalf("SetJointStatus: %v", err)
	}

	var runStatus, threadStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM run WHERE run_id=$1::uuid`, rID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM thread WHERE thread_id=$1::uuid`, thID).Scan(&threadStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "idle" {
		t.Errorf("run.status = %q, want idle", runStatus)
	}
	if threadStatus != "idle" {
		t.Errorf("thread.status = %q, want idle (no active runs after update)", threadStatus)
	}
}

func TestStore_SetJointStatus_RollbackDeletesRun(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g", nil)
	thID := "f2000002-0000-0000-0000-000000000002"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	rID := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")

	if err := store.SetJointStatus(ctx, threads.SetJointStatusInput{
		ThreadID:  thID,
		RunID:     rID,
		RunStatus: "rollback",
		GraphID:   "g-x",
	}); err != nil {
		t.Fatalf("SetJointStatus rollback: %v", err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM run WHERE run_id=$1::uuid`, rID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("run row should be deleted, got count=%d", n)
	}
}

func TestStore_SetJointStatus_BusyWhenOtherRunsActive(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g", nil)
	thID := "f3000003-0000-0000-0000-000000000003"
	testdb.MustInsertThread(t, ctx, pool, thID, nil)
	r1 := testdb.MustInsertRun(t, ctx, pool, thID, aID, "running")
	_ = testdb.MustInsertRun(t, ctx, pool, thID, aID, "pending") // second run still active

	if err := store.SetJointStatus(ctx, threads.SetJointStatusInput{
		ThreadID:  thID,
		RunID:     r1,
		RunStatus: "idle",
		GraphID:   "g",
	}); err != nil {
		t.Fatalf("SetJointStatus: %v", err)
	}

	var threadStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM thread WHERE thread_id=$1::uuid`, thID).Scan(&threadStatus); err != nil {
		t.Fatal(err)
	}
	if threadStatus != "busy" {
		t.Errorf("thread.status = %q, want busy (still a pending run)", threadStatus)
	}
}

// ── Task 4: TTL persistence ────────────────────────────────────────────────

func TestStore_Create_WithTTL_SetsExpiresAt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	ttl := 60.0
	th, err := store.Create(ctx, threads.CreateThreadInput{
		Metadata:   []byte(`{}`),
		TTLSeconds: &ttl,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if th.ExpiresAt == nil {
		t.Fatal("ExpiresAt is nil, want non-nil after Create with TTL")
	}
	diff := th.ExpiresAt.Sub(th.CreatedAt)
	const want = float64(60 * time.Second)
	const tol = float64(2 * time.Second)
	got := float64(diff)
	if got < want-tol || got > want+tol {
		t.Errorf("ExpiresAt - CreatedAt = %v, want ~60s", diff)
	}
}

func TestStore_Create_WithoutTTL_LeavesExpiresAtNil(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	th, err := store.Create(ctx, threads.CreateThreadInput{Metadata: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if th.ExpiresAt != nil {
		t.Errorf("ExpiresAt = %v, want nil when no TTL given", th.ExpiresAt)
	}
}

func TestStore_Patch_UpdatesExpiresAt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := "aa000001-0000-0000-0000-000000000001"
	testdb.MustInsertThread(t, ctx, pool, id, []byte(`{}`))
	ttl := 120.0
	patched, err := store.Patch(ctx, id, threads.PatchThreadInput{TTLSeconds: &ttl}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if patched.ExpiresAt == nil {
		t.Fatal("ExpiresAt is nil after Patch with TTL, want non-nil")
	}
}

// ── Task 5: values_json filter ────────────────────────────────────────────

func TestStore_Search_ValuesFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	th1, err := store.Create(ctx, threads.CreateThreadInput{Metadata: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Create th1: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE thread SET "values" = $1::jsonb WHERE thread_id = $2::uuid`,
		`{"name":"alice"}`, th1.ThreadID,
	); err != nil {
		t.Fatalf("set values th1: %v", err)
	}

	th2, err := store.Create(ctx, threads.CreateThreadInput{Metadata: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Create th2: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE thread SET "values" = $1::jsonb WHERE thread_id = $2::uuid`,
		`{"name":"bob"}`, th2.ThreadID,
	); err != nil {
		t.Fatalf("set values th2: %v", err)
	}

	results, err := store.Search(ctx, threads.SearchInput{
		ValuesFilter: []byte(`{"name":"alice"}`),
	}, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}
	if results[0].ThreadID != th1.ThreadID {
		t.Errorf("results[0].ThreadID = %q, want %q", results[0].ThreadID, th1.ThreadID)
	}
}

func TestThreadExistsAndAuth(t *testing.T) {
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

	threadID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	testdb.MustInsertThread(t, ctx, pool, threadID, []byte(`{"owner":"carol"}`))

	store := threads.NewStore(pool)

	// Positive: no filters.
	if err := store.ThreadExistsAndAuth(ctx, threadID, nil); err != nil {
		t.Errorf("positive (no filters): %v", err)
	}

	// Positive: matching filter.
	matchFilter := []*coreapi.AuthFilter{
		{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "owner", Match: "carol"}}},
	}
	if err := store.ThreadExistsAndAuth(ctx, threadID, matchFilter); err != nil {
		t.Errorf("matching filter: %v", err)
	}

	// Negative: mismatched filter → ErrForbidden.
	mismatchFilter := []*coreapi.AuthFilter{
		{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "owner", Match: "eve"}}},
	}
	if err := store.ThreadExistsAndAuth(ctx, threadID, mismatchFilter); err == nil {
		t.Error("mismatch filter: expected error, got nil")
	} else if !errors.Is(err, threads.ErrForbidden) {
		t.Errorf("mismatch filter: got %v, want threads.ErrForbidden", err)
	}

	// Negative: non-existent thread → ErrNotFound.
	fakeID := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	if err := store.ThreadExistsAndAuth(ctx, fakeID, nil); err == nil {
		t.Error("non-existent thread: expected ErrNotFound, got nil")
	} else if !errors.Is(err, threads.ErrNotFound) {
		t.Errorf("non-existent thread: got %v, want threads.ErrNotFound", err)
	}
}

// ── C10: Patch metadata MERGE ─────────────────────────────────────────────────

func TestStore_Patch_MetadataMerges(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := "a0100001-0000-0000-0000-000000000001"
	// Seed with {"a":1,"b":2}
	testdb.MustInsertThread(t, ctx, pool, id, []byte(`{"a":1,"b":2}`))
	// Patch with {"b":99,"c":3} — must merge, not replace
	_, err := store.Patch(ctx, id, threads.PatchThreadInput{
		Metadata: []byte(`{"b":99,"c":3}`),
	}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	th, err := store.Get(ctx, id, nil)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(th.Metadata, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// "a" must still be present (merge semantics — ops.py:883)
	if m["a"] == nil {
		t.Error("key 'a' was wiped by patch — want merge semantics (metadata = metadata || patch)")
	}
	// "b" must be updated
	if m["b"] != float64(99) {
		t.Errorf("key 'b' = %v, want 99", m["b"])
	}
	// "c" must be added
	if m["c"] != float64(3) {
		t.Errorf("key 'c' = %v, want 3", m["c"])
	}
}

// ── C11: SetStatus writes values/interrupts + busy CASE ──────────────────────

func TestStore_SetStatus_WritesValuesAndInterrupts(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := "a1100001-0000-0000-0000-000000000001"
	testdb.MustInsertThread(t, ctx, pool, id, nil)

	busy, err := store.SetStatus(ctx, threads.SetStatusInput{
		ThreadID:   id,
		StatusText: "idle",
		ValuesJSON: []byte(`{"x":42}`),
		Interrupts: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	if busy {
		t.Error("expected busy=false (no pending/running runs)")
	}
	var values []byte
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE("values",'null'::jsonb)::text::bytea FROM thread WHERE thread_id=$1::uuid`, id,
	).Scan(&values); err != nil {
		t.Fatalf("scan values: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(values, &m); err != nil {
		t.Fatalf("unmarshal values: %v", err)
	}
	if m["x"] != float64(42) {
		t.Errorf("values[x] = %v, want 42", m["x"])
	}
}

func TestStore_SetStatus_BusyWhenPendingRunExists(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g", nil)
	id := "a1100001-0000-0000-0000-000000000002"
	testdb.MustInsertThread(t, ctx, pool, id, nil)
	// Insert a pending run — this should make the result 'busy'
	testdb.MustInsertRun(t, ctx, pool, id, aID, "pending")

	busy, err := store.SetStatus(ctx, threads.SetStatusInput{
		ThreadID:   id,
		StatusText: "idle",
	})
	if err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	if !busy {
		t.Error("expected busy=true (pending run present) — ops.py:922-930")
	}
	var st string
	if err := pool.QueryRow(ctx, `SELECT status FROM thread WHERE thread_id=$1::uuid`, id).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "busy" {
		t.Errorf("thread status = %q, want 'busy'", st)
	}
}

func TestStore_SetStatus_NilCheckpointWritesNullValues(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := "a1100001-0000-0000-0000-000000000003"
	testdb.MustInsertThread(t, ctx, pool, id, nil)
	// Pre-set a non-null values
	if _, err := pool.Exec(ctx,
		`UPDATE thread SET "values" = $1::jsonb WHERE thread_id=$2::uuid`,
		`{"old":true}`, id,
	); err != nil {
		t.Fatal(err)
	}
	// nil ValuesJSON → pass NULL → overwrite the old value (ops.py:936: None when checkpoint is None)
	_, err := store.SetStatus(ctx, threads.SetStatusInput{
		ThreadID:   id,
		StatusText: "idle",
		ValuesJSON: nil, // no checkpoint → NULL
	})
	if err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	var isNull bool
	if err := pool.QueryRow(ctx,
		`SELECT "values" IS NULL FROM thread WHERE thread_id=$1::uuid`, id,
	).Scan(&isNull); err != nil {
		t.Fatal(err)
	}
	if !isNull {
		t.Error("values should be NULL when no checkpoint provided — ops.py:936")
	}
}

// ── C12: Atomic Create with if_exists ────────────────────────────────────────

func TestStore_Create_AtomicDoNothing_ReturnsSameRow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	id := "c1200001-0000-0000-0000-000000000001"
	th1, err := store.Create(ctx, threads.CreateThreadInput{
		ThreadID: id,
		Metadata: []byte(`{"first":true}`),
		IfExists: "do_nothing",
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	th2, err := store.Create(ctx, threads.CreateThreadInput{
		ThreadID: id,
		Metadata: []byte(`{"second":true}`),
		IfExists: "do_nothing",
	})
	if err != nil {
		t.Fatalf("second Create (do_nothing): %v", err)
	}
	if th1.ThreadID != th2.ThreadID {
		t.Errorf("thread IDs differ: %q vs %q", th1.ThreadID, th2.ThreadID)
	}
	// The returned row should be the first (original) row, not overwritten
	var m map[string]any
	if err := json.Unmarshal(th2.Metadata, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["first"] != true {
		t.Errorf("do_nothing should return original row metadata, got %v", th2.Metadata)
	}
}

func TestStore_Create_RaiseMode_ErrAlreadyExists(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	id := "c1200001-0000-0000-0000-000000000002"
	if _, err := store.Create(ctx, threads.CreateThreadInput{
		ThreadID: id,
		IfExists: "raise",
	}); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	_, err := store.Create(ctx, threads.CreateThreadInput{
		ThreadID: id,
		IfExists: "raise",
	})
	if !errors.Is(err, threads.ErrAlreadyExists) {
		t.Errorf("second Create (raise): want ErrAlreadyExists, got %v", err)
	}
}

func TestStore_Create_ConcurrentDoNothing_BothSucceed(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	id := "c1200001-0000-0000-0000-000000000003"

	type result struct {
		th  *threads.Thread
		err error
	}
	ch := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			th, err := store.Create(ctx, threads.CreateThreadInput{
				ThreadID: id,
				Metadata: []byte(`{}`),
				IfExists: "do_nothing",
			})
			ch <- result{th, err}
		}()
	}
	r1, r2 := <-ch, <-ch
	if r1.err != nil {
		t.Errorf("goroutine 1 error: %v", r1.err)
	}
	if r2.err != nil {
		t.Errorf("goroutine 2 error: %v", r2.err)
	}
	if r1.th != nil && r2.th != nil && r1.th.ThreadID != r2.th.ThreadID {
		t.Errorf("concurrent do_nothing returned different IDs: %q vs %q", r1.th.ThreadID, r2.th.ThreadID)
	}
}

// ── C13/C14: Search ids filter and sort_by/sort_order ────────────────────────

func TestStore_Search_IdsFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	ids := []string{
		"a1300001-0000-0000-0000-000000000001",
		"a1300001-0000-0000-0000-000000000002",
		"a1300001-0000-0000-0000-000000000003",
	}
	for _, id := range ids {
		testdb.MustInsertThread(t, ctx, pool, id, nil)
	}

	// Search for only the first two
	results, err := store.Search(ctx, threads.SearchInput{
		Ids:   ids[:2],
		Limit: 10,
	}, nil)
	if err != nil {
		t.Fatalf("Search with ids: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("len(results) = %d, want 2", len(results))
	}
	gotIDs := map[string]bool{}
	for _, r := range results {
		gotIDs[r.ThreadID] = true
	}
	for _, id := range ids[:2] {
		if !gotIDs[id] {
			t.Errorf("missing id %q in results", id)
		}
	}
	if gotIDs[ids[2]] {
		t.Errorf("id %q should not be in results", ids[2])
	}
}

func TestStore_Search_SortByUpdatedAt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	ids := []string{
		"a1400001-0000-0000-0000-000000000001",
		"a1400001-0000-0000-0000-000000000002",
	}
	for i, id := range ids {
		testdb.MustInsertThread(t, ctx, pool, id, nil)
		// Stagger updated_at
		if _, err := pool.Exec(ctx,
			`UPDATE thread SET updated_at = now() + ($1 * interval '1 second') WHERE thread_id=$2::uuid`,
			i, id,
		); err != nil {
			t.Fatalf("update updated_at: %v", err)
		}
	}

	// Sort ASC — ids[0] should come before ids[1]
	results, err := store.Search(ctx, threads.SearchInput{
		Ids:       ids,
		SortBy:    "updated_at",
		SortOrder: "asc",
		Limit:     10,
	}, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) < 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].ThreadID != ids[0] {
		t.Errorf("ASC sort: first=%q, want %q", results[0].ThreadID, ids[0])
	}

	// Sort DESC — ids[1] should come first
	results, err = store.Search(ctx, threads.SearchInput{
		Ids:       ids,
		SortBy:    "updated_at",
		SortOrder: "desc",
		Limit:     10,
	}, nil)
	if err != nil {
		t.Fatalf("Search DESC: %v", err)
	}
	if len(results) < 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].ThreadID != ids[1] {
		t.Errorf("DESC sort: first=%q, want %q", results[0].ThreadID, ids[1])
	}
}

func TestStore_Search_InvalidSortBy_FallsBackToDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := "a1400001-0000-0000-0000-000000000009"
	testdb.MustInsertThread(t, ctx, pool, id, nil)

	// An invalid sort_by should fall back to "created_at DESC" without error.
	results, err := store.Search(ctx, threads.SearchInput{
		Ids:    []string{id},
		SortBy: "not_a_column", // invalid — should be silently ignored
		Limit:  10,
	}, nil)
	if err != nil {
		t.Fatalf("Search with invalid sort_by: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("len(results) = %d, want 1", len(results))
	}
}
