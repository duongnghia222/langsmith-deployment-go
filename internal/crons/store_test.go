package crons_test

import (
	"context"
	"testing"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/crons"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestStore(t *testing.T, ctx context.Context) (*crons.Store, *pgxpool.Pool) {
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
	return crons.NewStore(pool), pool
}

func TestStore_Search(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "cron-graph", nil)
	aID2 := testdb.MustInsertAssistant(t, ctx, pool, "cron-graph-2", nil)

	id1 := testdb.MustInsertCron(t, ctx, pool, aID, "* * * * *")
	id2 := testdb.MustInsertCron(t, ctx, pool, aID, "0 * * * *")
	id3 := testdb.MustInsertCron(t, ctx, pool, aID2, "0 0 * * *")
	allIDs := map[string]struct{}{id1: {}, id2: {}, id3: {}}

	// All results (no filter).
	results, err := store.Search(ctx, crons.SearchInput{Limit: 10}, nil)
	if err != nil {
		t.Fatalf("Search all: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("len(results) = %d, want 3", len(results))
	}

	// Filter by assistant_id.
	byAssistant, err := store.Search(ctx, crons.SearchInput{AssistantID: aID, Limit: 10}, nil)
	if err != nil {
		t.Fatalf("Search by assistant: %v", err)
	}
	if len(byAssistant) != 2 {
		t.Errorf("len(byAssistant) = %d, want 2", len(byAssistant))
	}

	// Pagination: limit 1.
	paged, err := store.Search(ctx, crons.SearchInput{Limit: 1, Offset: 0}, nil)
	if err != nil {
		t.Fatalf("Search paginated: %v", err)
	}
	if len(paged) != 1 {
		t.Errorf("len(paged) = %d, want 1", len(paged))
	}

	// Pagination: offset 2, should return 1 result.
	paged2, err := store.Search(ctx, crons.SearchInput{Limit: 10, Offset: 2}, nil)
	if err != nil {
		t.Fatalf("Search offset: %v", err)
	}
	if len(paged2) != 1 {
		t.Errorf("len(paged2 offset=2) = %d, want 1", len(paged2))
	}

	// Pagination coverage: verify page 1 (Limit=2, Offset=0) and page 2 (Limit=2, Offset=2)
	// together cover all 3 inserted cron_ids without overlap.
	// All rows share the same next_run_date (now()+1h), so the cron_id tiebreaker must
	// produce a stable, non-repeating order across pages.
	page1, err := store.Search(ctx, crons.SearchInput{Limit: 2, Offset: 0}, nil)
	if err != nil {
		t.Fatalf("Search page1: %v", err)
	}
	page2, err := store.Search(ctx, crons.SearchInput{Limit: 2, Offset: 2}, nil)
	if err != nil {
		t.Fatalf("Search page2: %v", err)
	}
	seen := make(map[string]struct{}, 3)
	for _, c := range append(page1, page2...) {
		seen[c.CronID] = struct{}{}
	}
	if len(seen) != 3 {
		t.Errorf("pagination coverage: got %d unique IDs across pages, want 3 (ORDER BY is non-deterministic or overlaps)", len(seen))
	}
	for id := range allIDs {
		if _, ok := seen[id]; !ok {
			t.Errorf("pagination coverage: cron_id %s missing from paginated results", id)
		}
	}
}

func TestStore_Count(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "cron-count-graph", nil)
	aID2 := testdb.MustInsertAssistant(t, ctx, pool, "cron-count-graph-2", nil)

	testdb.MustInsertCron(t, ctx, pool, aID, "* * * * *")
	testdb.MustInsertCron(t, ctx, pool, aID, "0 * * * *")
	testdb.MustInsertCron(t, ctx, pool, aID2, "0 0 * * *")

	// Count all.
	n, err := store.Count(ctx, crons.SearchInput{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 3 {
		t.Errorf("Count = %d, want 3", n)
	}

	// Count with assistant_id filter.
	nByAssistant, err := store.Count(ctx, crons.SearchInput{AssistantID: aID}, nil)
	if err != nil {
		t.Fatalf("Count by assistant: %v", err)
	}
	if nByAssistant != 2 {
		t.Errorf("Count(assistant) = %d, want 2", nByAssistant)
	}
}

func TestCronStore_Create_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)

	c, err := store.Create(ctx, crons.CreateCronInput{
		AssistantID: aID,
		Schedule:    "*/5 * * * *",
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if c.CronID == "" {
		t.Fatal("CronID empty")
	}
	if c.Schedule != "*/5 * * * *" {
		t.Errorf("Schedule = %q", c.Schedule)
	}
	if !c.Enabled {
		t.Error("Enabled = false, want true")
	}
}

func TestCronStore_Patch_UpdatesSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)
	cronID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)

	c, err := store.Patch(ctx, cronID, crons.PatchCronInput{Schedule: "0 * * * *"}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if c.Schedule != "0 * * * *" {
		t.Errorf("Schedule = %q, want 0 * * * *", c.Schedule)
	}
}

func TestCronStore_Delete_RemovesRow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)
	cronID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)

	if err := store.Delete(ctx, cronID, nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	rows, _ := store.Search(ctx, crons.SearchInput{}, nil)
	for _, c := range rows {
		if c.CronID == cronID {
			t.Errorf("cron still exists after delete")
		}
	}
}

func TestCronStore_Next_ReturnsDueCrons(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)
	cronID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)
	// set next_run_date in the past
	if _, err := pool.Exec(ctx,
		`UPDATE cron SET next_run_date = now() - interval '1 second' WHERE cron_id = $1::uuid`, cronID,
	); err != nil {
		t.Fatalf("set next_run_date: %v", err)
	}
	due, err := store.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	found := false
	for _, cw := range due {
		if cw.Cron.CronID == cronID {
			found = true
		}
	}
	if !found {
		t.Errorf("due cron %s not returned by Next", cronID)
	}
}

func TestStore_Search_ThreadFilters_Matches(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "tf-store-match-graph", nil)

	// Thread whose metadata matches the filter.
	tID := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"role":"admin"}`))
	matchingCronID := testdb.MustInsertCronWithThread(t, ctx, pool, aID, tID, "* * * * *")

	// Thread that does NOT match the filter.
	tID2 := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"role":"user"}`))
	testdb.MustInsertCronWithThread(t, ctx, pool, aID, tID2, "0 * * * *")

	results, err := store.Search(ctx, crons.SearchInput{
		ThreadFilters: []*coreapi.AuthFilter{
			{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "role", Match: `"admin"`}}},
		},
		Limit: 10,
	}, nil)
	if err != nil {
		t.Fatalf("Search with thread_filters: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("len(results) = %d, want 1", len(results))
	}
	if len(results) == 1 && results[0].CronID != matchingCronID {
		t.Errorf("got cron_id %q, want %q", results[0].CronID, matchingCronID)
	}
}

func TestStore_Search_ThreadFilters_NoMatch(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "tf-store-nomatch-graph", nil)

	// Two threads with roles that do NOT match the filter key "superadmin".
	tID := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"role":"admin"}`))
	testdb.MustInsertCronWithThread(t, ctx, pool, aID, tID, "* * * * *")

	tID2 := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"role":"user"}`))
	testdb.MustInsertCronWithThread(t, ctx, pool, aID, tID2, "0 * * * *")

	results, err := store.Search(ctx, crons.SearchInput{
		ThreadFilters: []*coreapi.AuthFilter{
			{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "role", Match: `"superadmin"`}}},
		},
		Limit: 10,
	}, nil)
	if err != nil {
		t.Fatalf("Search with no-match thread_filters: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("len(results) = %d, want 0", len(results))
	}
}

// TestStore_Search_ThreadFilters_ExemptsNullThread proves crons with no
// thread_id are exempt from thread auth filters (LEFT JOIN thread + "cron.
// thread_id IS NULL OR (...)" — ops.py:2440-2442), replacing the old INNER
// JOIN which excluded them outright.
func TestStore_Search_ThreadFilters_ExemptsNullThread(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "tf-store-nullthread-graph", nil)
	noThreadCronID := testdb.MustInsertCron(t, ctx, pool, aID, "* * * * *")

	tID := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"role":"user"}`))
	testdb.MustInsertCronWithThread(t, ctx, pool, aID, tID, "0 * * * *")

	results, err := store.Search(ctx, crons.SearchInput{
		ThreadFilters: []*coreapi.AuthFilter{
			{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "role", Match: `"admin"`}}},
		},
		Limit: 10,
	}, nil)
	if err != nil {
		t.Fatalf("Search with thread_filters: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1 (only the no-thread cron)", len(results))
	}
	if results[0].CronID != noThreadCronID {
		t.Errorf("got cron_id %q, want %q", results[0].CronID, noThreadCronID)
	}
}

func TestCronStore_SetNextRunDate_Updates(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)
	cronID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)

	future := time.Now().Add(1 * time.Hour)
	if err := store.SetNextRunDate(ctx, cronID, future); err != nil {
		t.Fatalf("SetNextRunDate: %v", err)
	}
	rows, _ := store.Search(ctx, crons.SearchInput{}, nil)
	for _, c := range rows {
		if c.CronID == cronID {
			if c.NextRunDate == nil || c.NextRunDate.Before(time.Now()) {
				t.Errorf("NextRunDate = %v, want future", c.NextRunDate)
			}
		}
	}
}

// TestCronStore_Next_ExcludesExpiredCrons verifies that Next() does not return
// crons whose end_time has passed.
// Keep-verbatim predicate: (end_time IS NULL OR end_time >= now()) — ops.py:2326.
func TestCronStore_Next_ExcludesExpiredCrons(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "next-expire-graph", nil)

	// Expired cron: next_run_date in past, end_time also in past.
	expiredID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)
	if _, err := pool.Exec(ctx,
		`UPDATE cron SET next_run_date = now() - interval '1 second', end_time = now() - interval '1 second' WHERE cron_id = $1::uuid`,
		expiredID,
	); err != nil {
		t.Fatalf("set expired: %v", err)
	}

	// Active cron: next_run_date in past, end_time in future (should be returned).
	activeID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "0 * * * *", true)
	if _, err := pool.Exec(ctx,
		`UPDATE cron SET next_run_date = now() - interval '1 second', end_time = now() + interval '1 hour' WHERE cron_id = $1::uuid`,
		activeID,
	); err != nil {
		t.Fatalf("set active: %v", err)
	}

	// Null end_time cron: should always be returned.
	nullEndID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "0 0 * * *", true)
	if _, err := pool.Exec(ctx,
		`UPDATE cron SET next_run_date = now() - interval '1 second', end_time = NULL WHERE cron_id = $1::uuid`,
		nullEndID,
	); err != nil {
		t.Fatalf("set null end: %v", err)
	}

	due, err := store.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	ids := make(map[string]struct{}, len(due))
	for _, cw := range due {
		ids[cw.Cron.CronID] = struct{}{}
	}

	if _, ok := ids[expiredID]; ok {
		t.Errorf("expired cron %s should NOT be returned by Next", expiredID)
	}
	if _, ok := ids[activeID]; !ok {
		t.Errorf("active (future end_time) cron %s should be returned by Next", activeID)
	}
	if _, ok := ids[nullEndID]; !ok {
		t.Errorf("null-end_time cron %s should be returned by Next", nullEndID)
	}
}

// TestCronStore_Next_DbNow verifies that Next() returns a non-zero DB-side now()
// snapshot for each due cron.
func TestCronStore_Next_DbNow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "next-dbnow-graph", nil)
	cronID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)
	if _, err := pool.Exec(ctx,
		`UPDATE cron SET next_run_date = now() - interval '1 second' WHERE cron_id = $1::uuid`, cronID,
	); err != nil {
		t.Fatalf("set next_run_date: %v", err)
	}

	due, err := store.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(due) == 0 {
		t.Fatal("Next returned 0 crons")
	}
	for _, cw := range due {
		if cw.Now.IsZero() {
			t.Errorf("CronWithNow.Now is zero for cron_id=%s", cw.Cron.CronID)
		}
	}
}

// TestCronStore_Patch_EndTime verifies that PatchCronInput.EndTime is persisted.
func TestCronStore_Patch_EndTime(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "patch-endtime-graph", nil)
	cronID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)

	future := time.Now().Add(48 * time.Hour).UTC().Truncate(time.Second)
	c, err := store.Patch(ctx, cronID, crons.PatchCronInput{EndTime: &future}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if c.EndTime == nil {
		t.Fatal("EndTime is nil after patch")
	}
	if !c.EndTime.UTC().Truncate(time.Second).Equal(future) {
		t.Errorf("EndTime = %v, want %v", c.EndTime.UTC(), future)
	}
}

// TestCronStore_Create_AuthFilters_NotFound verifies that when assistant/thread
// auth filters are provided and the target is not visible, Create returns ErrNotFound
// (ops.py:2278-2281: "Thread not found or not authorized").
func TestCronStore_Create_AuthFilters_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "auth-notfound-graph", []byte(`{"owner":"bob"}`))
	// Thread that exists but won't match the auth filter.
	tID := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"owner":"alice"}`))

	// AssistantFilters that don't match the assistant's metadata → no authorized_assistant row.
	_, err := store.Create(ctx, crons.CreateCronInput{
		AssistantID: aID,
		ThreadID:    tID,
		Schedule:    "* * * * *",
		Enabled:     true,
		AssistantFilters: []*coreapi.AuthFilter{
			{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "owner", Match: `"charlie"`}}},
		},
	})
	if err == nil {
		t.Fatal("expected ErrNotFound, got nil")
	}
	if err != crons.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// TestCronStore_Search_EnabledFilter verifies that the Enabled *bool filter works.
func TestCronStore_Search_EnabledFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "search-enabled-graph", nil)

	enabledID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)
	disabledID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "0 * * * *", false)

	trueVal := true
	enabled, err := store.Search(ctx, crons.SearchInput{Limit: 10, Enabled: &trueVal}, nil)
	if err != nil {
		t.Fatalf("Search enabled=true: %v", err)
	}
	ids := make(map[string]struct{})
	for _, c := range enabled {
		ids[c.CronID] = struct{}{}
	}
	if _, ok := ids[enabledID]; !ok {
		t.Errorf("enabled cron %s missing from enabled=true results", enabledID)
	}
	if _, ok := ids[disabledID]; ok {
		t.Errorf("disabled cron %s present in enabled=true results", disabledID)
	}

	falseVal := false
	disabled, err := store.Search(ctx, crons.SearchInput{Limit: 10, Enabled: &falseVal}, nil)
	if err != nil {
		t.Fatalf("Search enabled=false: %v", err)
	}
	ids2 := make(map[string]struct{})
	for _, c := range disabled {
		ids2[c.CronID] = struct{}{}
	}
	if _, ok := ids2[disabledID]; !ok {
		t.Errorf("disabled cron %s missing from enabled=false results", disabledID)
	}
	if _, ok := ids2[enabledID]; ok {
		t.Errorf("enabled cron %s present in enabled=false results", enabledID)
	}
}

// TestCronStore_Search_SortOrder verifies that sort_by/sort_order are respected.
func TestCronStore_Search_SortOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "search-sort-graph", nil)

	// Insert 3 crons with distinct, known next_run_dates.
	testdb.MustInsertCron(t, ctx, pool, aID, "* * * * *")
	testdb.MustInsertCron(t, ctx, pool, aID, "0 * * * *")
	testdb.MustInsertCron(t, ctx, pool, aID, "0 0 * * *")

	// Default order (created_at DESC): most-recently inserted cron first.
	defaultOrder, err := store.Search(ctx, crons.SearchInput{Limit: 10}, nil)
	if err != nil {
		t.Fatalf("Search default: %v", err)
	}
	if len(defaultOrder) < 2 {
		t.Fatalf("expected at least 2 results, got %d", len(defaultOrder))
	}
	// created_at DESC: first row's created_at >= second row's created_at.
	if defaultOrder[0].CreatedAt.Before(defaultOrder[1].CreatedAt) {
		t.Errorf("default sort: expected created_at DESC; got %v before %v",
			defaultOrder[0].CreatedAt, defaultOrder[1].CreatedAt)
	}

	// ASC order by created_at.
	ascOrder, err := store.Search(ctx, crons.SearchInput{Limit: 10, SortBy: "created_at", SortOrder: "asc"}, nil)
	if err != nil {
		t.Fatalf("Search asc: %v", err)
	}
	if len(ascOrder) < 2 {
		t.Fatalf("expected at least 2 results, got %d", len(ascOrder))
	}
	if ascOrder[0].CreatedAt.After(ascOrder[1].CreatedAt) {
		t.Errorf("asc sort: expected created_at ASC; got %v after %v",
			ascOrder[0].CreatedAt, ascOrder[1].CreatedAt)
	}
}

// TestCronStore_InvalidSchedule verifies that creating a cron with an invalid
// schedule returns an error prefixed with "schedule parse:".
func TestCronStore_InvalidSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "invalid-sched-graph", nil)

	_, err := store.Create(ctx, crons.CreateCronInput{
		AssistantID: aID,
		Schedule:    "not a cron",
		Enabled:     true,
	})
	if err == nil {
		t.Fatal("expected error for invalid schedule, got nil")
	}
	if len(err.Error()) < 15 || err.Error()[:15] != "schedule parse:" {
		t.Errorf("expected error prefixed 'schedule parse:', got %q", err.Error())
	}
}
