package crons_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/crons"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/jsonbutil"
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

// TestCronStore_Create_Idempotent verifies 4e: calling Create twice with the
// same CronID returns the same pre-existing row instead of erroring on a
// unique violation or creating a duplicate (ops.py:2237-2260's
// ON CONFLICT (cron_id) DO NOTHING + UNION ALL fallback SELECT).
func TestCronStore_Create_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "idempotent-graph", nil)

	first, err := store.Create(ctx, crons.CreateCronInput{
		AssistantID: aID,
		Schedule:    "*/5 * * * *",
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	second, err := store.Create(ctx, crons.CreateCronInput{
		CronID:      first.CronID,
		AssistantID: aID,
		Schedule:    "0 * * * *", // different from first; must be ignored, not applied
		Enabled:     false,
	})
	if err != nil {
		t.Fatalf("second Create (same cron_id): %v", err)
	}
	if second.CronID != first.CronID {
		t.Fatalf("CronID changed: got %q, want %q", second.CronID, first.CronID)
	}
	if second.Schedule != first.Schedule {
		t.Errorf("Schedule changed on conflict: got %q, want %q (original)", second.Schedule, first.Schedule)
	}
	if second.Enabled != first.Enabled {
		t.Errorf("Enabled changed on conflict: got %v, want %v (original)", second.Enabled, first.Enabled)
	}

	rows, err := store.Search(ctx, crons.SearchInput{}, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	count := 0
	for _, c := range rows {
		if c.CronID == first.CronID {
			count++
		}
	}
	if count != 1 {
		t.Errorf("found %d rows with cron_id %s, want exactly 1 (no duplicate)", count, first.CronID)
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

// TestCronStore_Patch_ScheduleOnly_PreservesTimezone verifies 4d-iii:
// patching only Schedule recomputes next_run_date using the row's stored
// timezone, not UTC.
func TestCronStore_Patch_ScheduleOnly_PreservesTimezone(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "patch-tz-preserve-graph", nil)

	created, err := store.Create(ctx, crons.CreateCronInput{
		AssistantID: aID,
		Schedule:    "0 12 * * *",
		Timezone:    "America/New_York",
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	patched, err := store.Patch(ctx, created.CronID, crons.PatchCronInput{Schedule: "30 12 * * *"}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if patched.Timezone != "America/New_York" {
		t.Errorf("Timezone = %q, want unchanged America/New_York", patched.Timezone)
	}
	if patched.NextRunDate == nil {
		t.Fatal("NextRunDate is nil")
	}
	// 12:30 America/New_York == 16:30 or 17:30 UTC depending on DST; either
	// way it must NOT be 12:30 UTC (which is what recomputing against
	// timezone="" would have produced).
	if patched.NextRunDate.UTC().Hour() == 12 && patched.NextRunDate.UTC().Minute() == 30 {
		t.Errorf("NextRunDate = %v looks computed against UTC, not the stored America/New_York timezone", patched.NextRunDate)
	}
}

// TestCronStore_Patch_TimezoneOnly_Recomputes verifies 4d-iii: patching only
// Timezone (with no Schedule in the request) still recomputes next_run_date,
// using the row's stored schedule.
func TestCronStore_Patch_TimezoneOnly_Recomputes(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "patch-tz-only-graph", nil)

	created, err := store.Create(ctx, crons.CreateCronInput{
		AssistantID: aID,
		Schedule:    "0 12 * * *",
		Timezone:    "UTC",
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.NextRunDate == nil {
		t.Fatal("NextRunDate is nil after Create")
	}
	before := *created.NextRunDate

	patched, err := store.Patch(ctx, created.CronID, crons.PatchCronInput{Timezone: "America/New_York"}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if patched.Schedule != "0 12 * * *" {
		t.Errorf("Schedule changed to %q, want unchanged 0 12 * * *", patched.Schedule)
	}
	if patched.Timezone != "America/New_York" {
		t.Errorf("Timezone = %q, want America/New_York", patched.Timezone)
	}
	if patched.NextRunDate == nil {
		t.Fatal("NextRunDate is nil after Patch")
	}
	if patched.NextRunDate.Equal(before) {
		t.Errorf("NextRunDate unchanged (%v) after patching timezone; recompute did not run", before)
	}
}

// TestCronStore_Patch_MergesPayloadAndMetadata verifies 4d-ii: Patch merges
// payload/metadata JSONB (||) instead of replacing, so unrelated top-level
// keys from the original row survive a partial patch.
func TestCronStore_Patch_MergesPayloadAndMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "patch-merge-graph", nil)

	created, err := store.Create(ctx, crons.CreateCronInput{
		AssistantID: aID,
		Schedule:    "* * * * *",
		Enabled:     true,
		Payload:     []byte(`{"assistant_id":"keep-me","input":{"x":1}}`),
		Metadata:    []byte(`{"owner":"alice","tag":"old"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	patched, err := store.Patch(ctx, created.CronID, crons.PatchCronInput{
		Payload:  []byte(`{"input":{"x":2}}`),
		Metadata: []byte(`{"tag":"new"}`),
	}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}

	var payload, metadata map[string]json.RawMessage
	if err := json.Unmarshal(patched.Payload, &payload); err != nil {
		t.Fatalf("payload not valid JSON: %v", err)
	}
	if err := json.Unmarshal(patched.Metadata, &metadata); err != nil {
		t.Fatalf("metadata not valid JSON: %v", err)
	}

	if _, ok := payload["assistant_id"]; !ok {
		t.Errorf("payload lost top-level key %q after replace-style patch: %s", "assistant_id", patched.Payload)
	}
	var input map[string]int
	if err := json.Unmarshal(payload["input"], &input); err != nil || input["x"] != 2 {
		t.Errorf("payload.input = %s, want {\"x\":2}", payload["input"])
	}
	if _, ok := metadata["owner"]; !ok {
		t.Errorf("metadata lost top-level key %q after replace-style patch: %s", "owner", patched.Metadata)
	}
	var tag string
	if err := json.Unmarshal(metadata["tag"], &tag); err != nil || tag != "new" {
		t.Errorf("metadata.tag = %s, want \"new\"", metadata["tag"])
	}
}

// TestCronStore_Patch_LegacyPayload_RoundTrips verifies fix round 1 finding 2:
// patching a legacy protojson-shaped row (pre-4a) must not silently discard
// the patch. Before the fix, merging a dict-shaped patch onto a
// protojson-shaped row produced a hybrid that decodePayload's legacy branch
// (protojson Unmarshal with DiscardUnknown:true) then dropped on every
// subsequent read.
func TestCronStore_Patch_LegacyPayload_RoundTrips(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "patch-legacy-graph", nil)
	cronID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)

	// Seed a pre-4a protojson-shaped payload directly (Create/Patch always
	// write the new dict shape, so this must bypass them, matching how a real
	// legacy row would already exist in the DB).
	legacy, err := jsonbutil.Marshal(&coreapi.CronPayload{
		AssistantId: "legacy-graph",
		InputJson:   []byte(`{"x":1}`),
	})
	if err != nil {
		t.Fatalf("jsonbutil.Marshal: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE cron SET payload = $2::jsonb WHERE cron_id = $1::uuid`, cronID, legacy); err != nil {
		t.Fatalf("seed legacy payload: %v", err)
	}

	patched, err := store.Patch(ctx, cronID, crons.PatchCronInput{
		Payload: []byte(`{"newkey":"newval"}`),
	}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(patched.Payload, &payload); err != nil {
		t.Fatalf("payload not valid JSON: %v", err)
	}
	if _, ok := payload["input_json"]; ok {
		t.Errorf("payload still legacy-shaped after patch: %s", patched.Payload)
	}
	var newkey string
	if err := json.Unmarshal(payload["newkey"], &newkey); err != nil || newkey != "newval" {
		t.Errorf("payload.newkey = %s, want \"newval\" (patch was discarded)", payload["newkey"])
	}
	var assistantID string
	if err := json.Unmarshal(payload["assistant_id"], &assistantID); err != nil || assistantID != "legacy-graph" {
		t.Errorf("payload.assistant_id = %s, want \"legacy-graph\" (legacy field lost)", payload["assistant_id"])
	}
	var input map[string]int
	if err := json.Unmarshal(payload["input"], &input); err != nil || input["x"] != 1 {
		t.Errorf("payload.input = %s, want {\"x\":1} (legacy field lost)", payload["input"])
	}

	// Re-read via a fresh query to confirm the fix persisted the normalized
	// shape to storage, not just to Patch's own RETURNING row.
	reread, err := store.Search(ctx, crons.SearchInput{Limit: 10}, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	found := false
	for _, c := range reread {
		if c.CronID != cronID {
			continue
		}
		found = true
		var rePayload map[string]json.RawMessage
		if err := json.Unmarshal(c.Payload, &rePayload); err != nil {
			t.Fatalf("reread payload not valid JSON: %v", err)
		}
		if _, ok := rePayload["newkey"]; !ok {
			t.Errorf("patched key missing on reread: %s", c.Payload)
		}
	}
	if !found {
		t.Fatalf("cron %s not found on reread", cronID)
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

// TestCronStore_Next_SkipsUnparseableSchedule verifies fix round 1 finding 3:
// a row whose schedule robfig cannot parse (reachable via the 4i croniter/
// robfig dialect gap, or a pre-validation legacy row) must be dropped from
// Next's due output rather than fired every tick with next_run_date left
// un-advanced — which would otherwise refire it forever.
func TestCronStore_Next_SkipsUnparseableSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "next-badsched-graph", nil)

	// Bypass Create's validation (which would reject this) — matches how a
	// genuinely unparseable schedule reaches the DB in production (4i dialect
	// gap, or a row written before Go-side validation existed).
	badID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "not a cron", true)
	if _, err := pool.Exec(ctx,
		`UPDATE cron SET next_run_date = now() - interval '1 second' WHERE cron_id = $1::uuid`, badID,
	); err != nil {
		t.Fatalf("set next_run_date: %v", err)
	}

	// A normal due cron in the same batch, to confirm the bad row doesn't
	// poison the whole claim.
	goodID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)
	if _, err := pool.Exec(ctx,
		`UPDATE cron SET next_run_date = now() - interval '1 second' WHERE cron_id = $1::uuid`, goodID,
	); err != nil {
		t.Fatalf("set next_run_date: %v", err)
	}

	due, err := store.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	ids := make(map[string]struct{}, len(due))
	for _, cw := range due {
		ids[cw.Cron.CronID] = struct{}{}
	}
	if _, ok := ids[badID]; ok {
		t.Errorf("unparseable-schedule cron %s should NOT be returned by Next", badID)
	}
	if _, ok := ids[goodID]; !ok {
		t.Errorf("valid cron %s should be returned by Next", goodID)
	}

	// The bad row's next_run_date must remain unchanged (still due) — proving
	// it wasn't silently advanced either; it's simply excluded from firing.
	var stillDue bool
	if err := pool.QueryRow(ctx,
		`SELECT next_run_date <= now() FROM cron WHERE cron_id = $1::uuid`, badID,
	).Scan(&stillDue); err != nil {
		t.Fatalf("check next_run_date: %v", err)
	}
	if !stillDue {
		t.Errorf("bad cron's next_run_date should remain due (unchanged), got advanced")
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

// TestCronStore_Next_AdvancesNextRunDate verifies that Next() itself advances
// next_run_date atomically (4c) — the caller no longer needs a separate
// SetNextRunDate call.
func TestCronStore_Next_AdvancesNextRunDate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "next-advance-graph", nil)
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
	var claimed *crons.Cron
	for _, cw := range due {
		if cw.Cron.CronID == cronID {
			claimed = cw.Cron
		}
	}
	if claimed == nil {
		t.Fatal("cron not claimed by Next")
	}

	var stored time.Time
	if err := pool.QueryRow(ctx, `SELECT next_run_date FROM cron WHERE cron_id = $1::uuid`, cronID).Scan(&stored); err != nil {
		t.Fatalf("select next_run_date: %v", err)
	}
	if !stored.After(time.Now()) {
		t.Errorf("next_run_date = %v, want advanced into the future", stored)
	}

	// A second Next() call must not re-claim the now-future-dated row.
	due2, err := store.Next(ctx)
	if err != nil {
		t.Fatalf("Next (second call): %v", err)
	}
	for _, cw := range due2 {
		if cw.Cron.CronID == cronID {
			t.Errorf("cron %s re-claimed by a second Next() call after advancing", cronID)
		}
	}
}

// TestCronStore_Next_ConcurrentCallsDoNotDoubleClaim proves that two
// concurrent Next() calls (simulating two scheduler replicas) never both
// claim the same due cron — FOR NO KEY UPDATE SKIP LOCKED plus the
// same-transaction next_run_date advance (4c) is what guarantees this.
func TestCronStore_Next_ConcurrentCallsDoNotDoubleClaim(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "next-concurrent-graph", nil)

	const n = 10
	ids := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		id := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)
		if _, err := pool.Exec(ctx,
			`UPDATE cron SET next_run_date = now() - interval '1 second' WHERE cron_id = $1::uuid`, id,
		); err != nil {
			t.Fatalf("set next_run_date: %v", err)
		}
		ids[id] = struct{}{}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	claimedBy := make(map[string]int) // cron_id -> number of goroutines that claimed it
	const replicas = 5
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			due, err := store.Next(ctx)
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, cw := range due {
				claimedBy[cw.Cron.CronID]++
			}
		}()
	}
	wg.Wait()

	total := 0
	for id, count := range claimedBy {
		if count > 1 {
			t.Errorf("cron %s claimed by %d concurrent Next() calls, want at most 1", id, count)
		}
		total += count
	}
	if total != n {
		t.Errorf("total claims across all replicas = %d, want %d (every due cron claimed exactly once)", total, n)
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
