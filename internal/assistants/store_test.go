package assistants_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/assistants"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestStore(t *testing.T, ctx context.Context) (*assistants.Store, *pgxpool.Pool) {
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
	return assistants.NewStore(pool), pool
}

func TestStore_Get_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	_, err := store.Get(ctx, "00000000-0000-0000-0000-000000000000", nil)
	if err == nil || !errors.Is(err, assistants.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestStore_Get_ReturnsRow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	id := testdb.MustInsertAssistant(t, ctx, pool, "my-graph", []byte(`{"owner":"alice"}`))

	a, err := store.Get(ctx, id, nil)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if a.AssistantID != id {
		t.Errorf("AssistantID = %q, want %q", a.AssistantID, id)
	}
	if a.GraphID != "my-graph" {
		t.Errorf("GraphID = %q, want my-graph", a.GraphID)
	}
	// PostgreSQL normalizes JSONB (e.g. adds spaces), so compare semantically.
	var got, want map[string]any
	if err := json.Unmarshal(a.Metadata, &got); err != nil {
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

	testdb.MustInsertAssistant(t, ctx, pool, "graph-a", []byte(`{"team":"red"}`))
	testdb.MustInsertAssistant(t, ctx, pool, "graph-b", []byte(`{"team":"blue"}`))
	testdb.MustInsertAssistant(t, ctx, pool, "graph-a", []byte(`{"team":"red"}`))

	// All results
	results, err := store.Search(ctx, assistants.SearchInput{Limit: 10}, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("len(results) = %d, want 3", len(results))
	}

	// Filter by graph_id
	filtered, err := store.Search(ctx, assistants.SearchInput{GraphID: "graph-a", Limit: 10}, nil)
	if err != nil {
		t.Fatalf("Search by graph_id: %v", err)
	}
	if len(filtered) != 2 {
		t.Errorf("len(filtered by graph-a) = %d, want 2", len(filtered))
	}

	// Pagination: limit 1
	paged, err := store.Search(ctx, assistants.SearchInput{Limit: 1, Offset: 0}, nil)
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

	testdb.MustInsertAssistant(t, ctx, pool, "graph-count", nil)
	testdb.MustInsertAssistant(t, ctx, pool, "graph-count", nil)

	n, err := store.Count(ctx, assistants.SearchInput{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 2 {
		t.Errorf("Count = %d, want 2", n)
	}

	// Count with graph_id filter
	nFiltered, err := store.Count(ctx, assistants.SearchInput{GraphID: "graph-count"}, nil)
	if err != nil {
		t.Fatalf("Count filtered: %v", err)
	}
	if nFiltered != 2 {
		t.Errorf("Count(graph-count) = %d, want 2", nFiltered)
	}
}

func TestStore_GetVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	id := testdb.MustInsertAssistant(t, ctx, pool, "graph-v", nil)
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 1, "graph-v", []byte(`{"v":1}`))
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 2, "graph-v", []byte(`{"v":2}`))

	versions, err := store.GetVersions(ctx, id, 10, 0, nil, nil)
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
	// Ordered by version DESC: first should be v=2
	if versions[0].Version != 2 {
		t.Errorf("versions[0].Version = %d, want 2", versions[0].Version)
	}
	if versions[1].Version != 1 {
		t.Errorf("versions[1].Version = %d, want 1", versions[1].Version)
	}

	// GetVersions on non-existent assistant returns empty slice, no error.
	empty, err := store.GetVersions(ctx, "00000000-0000-0000-0000-000000000000", 10, 0, nil, nil)
	if err != nil {
		t.Errorf("GetVersions on missing assistant: want nil error, got %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("GetVersions on missing assistant: want empty slice, got %d rows", len(empty))
	}
}

// TestStore_GetVersions_RespectsAuth proves GetVersions' auth filter is
// applied against the PARENT ASSISTANT's metadata (ops.py:588-600 joins
// assistant USING(assistant_id) and filters assistant.metadata), not each
// version row's own metadata. A matching assistant makes ALL of its versions
// visible; a mismatching assistant hides ALL of them, even though the
// per-version metadata never enters into it.
func TestStore_GetVersions_RespectsAuth(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	// Parent assistant's own metadata is the auth filter target.
	id := testdb.MustInsertAssistant(t, ctx, pool, "graph-auth", []byte(`{"tenant":"alpha"}`))
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 1, "graph-auth", []byte(`{"v":1}`))
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 2, "graph-auth", []byte(`{"v":2}`))

	// Auth filter matches the parent assistant's metadata: both versions visible.
	filters := []*coreapi.AuthFilter{
		{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "tenant", Match: `"alpha"`}}},
	}
	versions, err := store.GetVersions(ctx, id, 10, 0, nil, filters)
	if err != nil {
		t.Fatalf("GetVersions with auth filter: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2 (assistant's own metadata matches)", len(versions))
	}

	// Auth filter mismatching the parent assistant's metadata hides ALL versions,
	// regardless of any per-version metadata content.
	noMatch := []*coreapi.AuthFilter{
		{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "tenant", Match: `"beta"`}}},
	}
	none, err := store.GetVersions(ctx, id, 10, 0, nil, noMatch)
	if err != nil {
		t.Fatalf("GetVersions no-match: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("len(none) = %d, want 0", len(none))
	}
}

func TestStore_Create_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	_ = pool
	a, err := store.Create(ctx, assistants.CreateInput{
		GraphID:  "g1",
		Name:     "test-assistant",
		Metadata: []byte(`{"owner":"alice"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if a.AssistantID == "" {
		t.Fatal("AssistantID empty")
	}
	if a.GraphID != "g1" {
		t.Errorf("GraphID = %q, want g1", a.GraphID)
	}
	if a.Version != 1 {
		t.Errorf("Version = %d, want 1", a.Version)
	}
	// Get it back
	got, err := store.Get(ctx, a.AssistantID, nil)
	if err != nil {
		t.Fatalf("Get after Create: %v", err)
	}
	if got.Name != "test-assistant" {
		t.Errorf("Name = %q, want test-assistant", got.Name)
	}
}

func TestStore_Create_WritesVersionName(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	a, err := store.Create(ctx, assistants.CreateInput{
		GraphID: "g-vn",
		Name:    "my-assistant",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	versions, err := store.GetVersions(ctx, a.AssistantID, 10, 0, nil, nil)
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("len(versions) = %d, want 1", len(versions))
	}
	if versions[0].Name != "my-assistant" {
		t.Errorf("Name = %q, want my-assistant", versions[0].Name)
	}
}

func TestStore_Patch_WritesVersionName(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	a, err := store.Create(ctx, assistants.CreateInput{
		GraphID: "g-vn-patch",
		Name:    "original",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	renamedName := "renamed"
	_, err = store.Patch(ctx, a.AssistantID, assistants.PatchInput{Name: &renamedName}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	versions, err := store.GetVersions(ctx, a.AssistantID, 10, 0, nil, nil)
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	// Versions are ordered DESC, so versions[0] is v2 (the patched one).
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
	if versions[0].Version != 2 {
		t.Errorf("versions[0].Version = %d, want 2 (DESC order)", versions[0].Version)
	}
	if versions[0].Name != "renamed" {
		t.Errorf("versions[0].Name = %q, want renamed", versions[0].Name)
	}
	if versions[1].Name != "original" {
		t.Errorf("versions[1].Name = %q, want original", versions[1].Name)
	}
}

func TestStore_Patch_BumpsVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	a, err := store.Create(ctx, assistants.CreateInput{GraphID: "g1", Name: "original"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	patchedName := "patched"
	patched, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{Name: &patchedName}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if patched.Name != "patched" {
		t.Errorf("Name = %q, want patched", patched.Name)
	}
	if patched.Version != 2 {
		t.Errorf("Version = %d, want 2", patched.Version)
	}
}

func TestStore_Delete_RemovesRow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g1", nil)
	deleted, err := store.Delete(ctx, aID, false, nil)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(deleted) != 1 || deleted[0] != aID {
		t.Errorf("deleted IDs = %v, want [%s]", deleted, aID)
	}
	_, err = store.Get(ctx, aID, nil)
	if !errors.Is(err, assistants.ErrNotFound) {
		t.Errorf("after delete Get = %v, want ErrNotFound", err)
	}
}

// TestStore_Delete_NoMatch_ReturnsEmpty verifies Delete on a nonexistent ID
// returns an empty slice with no error, not ErrNotFound — the Go-service
// contract (Service.Delete always returns a list, empty or not); the Python
// client (5b) is responsible for turning an empty list into a 404.
func TestStore_Delete_NoMatch_ReturnsEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	deleted, err := store.Delete(ctx, "00000000-0000-0000-0000-000000000000", false, nil)
	if err != nil {
		t.Fatalf("Delete on missing id: want nil error, got %v", err)
	}
	if len(deleted) != 0 {
		t.Errorf("deleted = %v, want empty", deleted)
	}
}

// TestStore_Delete_ThreadsFlag verifies delete_threads (5b, LSD-only — no
// Python parity): when true, threads tagged with the assistant_id via
// metadata->>'assistant_id' are also removed in the same transaction; when
// false (default), threads are left untouched.
func TestStore_Delete_ThreadsFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	// Assistant whose threads WILL be swept.
	aID := testdb.MustInsertAssistant(t, ctx, pool, "g-threads", nil)
	threadID := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"assistant_id":"`+aID+`"}`))

	// Unrelated assistant + thread that must survive untouched.
	otherID := testdb.MustInsertAssistant(t, ctx, pool, "g-threads-other", nil)
	otherThreadID := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"assistant_id":"`+otherID+`"}`))

	deleted, err := store.Delete(ctx, aID, true, nil)
	if err != nil {
		t.Fatalf("Delete with delete_threads: %v", err)
	}
	if len(deleted) != 1 || deleted[0] != aID {
		t.Errorf("deleted IDs = %v, want [%s]", deleted, aID)
	}

	threadExists := func(id string) bool {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM thread WHERE thread_id = $1::uuid)`, id).Scan(&exists); err != nil {
			t.Fatalf("check thread existence: %v", err)
		}
		return exists
	}
	if threadExists(threadID) {
		t.Errorf("thread %s still exists after delete_threads=true", threadID)
	}
	if !threadExists(otherThreadID) {
		t.Errorf("unrelated thread %s was removed; delete_threads must scope to this assistant only", otherThreadID)
	}
}

func TestStore_SetLatest_RollsBackVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	a, err := store.Create(ctx, assistants.CreateInput{GraphID: "g1", Name: "v1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// create version 2 via patch
	v2Name := "v2"
	if _, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{Name: &v2Name}, nil); err != nil {
		t.Fatalf("Patch: %v", err)
	}
	// roll back to version 1
	rolled, err := store.SetLatest(ctx, a.AssistantID, 1, nil)
	if err != nil {
		t.Fatalf("SetLatest: %v", err)
	}
	if rolled.Version != 1 {
		t.Errorf("Version = %d, want 1", rolled.Version)
	}
}

// Regression: Patch after SetLatest must compute the next version from
// MAX(assistant_versions.version)+1, not from assistant.version+1, otherwise
// it collides with an existing version row.
func TestStore_PatchAfterSetLatest_NoConflict(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	a, err := store.Create(ctx, assistants.CreateInput{GraphID: "g1", Name: "v1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// versions = [1]
	name := "v2"
	if _, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{Name: &name}, nil); err != nil {
		t.Fatalf("Patch v2: %v", err)
	}
	// versions = [1, 2]
	name = "v3"
	if _, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{Name: &name}, nil); err != nil {
		t.Fatalf("Patch v3: %v", err)
	}
	// versions = [1, 2, 3]
	if _, err := store.SetLatest(ctx, a.AssistantID, 1, nil); err != nil {
		t.Fatalf("SetLatest: %v", err)
	}
	// versions = [1, 2, 3] (preserved); assistant.version = 1
	name = "v4"
	final, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{Name: &name}, nil)
	if err != nil {
		t.Fatalf("Patch after SetLatest: %v", err)
	}
	if final.Version != 4 {
		t.Errorf("Version = %d, want 4 (MAX(1,2,3)+1)", final.Version)
	}
}

// ─── New parity tests ────────────────────────────────────────────────────────

// TestStore_Patch_MetadataMerge verifies that Patch merges metadata rather than
// replacing it — ops.py:455: metadata = assistant.metadata || %(metadata)s.
func TestStore_Patch_MetadataMerge(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	a, err := store.Create(ctx, assistants.CreateInput{
		GraphID:  "g-meta",
		Name:     "meta-test",
		Metadata: []byte(`{"owner":"alice","env":"prod"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Patch adds a new key and changes an existing one.
	patched, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{
		Metadata: []byte(`{"env":"staging","region":"us-east-1"}`),
	}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}

	// assistant row must have merged metadata.
	var got map[string]any
	if err := json.Unmarshal(patched.Metadata, &got); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	// "owner" from original must be preserved.
	if got["owner"] != "alice" {
		t.Errorf("metadata[owner] = %v, want alice (merge must preserve original keys)", got["owner"])
	}
	// "env" must be overwritten by patch.
	if got["env"] != "staging" {
		t.Errorf("metadata[env] = %v, want staging", got["env"])
	}
	// "region" must be added by patch.
	if got["region"] != "us-east-1" {
		t.Errorf("metadata[region] = %v, want us-east-1", got["region"])
	}
}

// TestStore_Patch_VersionRowMetadata verifies that the inserted version row also
// has merged metadata (ops.py:477-479).
func TestStore_Patch_VersionRowMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	a, err := store.Create(ctx, assistants.CreateInput{
		GraphID:  "g-vmeta",
		Name:     "vmeta-test",
		Metadata: []byte(`{"base":"yes"}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, err = store.Patch(ctx, a.AssistantID, assistants.PatchInput{
		Metadata: []byte(`{"extra":"val"}`),
	}, nil); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	versions, err := store.GetVersions(ctx, a.AssistantID, 10, 0, nil, nil)
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
	// Version 2 (the patched row, desc order) must carry merged metadata.
	var got map[string]any
	if err := json.Unmarshal(versions[0].Metadata, &got); err != nil {
		t.Fatalf("unmarshal version metadata: %v", err)
	}
	if got["base"] != "yes" {
		t.Errorf("version metadata[base] = %v, want yes", got["base"])
	}
	if got["extra"] != "val" {
		t.Errorf("version metadata[extra] = %v, want val", got["extra"])
	}
}

// TestStore_Patch_EmptyStringClearsNameAndDescription verifies (5e) that an
// explicit "" is applied, not treated as absent — ops.py:457-461 use
// `if name is not None` / `if description is not None`, so unlike the zero
// value of a non-pointer field, PatchInput.Name/Description being non-nil
// (even "") must overwrite the existing value.
func TestStore_Patch_EmptyStringClearsNameAndDescription(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)

	desc := "original-desc"
	a, err := store.Create(ctx, assistants.CreateInput{
		GraphID:     "g-clear",
		Name:        "original-name",
		Description: &desc,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	empty := ""
	patched, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{
		Name:        &empty,
		Description: &empty,
	}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if patched.Name != "" {
		t.Errorf("Name = %q, want empty string (explicit clear)", patched.Name)
	}
	if patched.Description == nil || *patched.Description != "" {
		t.Errorf("Description = %v, want empty string (explicit clear)", patched.Description)
	}
}

// TestStore_Patch_NilNameLeavesExisting verifies that PatchInput.Name == nil
// (not provided) leaves the existing name untouched, distinguishing "unset"
// from "explicitly cleared" (5e).
func TestStore_Patch_NilNameLeavesExisting(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)

	a, err := store.Create(ctx, assistants.CreateInput{GraphID: "g-nilname", Name: "keep-me"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	patched, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{
		GraphID: "g-nilname-2",
	}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if patched.Name != "keep-me" {
		t.Errorf("Name = %q, want keep-me (nil Name must not touch existing name)", patched.Name)
	}
}

// TestStore_Patch_UpdatedAtMatchesVersionCreatedAt verifies that the assistant's
// updated_at equals the version row's created_at — ops.py:488.
func TestStore_Patch_UpdatedAtMatchesVersionCreatedAt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	a, err := store.Create(ctx, assistants.CreateInput{GraphID: "g-ts", Name: "ts-test"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	renamed := "renamed"
	patched, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{Name: &renamed}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}

	versions, err := store.GetVersions(ctx, a.AssistantID, 10, 0, nil, nil)
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	// versions[0] is the newest (v2) because GetVersions is ordered DESC.
	if len(versions) < 1 {
		t.Fatal("no versions returned")
	}
	v2 := versions[0]
	if !patched.UpdatedAt.Equal(v2.CreatedAt) {
		t.Errorf("assistant.updated_at (%v) != version.created_at (%v); must be equal (ops.py:488)",
			patched.UpdatedAt, v2.CreatedAt)
	}
}

// TestStore_Create_AtomicDoNothing verifies that concurrent/repeated Create with
// do_nothing returns the existing row without error (atomic, no TOCTOU).
func TestStore_Create_AtomicDoNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	const id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

	first, err := store.Create(ctx, assistants.CreateInput{
		AssistantID: id,
		GraphID:     "g-atomic",
		Name:        "original",
		IfExists:    "do_nothing",
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	// Second create with same ID and do_nothing must return the ORIGINAL row, no error.
	second, err := store.Create(ctx, assistants.CreateInput{
		AssistantID: id,
		GraphID:     "g-atomic",
		Name:        "should-be-ignored",
		IfExists:    "do_nothing",
	})
	if err != nil {
		t.Fatalf("second Create (do_nothing): %v", err)
	}
	if second.AssistantID != first.AssistantID {
		t.Errorf("second.AssistantID = %q, want %q", second.AssistantID, first.AssistantID)
	}
	if second.Name != "original" {
		t.Errorf("second.Name = %q, want original (do_nothing must return existing row)", second.Name)
	}
}

// TestStore_Create_DoNothing_AuthFilters proves auth filters are applied to
// the do_nothing "return pre-existing row" leg (ops.py:356-371): a matching
// filter returns the existing assistant, a mismatching filter surfaces the
// same ErrAlreadyExists as an unfiltered conflict (ops.py's fetchone(...,
// not_found_code=409) treats "no row" identically whether caused by the
// conflict itself or by the filter excluding it).
func TestStore_Create_DoNothing_AuthFilters(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	const id = "dddddddd-dddd-dddd-dddd-dddddddddddd"

	if _, err := store.Create(ctx, assistants.CreateInput{
		AssistantID: id,
		GraphID:     "g-donothing-auth",
		Name:        "original",
		Metadata:    []byte(`{"owner":"alice"}`),
		IfExists:    "do_nothing",
	}); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	matching := []*coreapi.AuthFilter{
		{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "owner", Match: `"alice"`}}},
	}
	second, err := store.Create(ctx, assistants.CreateInput{
		AssistantID: id,
		GraphID:     "g-donothing-auth",
		Name:        "should-be-ignored",
		IfExists:    "do_nothing",
		Filters:     matching,
	})
	if err != nil {
		t.Fatalf("do_nothing with matching filter: %v", err)
	}
	if second.Name != "original" {
		t.Errorf("second.Name = %q, want original", second.Name)
	}

	mismatching := []*coreapi.AuthFilter{
		{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "owner", Match: `"bob"`}}},
	}
	_, err = store.Create(ctx, assistants.CreateInput{
		AssistantID: id,
		GraphID:     "g-donothing-auth",
		Name:        "should-be-ignored",
		IfExists:    "do_nothing",
		Filters:     mismatching,
	})
	if !errors.Is(err, assistants.ErrAlreadyExists) {
		t.Errorf("do_nothing with mismatching filter: want ErrAlreadyExists, got %v", err)
	}
}

// TestStore_Create_AtomicRaise verifies that Create with raise mode returns
// ErrAlreadyExists on conflict — ops.py:372-374.
func TestStore_Create_AtomicRaise(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)
	const id = "cccccccc-cccc-cccc-cccc-cccccccccccc"

	if _, err := store.Create(ctx, assistants.CreateInput{
		AssistantID: id,
		GraphID:     "g-raise",
		Name:        "first",
		IfExists:    "raise",
	}); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err := store.Create(ctx, assistants.CreateInput{
		AssistantID: id,
		GraphID:     "g-raise",
		Name:        "second",
		IfExists:    "raise",
	})
	if !errors.Is(err, assistants.ErrAlreadyExists) {
		t.Errorf("want ErrAlreadyExists, got %v", err)
	}
}

// TestStore_SetLatest_PreservesGraphIDContextDescription verifies that SetLatest
// does NOT overwrite graph_id, context, or description — ops.py:556-560.
func TestStore_SetLatest_PreservesGraphIDContextDescription(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)

	// Create v1.
	a, err := store.Create(ctx, assistants.CreateInput{
		GraphID: "graph-original",
		Name:    "v1",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Patch to v2: change graph_id and description (context stays default).
	desc := "patched-desc"
	if _, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{
		GraphID:     "graph-patched",
		Description: &desc,
	}, nil); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	// Roll back to v1 — graph_id/description on the assistant row must NOT revert.
	rolled, err := store.SetLatest(ctx, a.AssistantID, 1, nil)
	if err != nil {
		t.Fatalf("SetLatest: %v", err)
	}
	// version pointer reverts.
	if rolled.Version != 1 {
		t.Errorf("Version = %d, want 1", rolled.Version)
	}
	// graph_id must remain as-is from the latest patch (not reverted to v1's graph_id).
	if rolled.GraphID != "graph-patched" {
		t.Errorf("GraphID = %q, want graph-patched (SetLatest must not revert graph_id)", rolled.GraphID)
	}
	// description must remain as-is.
	if rolled.Description == nil || *rolled.Description != "patched-desc" {
		t.Errorf("Description = %v, want patched-desc (SetLatest must not revert description)", rolled.Description)
	}
}

// TestStore_SetLatest_DoesNotBumpUpdatedAt verifies (5d) that SetLatest's SET
// clause has no updated_at — ops.py:556-563 restores config/metadata/version
// only, so rolling back a version must not look like a fresh edit.
func TestStore_SetLatest_DoesNotBumpUpdatedAt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, _ := newTestStore(t, ctx)

	a, err := store.Create(ctx, assistants.CreateInput{GraphID: "g-ts2", Name: "v1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	name := "v2"
	patched, err := store.Patch(ctx, a.AssistantID, assistants.PatchInput{Name: &name}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	beforeUpdatedAt := patched.UpdatedAt

	// Sleep past typical timestamp resolution so a spurious now() bump would
	// be detectable rather than accidentally landing on the same instant.
	time.Sleep(10 * time.Millisecond)

	rolled, err := store.SetLatest(ctx, a.AssistantID, 1, nil)
	if err != nil {
		t.Fatalf("SetLatest: %v", err)
	}
	if !rolled.UpdatedAt.Equal(beforeUpdatedAt) {
		t.Errorf("updated_at = %v, want unchanged %v (SetLatest must not bump updated_at)", rolled.UpdatedAt, beforeUpdatedAt)
	}
}

// TestStore_GetVersions_MetadataFilter verifies that GetVersions honours the
// metadata containment filter — ops.py:599.
func TestStore_GetVersions_MetadataFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	id := testdb.MustInsertAssistant(t, ctx, pool, "graph-mfv", nil)
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 1, "graph-mfv", []byte(`{"env":"prod","ver":1}`))
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 2, "graph-mfv", []byte(`{"env":"staging","ver":2}`))
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 3, "graph-mfv", []byte(`{"env":"prod","ver":3}`))

	// Filter: only "env":"prod" versions.
	filter := []byte(`{"env":"prod"}`)
	versions, err := store.GetVersions(ctx, id, 10, 0, filter, nil)
	if err != nil {
		t.Fatalf("GetVersions with metadata filter: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2 (env=prod has v1 and v3)", len(versions))
	}
	// Results ordered DESC — first should be v3.
	if versions[0].Version != 3 {
		t.Errorf("versions[0].Version = %d, want 3", versions[0].Version)
	}
	if versions[1].Version != 1 {
		t.Errorf("versions[1].Version = %d, want 1", versions[1].Version)
	}
}

// TestStore_Search_SortBy verifies that the sort_by/sort_order whitelist is
// respected and the default is created_at DESC — ops.py:184-200.
func TestStore_Search_SortBy(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	store, pool := newTestStore(t, ctx)

	testdb.MustInsertAssistant(t, ctx, pool, "graph-sort", nil)
	testdb.MustInsertAssistant(t, ctx, pool, "graph-sort", nil)
	testdb.MustInsertAssistant(t, ctx, pool, "graph-sort", nil)

	// Default sort (no sort_by): must return results without error.
	results, err := store.Search(ctx, assistants.SearchInput{Limit: 10}, nil)
	if err != nil {
		t.Fatalf("Search default sort: %v", err)
	}
	if len(results) < 3 {
		t.Errorf("expected at least 3 results, got %d", len(results))
	}

	// Explicit valid sort_by: updated_at ASC.
	resultsAsc, err := store.Search(ctx, assistants.SearchInput{
		Limit:     10,
		SortBy:    "updated_at",
		SortOrder: "asc",
	}, nil)
	if err != nil {
		t.Fatalf("Search sort updated_at asc: %v", err)
	}
	if len(resultsAsc) < 3 {
		t.Errorf("expected at least 3 results with sort, got %d", len(resultsAsc))
	}
	// Verify ASC order: each updated_at must be <= next.
	for i := 1; i < len(resultsAsc); i++ {
		if resultsAsc[i-1].UpdatedAt.After(resultsAsc[i].UpdatedAt) {
			t.Errorf("results not in ASC updated_at order at index %d", i)
		}
	}

	// Invalid sort_by falls back to created_at DESC (no error).
	_, err = store.Search(ctx, assistants.SearchInput{
		Limit:  10,
		SortBy: "injected; DROP TABLE assistant; --",
	}, nil)
	if err != nil {
		t.Fatalf("Search invalid sort_by should fall back, got error: %v", err)
	}
}
