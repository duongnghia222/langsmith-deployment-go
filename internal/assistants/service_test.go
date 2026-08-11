package assistants_test

import (
	"context"
	"testing"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	engcommon "github.com/duongnghia222/langsmith-deployment-go/gen/engine_common"
	"github.com/duongnghia222/langsmith-deployment-go/internal/assistants"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func newTestService(t *testing.T, ctx context.Context) (*assistants.Service, *pgxpool.Pool) {
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
	return assistants.NewService(pool), pool
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
	svc := assistants.NewService(pool)
	_, err = svc.Get(ctx, &coreapi.GetAssistantRequest{
		AssistantId: "00000000-0000-0000-0000-000000000000",
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

	id := testdb.MustInsertAssistant(t, ctx, pool, "my-graph", []byte(`{"k":"v"}`))
	svc := assistants.NewService(pool)
	a, err := svc.Get(ctx, &coreapi.GetAssistantRequest{AssistantId: id})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if a.GetAssistantId() != id {
		t.Errorf("AssistantId = %q, want %q", a.GetAssistantId(), id)
	}
	if a.GetGraphId() != "my-graph" {
		t.Errorf("GraphId = %q, want my-graph", a.GetGraphId())
	}
}

func TestService_Search_HappyPath(t *testing.T) {
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

	testdb.MustInsertAssistant(t, ctx, pool, "graph-search", nil)
	testdb.MustInsertAssistant(t, ctx, pool, "graph-search", nil)

	svc := assistants.NewService(pool)
	resp, err := svc.Search(ctx, &coreapi.SearchAssistantsRequest{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(resp.GetAssistants()) != 2 {
		t.Errorf("len(assistants) = %d, want 2", len(resp.GetAssistants()))
	}
}

func TestService_Count_HappyPath(t *testing.T) {
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

	testdb.MustInsertAssistant(t, ctx, pool, "graph-cnt", nil)

	svc := assistants.NewService(pool)
	resp, err := svc.Count(ctx, &coreapi.CountAssistantsRequest{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if resp.GetCount() != 1 {
		t.Errorf("Count = %d, want 1", resp.GetCount())
	}
}

func TestService_GetVersions_HappyPath(t *testing.T) {
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

	id := testdb.MustInsertAssistant(t, ctx, pool, "graph-ver", nil)
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 1, "graph-ver", nil)
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 2, "graph-ver", nil)

	svc := assistants.NewService(pool)
	resp, err := svc.GetVersions(ctx, &coreapi.GetAssistantVersionsRequest{AssistantId: id})
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	if len(resp.GetVersions()) != 2 {
		t.Errorf("len(versions) = %d, want 2", len(resp.GetVersions()))
	}
}

func TestService_GetVersions_ReturnsName(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx)

	createResp, err := svc.Create(ctx, &coreapi.CreateAssistantRequest{
		GraphId: "g-vn-svc",
		Name:    "alpha",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	resp, err := svc.GetVersions(ctx, &coreapi.GetAssistantVersionsRequest{
		AssistantId: createResp.GetAssistantId(),
	})
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	if len(resp.GetVersions()) == 0 {
		t.Fatal("expected at least one version")
	}
	if resp.GetVersions()[0].GetName() != "alpha" {
		t.Errorf("Name = %q, want alpha", resp.GetVersions()[0].GetName())
	}
}

func TestService_Create_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx)
	resp, err := svc.Create(ctx, &coreapi.CreateAssistantRequest{
		GraphId: "g-create",
		Name:    "my-assistant",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if resp.GetAssistantId() == "" {
		t.Fatal("AssistantId empty")
	}
	if resp.GetVersion() != 1 {
		t.Errorf("Version = %d, want 1", resp.GetVersion())
	}
}

func TestService_ConfigRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx)

	gid := "g-1"
	runID := "r-1"
	cfg := &engcommon.EngineRunnableConfig{
		Tags:    []string{"alpha", "beta"},
		RunName: proto.String("ingest"),
		RunId:   &runID,
		GraphId: &gid,
	}

	created, err := svc.Create(ctx, &coreapi.CreateAssistantRequest{
		GraphId: "g-1",
		Name:    "round-trip",
		Config:  cfg,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !proto.Equal(created.GetConfig(), cfg) {
		t.Fatalf("Create response config mismatch:\nwant=%v\ngot=%v", cfg, created.GetConfig())
	}

	got, err := svc.Get(ctx, &coreapi.GetAssistantRequest{AssistantId: created.GetAssistantId()})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !proto.Equal(got.GetConfig(), cfg) {
		t.Fatalf("Get response config mismatch:\nwant=%v\ngot=%v", cfg, got.GetConfig())
	}
}

func TestService_Delete_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx)
	resp, err := svc.Delete(ctx, &coreapi.DeleteAssistantRequest{
		AssistantId: "00000000-0000-0000-0000-000000000000",
	})
	if err != nil {
		t.Fatalf("Delete (not found) unexpected error: %v", err)
	}
	// Returns empty list, not an error
	if len(resp.GetAssistantIds()) != 0 {
		t.Errorf("want empty list, got %v", resp.GetAssistantIds())
	}
}

// ─── New parity tests ────────────────────────────────────────────────────────

// TestService_Create_RaiseReturnsAlreadyExists verifies that Create with RAISE
// mode returns codes.AlreadyExists on a duplicate assistant_id.
func TestService_Create_RaiseReturnsAlreadyExists(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx)
	const id = "dddddddd-dddd-dddd-dddd-dddddddddddd"

	if _, err := svc.Create(ctx, &coreapi.CreateAssistantRequest{
		AssistantId: id,
		GraphId:     "g-raise-svc",
		Name:        "first",
		IfExists:    coreapi.OnConflictBehavior_RAISE,
	}); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err := svc.Create(ctx, &coreapi.CreateAssistantRequest{
		AssistantId: id,
		GraphId:     "g-raise-svc",
		Name:        "second",
		IfExists:    coreapi.OnConflictBehavior_RAISE,
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("want codes.AlreadyExists, got %v", status.Code(err))
	}
}

// TestService_Create_DoNothingReturnsExisting verifies that Create with
// DO_NOTHING mode returns the original row on duplicate.
func TestService_Create_DoNothingReturnsExisting(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx)
	const id = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"

	first, err := svc.Create(ctx, &coreapi.CreateAssistantRequest{
		AssistantId: id,
		GraphId:     "g-dn-svc",
		Name:        "original-name",
		IfExists:    coreapi.OnConflictBehavior_DO_NOTHING,
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	second, err := svc.Create(ctx, &coreapi.CreateAssistantRequest{
		AssistantId: id,
		GraphId:     "g-dn-svc",
		Name:        "different-name",
		IfExists:    coreapi.OnConflictBehavior_DO_NOTHING,
	})
	if err != nil {
		t.Fatalf("second Create (do_nothing): %v", err)
	}
	if second.GetAssistantId() != first.GetAssistantId() {
		t.Errorf("second.AssistantId = %q, want %q", second.GetAssistantId(), first.GetAssistantId())
	}
	if second.GetName() != "original-name" {
		t.Errorf("Name = %q, want original-name (do_nothing must return existing)", second.GetName())
	}
}

// TestService_GetVersions_MetadataFilter verifies that GetVersions plumbs the
// metadata_json filter through — ops.py:599.
func TestService_GetVersions_MetadataFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	id := testdb.MustInsertAssistant(t, ctx, pool, "graph-mf-svc", nil)
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 1, "graph-mf-svc", []byte(`{"tier":"free"}`))
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 2, "graph-mf-svc", []byte(`{"tier":"pro"}`))
	testdb.MustInsertAssistantVersion(t, ctx, pool, id, 3, "graph-mf-svc", []byte(`{"tier":"free"}`))

	resp, err := svc.GetVersions(ctx, &coreapi.GetAssistantVersionsRequest{
		AssistantId:  id,
		MetadataJson: []byte(`{"tier":"free"}`),
	})
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	if len(resp.GetVersions()) != 2 {
		t.Errorf("len(versions) = %d, want 2 (tier=free)", len(resp.GetVersions()))
	}
}

// TestService_SetLatest_PreservesNonRestoredFields verifies that SetLatest only
// restores config/metadata/version and leaves graph_id/context/description intact.
func TestService_SetLatest_PreservesNonRestoredFields(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, _ := newTestService(t, ctx)

	// Create v1 with graph-original.
	created, err := svc.Create(ctx, &coreapi.CreateAssistantRequest{
		GraphId: "graph-orig-svc",
		Name:    "setlatest-test",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	id := created.GetAssistantId()

	// Patch to v2: change graph_id.
	desc := "patched"
	if _, err := svc.Patch(ctx, &coreapi.PatchAssistantRequest{
		AssistantId: id,
		GraphId:     proto.String("graph-patched-svc"),
		Description: &desc,
	}); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	// Roll back to v1.
	rolled, err := svc.SetLatest(ctx, &coreapi.SetLatestAssistantRequest{
		AssistantId: id,
		Version:     1,
	})
	if err != nil {
		t.Fatalf("SetLatest: %v", err)
	}
	// Version pointer reverts.
	if rolled.GetVersion() != 1 {
		t.Errorf("Version = %d, want 1", rolled.GetVersion())
	}
	// graph_id must remain from the latest patch, not revert.
	if rolled.GetGraphId() != "graph-patched-svc" {
		t.Errorf("GraphId = %q, want graph-patched-svc (must not revert graph_id)", rolled.GetGraphId())
	}
}
