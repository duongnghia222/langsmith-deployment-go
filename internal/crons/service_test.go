package crons_test

import (
	"context"
	"testing"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	enumcronorc "github.com/duongnghia222/langsmith-deployment-go/gen/enum_cron_on_run_completed"
	"github.com/duongnghia222/langsmith-deployment-go/internal/crons"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestService(t *testing.T, ctx context.Context) (*crons.Service, *pgxpool.Pool) {
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
	return crons.NewService(pool), pool
}

func TestService_Search_HappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-cron-graph", nil)
	testdb.MustInsertCron(t, ctx, pool, aID, "* * * * *")
	testdb.MustInsertCron(t, ctx, pool, aID, "0 * * * *")

	resp, err := svc.Search(ctx, &coreapi.SearchCronsRequest{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(resp.GetCrons()) != 2 {
		t.Errorf("len(crons) = %d, want 2", len(resp.GetCrons()))
	}
}

func TestService_Search_FilterByAssistant(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-filter-graph-a", nil)
	aID2 := testdb.MustInsertAssistant(t, ctx, pool, "svc-filter-graph-b", nil)
	testdb.MustInsertCron(t, ctx, pool, aID, "* * * * *")
	testdb.MustInsertCron(t, ctx, pool, aID2, "0 0 * * *")

	resp, err := svc.Search(ctx, &coreapi.SearchCronsRequest{
		AssistantId: &coreapi.UUID{Value: aID},
	})
	if err != nil {
		t.Fatalf("Search filtered: %v", err)
	}
	if len(resp.GetCrons()) != 1 {
		t.Errorf("len(crons) = %d, want 1", len(resp.GetCrons()))
	}
	if resp.GetCrons()[0].GetAssistantId() != aID {
		t.Errorf("AssistantId = %q, want %q", resp.GetCrons()[0].GetAssistantId(), aID)
	}
}

func TestService_Count_HappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-count-graph", nil)
	testdb.MustInsertCron(t, ctx, pool, aID, "* * * * *")

	resp, err := svc.Count(ctx, &coreapi.CountCronsRequest{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if resp.GetCount() != 1 {
		t.Errorf("Count = %d, want 1", resp.GetCount())
	}
}

func TestService_Count_FilterByAssistant(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-count-filter-a", nil)
	aID2 := testdb.MustInsertAssistant(t, ctx, pool, "svc-count-filter-b", nil)
	testdb.MustInsertCron(t, ctx, pool, aID, "* * * * *")
	testdb.MustInsertCron(t, ctx, pool, aID, "0 * * * *")
	testdb.MustInsertCron(t, ctx, pool, aID2, "0 0 * * *")

	resp, err := svc.Count(ctx, &coreapi.CountCronsRequest{
		AssistantId: &coreapi.UUID{Value: aID},
	})
	if err != nil {
		t.Fatalf("Count filtered: %v", err)
	}
	if resp.GetCount() != 2 {
		t.Errorf("Count(assistant) = %d, want 2", resp.GetCount())
	}
}

func TestService_PayloadRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "g-payload", nil)

	webhook := "https://hook.invalid"
	payload := &coreapi.CronPayload{
		AssistantId: aID,
		InputJson:   []byte(`{"q":"hello"}`),
		Webhook:     &webhook,
		ExtraJson: map[string][]byte{
			"feedback_keys": []byte(`["k1"]`),
		},
	}

	created, err := svc.Create(ctx, &coreapi.CreateCronRequest{
		Schedule: "* * * * *",
		Payload:  payload,
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !proto.Equal(created.GetPayload(), payload) {
		t.Fatalf("Create payload mismatch:\nwant=%v\ngot=%v", payload, created.GetPayload())
	}

	got, err := svc.Search(ctx, &coreapi.SearchCronsRequest{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got.GetCrons()) != 1 {
		t.Fatalf("expected 1 cron, got %d", len(got.GetCrons()))
	}
	if !proto.Equal(got.GetCrons()[0].GetPayload(), payload) {
		t.Fatalf("Search payload mismatch:\nwant=%v\ngot=%v", payload, got.GetCrons()[0].GetPayload())
	}
}

func TestService_OnRunCompletedRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "g-onruncompleted", nil)

	orc := enumcronorc.CronOnRunCompleted_keep
	created, err := svc.Create(ctx, &coreapi.CreateCronRequest{
		Schedule: "* * * * *",
		Payload: &coreapi.CronPayload{
			AssistantId: aID,
		},
		Enabled:        true,
		OnRunCompleted: &orc,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.OnRunCompleted == nil {
		t.Fatalf("Create: OnRunCompleted is nil")
	}
	if got := *created.OnRunCompleted; got != enumcronorc.CronOnRunCompleted_keep {
		t.Errorf("Create OnRunCompleted = %v, want keep", got)
	}

	got, err := svc.Search(ctx, &coreapi.SearchCronsRequest{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got.GetCrons()) != 1 {
		t.Fatalf("expected 1 cron, got %d", len(got.GetCrons()))
	}
	if got.GetCrons()[0].OnRunCompleted == nil {
		t.Fatalf("Search: OnRunCompleted is nil")
	}
	if v := *got.GetCrons()[0].OnRunCompleted; v != enumcronorc.CronOnRunCompleted_keep {
		t.Errorf("Search OnRunCompleted = %v, want keep", v)
	}

	// Patch to delete.
	orcDel := enumcronorc.CronOnRunCompleted_delete
	patched, err := svc.Patch(ctx, &coreapi.PatchCronRequest{
		CronId:         created.GetCronId(),
		OnRunCompleted: &orcDel,
	})
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if patched.OnRunCompleted == nil {
		t.Fatalf("Patch: OnRunCompleted is nil")
	}
	if v := *patched.OnRunCompleted; v != enumcronorc.CronOnRunCompleted_delete {
		t.Errorf("Patch OnRunCompleted = %v, want delete", v)
	}
}

// TestService_Search_ThreadFilters verifies that thread_filters are accepted and
// correctly constrain results via a LEFT JOIN with the thread table.
// Crons with a matching thread pass; crons with no thread (NULL thread_id) are
// exempt from thread_filters entirely (ops.py:2440-2442: "cron.thread_id IS
// NULL OR (...)") and are therefore ALSO included.
func TestService_Search_ThreadFilters(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "tf-search-graph", nil)

	// Cron with a thread whose metadata matches the filter.
	tID := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"role":"admin"}`))
	testdb.MustInsertCronWithThread(t, ctx, pool, aID, tID, "* * * * *")

	// Cron with a thread that does NOT match the filter.
	tID2 := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"role":"user"}`))
	testdb.MustInsertCronWithThread(t, ctx, pool, aID, tID2, "0 * * * *")

	// Cron with no thread — exempt from thread_filters, always included.
	testdb.MustInsertCron(t, ctx, pool, aID, "0 0 * * *")

	resp, err := svc.Search(ctx, &coreapi.SearchCronsRequest{
		ThreadFilters: []*coreapi.AuthFilter{
			{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "role", Match: `"admin"`}}},
		},
	})
	if err != nil {
		t.Fatalf("Search with thread_filters: %v", err)
	}
	if len(resp.GetCrons()) != 2 {
		t.Errorf("len(crons) = %d, want 2 (matching thread + no-thread cron)", len(resp.GetCrons()))
	}
}

// TestService_Count_ThreadFilters verifies that thread_filters are accepted and
// correctly constrain the count via a LEFT JOIN with the thread table, exempting
// no-thread crons from the filter (ops.py:2440-2442).
func TestService_Count_ThreadFilters(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "tf-count-graph", nil)

	// Cron with a thread whose metadata matches the filter.
	tID := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"role":"admin"}`))
	testdb.MustInsertCronWithThread(t, ctx, pool, aID, tID, "* * * * *")

	// Cron with a thread that does NOT match the filter.
	tID2 := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"role":"user"}`))
	testdb.MustInsertCronWithThread(t, ctx, pool, aID, tID2, "0 * * * *")

	// Cron with no thread — exempt from thread_filters, always included.
	testdb.MustInsertCron(t, ctx, pool, aID, "0 0 * * *")

	resp, err := svc.Count(ctx, &coreapi.CountCronsRequest{
		ThreadFilters: []*coreapi.AuthFilter{
			{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "role", Match: `"admin"`}}},
		},
	})
	if err != nil {
		t.Fatalf("Count with thread_filters: %v", err)
	}
	if resp.GetCount() != 2 {
		t.Errorf("Count(thread_filters) = %d, want 2 (matching thread + no-thread cron)", resp.GetCount())
	}
}

// TestService_Next_PopulatesNow verifies that Service.Next returns at least one
// due cron and that the CronWithNow.Now field is non-nil.
func TestService_Next_PopulatesNow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-next-now-graph", nil)
	cronID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)
	// Set next_run_date to the past so it is due now.
	if _, err := pool.Exec(ctx,
		`UPDATE cron SET next_run_date = now() - interval '1 second' WHERE cron_id = $1::uuid`, cronID,
	); err != nil {
		t.Fatalf("set next_run_date: %v", err)
	}

	resp, err := svc.Next(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(resp.GetCrons()) == 0 {
		t.Fatal("Next returned 0 crons, want at least 1")
	}
	if resp.GetCrons()[0].GetNow() == nil {
		t.Error("CronWithNow.Now is nil, want non-nil timestamp")
	}
}

// TestService_Next_ExcludesExpired verifies that Next() skips crons whose
// end_time has passed (keep-verbatim: end_time IS NULL OR end_time >= now()).
func TestService_Next_ExcludesExpired(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-next-expire-graph", nil)

	expiredID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)
	if _, err := pool.Exec(ctx,
		`UPDATE cron SET next_run_date = now() - interval '1 second', end_time = now() - interval '1 second' WHERE cron_id = $1::uuid`,
		expiredID,
	); err != nil {
		t.Fatalf("set expired: %v", err)
	}

	activeID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "0 * * * *", true)
	if _, err := pool.Exec(ctx,
		`UPDATE cron SET next_run_date = now() - interval '1 second', end_time = now() + interval '1 hour' WHERE cron_id = $1::uuid`,
		activeID,
	); err != nil {
		t.Fatalf("set active: %v", err)
	}

	resp, err := svc.Next(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	ids := make(map[string]struct{})
	for _, cw := range resp.GetCrons() {
		ids[cw.GetCron().GetCronId().GetValue()] = struct{}{}
	}
	if _, ok := ids[expiredID]; ok {
		t.Errorf("expired cron %s should NOT be returned by Next", expiredID)
	}
	if _, ok := ids[activeID]; !ok {
		t.Errorf("active cron %s should be returned by Next", activeID)
	}
}

// TestService_Create_AuthFilters_NotFound verifies that Create with non-matching
// assistant_filters returns codes.NotFound (ops.py:2279: "Thread not found or not authorized").
func TestService_Create_AuthFilters_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-auth-notfound-graph", []byte(`{"owner":"bob"}`))

	_, err := svc.Create(ctx, &coreapi.CreateCronRequest{
		Schedule: "* * * * *",
		Payload: &coreapi.CronPayload{
			AssistantId: aID,
		},
		Enabled: true,
		AssistantFilters: []*coreapi.AuthFilter{
			{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "owner", Match: `"charlie"`}}},
		},
	})
	if err == nil {
		t.Fatal("expected NotFound error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("expected codes.NotFound, got %v", st.Code())
	}
}

// TestService_Create_MissingThread_NotFound verifies that Create with a
// non-existent thread_id and thread_filters returns codes.NotFound (not Internal).
func TestService_Create_MissingThread_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-missing-thread-graph", nil)
	const nonexistentThread = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	_, err := svc.Create(ctx, &coreapi.CreateCronRequest{
		Schedule: "* * * * *",
		Payload: &coreapi.CronPayload{
			AssistantId: aID,
		},
		ThreadId: &coreapi.UUID{Value: nonexistentThread},
		Enabled:  true,
		ThreadFilters: []*coreapi.AuthFilter{
			{Filter: &coreapi.AuthFilter_Eq{Eq: &coreapi.EqAuthFilter{Key: "owner", Match: `"alice"`}}},
		},
	})
	if err == nil {
		t.Fatal("expected NotFound error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("expected codes.NotFound (not Internal), got %v", st.Code())
	}
}

// TestService_Patch_EndTime verifies that Patch propagates end_time from the proto request.
func TestService_Patch_EndTime(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-patch-endtime-graph", nil)
	cronID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)

	future := time.Now().Add(48 * time.Hour).UTC().Truncate(time.Second)
	patched, err := svc.Patch(ctx, &coreapi.PatchCronRequest{
		CronId:  &coreapi.UUID{Value: cronID},
		EndTime: timestamppb.New(future),
	})
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if patched.GetEndTime() == nil {
		t.Fatal("EndTime is nil after patch")
	}
	got := patched.GetEndTime().AsTime().UTC().Truncate(time.Second)
	if !got.Equal(future) {
		t.Errorf("EndTime = %v, want %v", got, future)
	}
}

// TestService_Search_SortBy verifies that sort_by proto enum is forwarded correctly.
func TestService_Search_SortBy(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-sortby-graph", nil)
	testdb.MustInsertCron(t, ctx, pool, aID, "* * * * *")
	testdb.MustInsertCron(t, ctx, pool, aID, "0 * * * *")
	testdb.MustInsertCron(t, ctx, pool, aID, "0 0 * * *")

	sortBy := coreapi.CronsSortBy_CRONS_SORT_BY_CREATED_AT
	sortOrder := coreapi.SortOrder_DESC
	resp, err := svc.Search(ctx, &coreapi.SearchCronsRequest{
		SortBy:    &sortBy,
		SortOrder: &sortOrder,
	})
	if err != nil {
		t.Fatalf("Search sorted: %v", err)
	}
	if len(resp.GetCrons()) < 2 {
		t.Fatalf("expected >= 2 results, got %d", len(resp.GetCrons()))
	}
	// DESC: first cron's created_at >= second cron's created_at.
	c0 := resp.GetCrons()[0].GetCreatedAt().AsTime()
	c1 := resp.GetCrons()[1].GetCreatedAt().AsTime()
	if c0.Before(c1) {
		t.Errorf("sort DESC: expected first.created_at >= second.created_at, got %v < %v", c0, c1)
	}
}

// TestService_Search_EnabledFilter verifies that the enabled *bool filter is forwarded.
func TestService_Search_EnabledFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-enabled-graph", nil)
	enabledID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "* * * * *", true)
	disabledID := testdb.MustInsertCronEnabled(t, ctx, pool, aID, "0 * * * *", false)

	trueVal := true
	resp, err := svc.Search(ctx, &coreapi.SearchCronsRequest{Enabled: &trueVal})
	if err != nil {
		t.Fatalf("Search enabled=true: %v", err)
	}
	ids := make(map[string]struct{})
	for _, c := range resp.GetCrons() {
		ids[c.GetCronId().GetValue()] = struct{}{}
	}
	if _, ok := ids[enabledID]; !ok {
		t.Errorf("enabled cron %s missing from enabled=true results", enabledID)
	}
	if _, ok := ids[disabledID]; ok {
		t.Errorf("disabled cron %s present in enabled=true results", disabledID)
	}
}

// TestService_Create_InvalidSchedule verifies that an invalid schedule expression
// returns codes.InvalidArgument with message "Invalid cron schedule" (ops.py:2174).
func TestService_Create_InvalidSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	svc, pool := newTestService(t, ctx)

	aID := testdb.MustInsertAssistant(t, ctx, pool, "svc-invalid-sched-graph", nil)

	_, err := svc.Create(ctx, &coreapi.CreateCronRequest{
		Schedule: "not a cron expression",
		Payload: &coreapi.CronPayload{
			AssistantId: aID,
		},
		Enabled: true,
	})
	if err == nil {
		t.Fatal("expected InvalidArgument error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected codes.InvalidArgument, got %v", st.Code())
	}
	if st.Message() != "Invalid cron schedule" {
		t.Errorf("expected message %q, got %q", "Invalid cron schedule", st.Message())
	}
}
