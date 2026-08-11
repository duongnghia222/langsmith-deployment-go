package auth_test

import (
	"context"
	"strings"
	"testing"

	pb "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/auth"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestApplyToQuery_Empty(t *testing.T) {
	sql, args, err := auth.ApplyToQuery(nil, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if sql != "" || len(args) != 0 {
		t.Errorf("expected empty result, got sql=%q args=%v", sql, args)
	}
}

// TestApplyToQuery_Eq pins the target SQL form (ops.py:2688 parity): jsonb
// containment against a single-key object, with both key and value bound as
// parameters ($m is the JSON-encoded match string from the wire).
func TestApplyToQuery_Eq(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "owner", Match: `"alice"`}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := `(metadata @> jsonb_build_object($1::text, $2::jsonb))`
	if sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
	if len(args) != 2 || args[0] != "owner" || args[1] != `"alice"` {
		t.Errorf("args = %v, want [owner \"alice\"]", args)
	}
}

// TestApplyToQuery_Contains pins the target SQL form (ops.py:2694-2700 parity):
// jsonb containment on the value at key, binding the whole JSON-encoded value
// (scalar or list) as ONE jsonb parameter.
func TestApplyToQuery_Contains(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Contains{Contains: &pb.ContainsAuthFilter{
			Key: "tags", Matches: []string{`["x","y"]`},
		}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := `((metadata -> $1::text) @> $2::jsonb)`
	if sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
	if len(args) != 2 || args[0] != "tags" || args[1] != `["x","y"]` {
		t.Errorf(`args = %v, want [tags ["x","y"]]`, args)
	}
}

// TestApplyToQuery_Contains_RejectsNotExactlyOneMatch guards the Go-side
// assumption that the client always sends the whole value as ONE JSON string
// (see api/grpc/ops/__init__.py's _filters_to_proto $contains branch).
func TestApplyToQuery_Contains_RejectsNotExactlyOneMatch(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Contains{Contains: &pb.ContainsAuthFilter{
			Key: "tags", Matches: []string{"a", "b"},
		}}},
	}
	_, _, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err == nil {
		t.Fatal("expected error for Matches with != 1 element")
	}
}

func TestApplyToQuery_Or(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_OrFilter{OrFilter: &pb.OrAuthFilter{Filters: []*pb.AuthFilter{
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "a", Match: `"1"`}}},
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "b", Match: `"2"`}}},
		}}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(sql, " OR ") {
		t.Errorf("expected OR in sql, got %q", sql)
	}
	if len(args) != 4 {
		t.Fatalf("args len = %d, want 4", len(args))
	}
	if args[0] != "a" || args[1] != `"1"` || args[2] != "b" || args[3] != `"2"` {
		t.Errorf(`args = %v, want [a "1" b "2"]`, args)
	}
}

func TestApplyToQuery_MultipleFiltersAreAndJoined(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "a", Match: `"1"`}}},
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "b", Match: `"2"`}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(sql, " AND ") {
		t.Errorf("expected AND in sql, got %q", sql)
	}
	if len(args) != 4 {
		t.Errorf("args len = %d, want 4", len(args))
	}
}

// TestApplyToQuery_StartIdx pins the placeholder-counting contract when
// startIdx > 1 (i.e., the caller already has bound parameters). Each Eq/
// Contains leaf now consumes TWO placeholders (key, value).
func TestApplyToQuery_StartIdx(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "a", Match: `"val1"`}}},
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "b", Match: `"val2"`}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 5)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, want := range []string{"$5", "$6", "$7", "$8"} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected %s in sql, got %q", want, sql)
		}
	}
	if len(args) != 4 {
		t.Fatalf("args len = %d, want 4", len(args))
	}
	if args[0] != "a" || args[1] != `"val1"` || args[2] != "b" || args[3] != `"val2"` {
		t.Errorf(`args = %v, want [a "val1" b "val2"]`, args)
	}
}

// TestApplyToQuery_AndFilter exercises the AndFilter oneof variant directly —
// a single top-level filter that wraps an AND over two Eq children.
func TestApplyToQuery_AndFilter(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_AndFilter{AndFilter: &pb.AndAuthFilter{Filters: []*pb.AuthFilter{
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "x", Match: `"foo"`}}},
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "y", Match: `"bar"`}}},
		}}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(sql, " AND ") {
		t.Errorf("expected AND in sql, got %q", sql)
	}
	if len(args) != 4 {
		t.Fatalf("args len = %d, want 4", len(args))
	}
	if args[0] != "x" || args[1] != `"foo"` || args[2] != "y" || args[3] != `"bar"` {
		t.Errorf(`args = %v, want [x "foo" y "bar"]`, args)
	}
}

// TestApplyToQuery_KeyIsBoundParam verifies the key is never interpolated into
// the SQL string — even a key shaped like a SQL-injection attempt is accepted
// and passed through as a bind parameter. Per the brief, the validKey regexp
// check is no longer needed for injection safety (ops.py accepts any key)
// because the key is always bound, not inlined.
func TestApplyToQuery_KeyIsBoundParam(t *testing.T) {
	key := "x'; DROP TABLE foo; --"
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: key, Match: `"y"`}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if strings.Contains(sql, key) {
		t.Errorf("key leaked into sql literal: %q", sql)
	}
	if args[0] != key {
		t.Errorf("args[0] = %v, want key bound as parameter", args[0])
	}
}

func TestApplyToQuery_RejectsEmptyKey(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "", Match: `"y"`}}},
	}
	_, _, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err == nil {
		t.Fatal("expected error for empty filter key")
	}
}

func TestApplyToQuery_RejectsEmptyKeyInNested(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_OrFilter{OrFilter: &pb.OrAuthFilter{Filters: []*pb.AuthFilter{
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "good", Match: `"ok"`}}},
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "", Match: `"evil"`}}},
		}}}},
	}
	_, _, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err == nil {
		t.Fatal("expected error for empty key in nested filter")
	}
}

// TestApplyToQuery_NilFilterElement verifies that a nil *pb.AuthFilter in the
// top-level slice returns an error rather than panicking.
func TestApplyToQuery_NilFilterElement(t *testing.T) {
	filters := []*pb.AuthFilter{nil}
	_, _, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err == nil {
		t.Fatal("expected error for nil filter element, got nil")
	}
}

// TestApplyToQuery_NilFilterElementInNested verifies that a nil *pb.AuthFilter
// nested inside an OrFilter returns an error rather than panicking.
func TestApplyToQuery_NilFilterElementInNested(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_OrFilter{OrFilter: &pb.OrAuthFilter{Filters: []*pb.AuthFilter{
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "ok", Match: `"v"`}}},
			nil,
		}}}},
	}
	_, _, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err == nil {
		t.Fatal("expected error for nil nested filter element, got nil")
	}
}

// ─── DB-backed containment semantics (ops.py parity) ─────────────────────────
//
// These exercise the generated SQL against a real Postgres jsonb column,
// because containment (@>) semantics for arrays/objects are not meaningfully
// verifiable by string comparison alone.

func newAuthTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
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
	return pool
}

// TestApplyToQuery_Contains_ArrayRequiresAllElements verifies list-valued
// $contains against array metadata: a row matches only when ALL elements of
// the filter list are present in the row's array (jsonb array containment
// gives this for free — order-independent).
func TestApplyToQuery_Contains_ArrayRequiresAllElements(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	pool := newAuthTestPool(t, ctx)

	full := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"tags":["a","b","c"]}`))
	testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"tags":["a","b"]}`)) // missing "c"

	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Contains{Contains: &pb.ContainsAuthFilter{
			Key: "tags", Matches: []string{`["a","c"]`},
		}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("ApplyToQuery: %v", err)
	}

	rows, err := pool.Query(ctx, "SELECT thread_id::text FROM thread WHERE "+sql, args...)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, id)
	}
	if len(got) != 1 || got[0] != full {
		t.Errorf("matches = %v, want only [%s] (partial-overlap row must be excluded)", got, full)
	}
}

// TestApplyToQuery_Eq_ObjectValueContainment verifies $eq with an object
// value matches via containment, not equality — ops.py:2688 behavior:
// {"a":1} matches metadata {"k":{"a":1,"b":2}} because it is a subset.
func TestApplyToQuery_Eq_ObjectValueContainment(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	pool := newAuthTestPool(t, ctx)

	superset := testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"k":{"a":1,"b":2}}`))
	testdb.MustInsertThreadWithMeta(t, ctx, pool, []byte(`{"k":{"a":9,"b":2}}`)) // "a" differs

	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "k", Match: `{"a":1}`}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("ApplyToQuery: %v", err)
	}

	rows, err := pool.Query(ctx, "SELECT thread_id::text FROM thread WHERE "+sql, args...)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, id)
	}
	if len(got) != 1 || got[0] != superset {
		t.Errorf("matches = %v, want only [%s]", got, superset)
	}
}
