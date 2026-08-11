package auth_test

import (
	"reflect"
	"strings"
	"testing"

	pb "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/auth"
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

func TestApplyToQuery_Eq(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "owner", Match: "alice"}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := `((metadata->>'owner') = $1)`
	if sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
	if len(args) != 1 || args[0] != "alice" {
		t.Errorf("args = %v, want [alice]", args)
	}
}

func TestApplyToQuery_Contains(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Contains{Contains: &pb.ContainsAuthFilter{
			Key: "tag", Matches: []string{"x", "y"},
		}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := `((metadata->>'tag') = ANY($1::text[]))`
	if sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
	if len(args) != 1 {
		t.Fatalf("args = %v, want 1 element", args)
	}
	wantMatches := []string{"x", "y"}
	if !reflect.DeepEqual(args[0], wantMatches) {
		t.Errorf("args[0] = %v, want %v", args[0], wantMatches)
	}
}

func TestApplyToQuery_Or(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_OrFilter{OrFilter: &pb.OrAuthFilter{Filters: []*pb.AuthFilter{
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "a", Match: "1"}}},
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "b", Match: "2"}}},
		}}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(sql, " OR ") {
		t.Errorf("expected OR in sql, got %q", sql)
	}
	if len(args) != 2 {
		t.Fatalf("args len = %d, want 2", len(args))
	}
	if args[0] != "1" {
		t.Errorf("args[0] = %v, want %q", args[0], "1")
	}
	if args[1] != "2" {
		t.Errorf("args[1] = %v, want %q", args[1], "2")
	}
}

func TestApplyToQuery_RejectsInjectionInKey(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "x'; DROP TABLE foo; --", Match: "y"}}},
	}
	_, _, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err == nil {
		t.Fatal("expected error for SQL injection attempt in key")
	}
}

func TestApplyToQuery_MultipleFiltersAreAndJoined(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "a", Match: "1"}}},
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "b", Match: "2"}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(sql, " AND ") {
		t.Errorf("expected AND in sql, got %q", sql)
	}
	if len(args) != 2 {
		t.Errorf("args len = %d, want 2", len(args))
	}
}

// TestApplyToQuery_StartIdx pins the placeholder-counting contract when
// startIdx > 1 (i.e., the caller already has bound parameters).
func TestApplyToQuery_StartIdx(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "a", Match: "val1"}}},
		{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "b", Match: "val2"}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 5)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(sql, "$5") {
		t.Errorf("expected $5 in sql, got %q", sql)
	}
	if !strings.Contains(sql, "$6") {
		t.Errorf("expected $6 in sql, got %q", sql)
	}
	if len(args) != 2 {
		t.Fatalf("args len = %d, want 2", len(args))
	}
	if args[0] != "val1" {
		t.Errorf("args[0] = %v, want %q", args[0], "val1")
	}
	if args[1] != "val2" {
		t.Errorf("args[1] = %v, want %q", args[1], "val2")
	}
}

// TestApplyToQuery_AndFilter exercises the AndFilter oneof variant directly —
// a single top-level filter that wraps an AND over two Eq children.
func TestApplyToQuery_AndFilter(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_AndFilter{AndFilter: &pb.AndAuthFilter{Filters: []*pb.AuthFilter{
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "x", Match: "foo"}}},
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "y", Match: "bar"}}},
		}}}},
	}
	sql, args, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(sql, " AND ") {
		t.Errorf("expected AND in sql, got %q", sql)
	}
	if len(args) != 2 {
		t.Fatalf("args len = %d, want 2", len(args))
	}
	if args[0] != "foo" {
		t.Errorf("args[0] = %v, want %q", args[0], "foo")
	}
	if args[1] != "bar" {
		t.Errorf("args[1] = %v, want %q", args[1], "bar")
	}
}

// TestApplyToQuery_RejectsInjectionInNestedKey verifies that key validation
// applies transitively through nested OrFilter children.
func TestApplyToQuery_RejectsInjectionInNestedKey(t *testing.T) {
	filters := []*pb.AuthFilter{
		{Filter: &pb.AuthFilter_OrFilter{OrFilter: &pb.OrAuthFilter{Filters: []*pb.AuthFilter{
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "good", Match: "ok"}}},
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "bad'; DROP TABLE runs; --", Match: "evil"}}},
		}}}},
	}
	_, _, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err == nil {
		t.Fatal("expected error for SQL injection attempt in nested key")
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
			{Filter: &pb.AuthFilter_Eq{Eq: &pb.EqAuthFilter{Key: "ok", Match: "v"}}},
			nil,
		}}}},
	}
	_, _, err := auth.ApplyToQuery(filters, "metadata", 1)
	if err == nil {
		t.Fatal("expected error for nil nested filter element, got nil")
	}
}
