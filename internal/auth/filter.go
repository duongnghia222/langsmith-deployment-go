// Package auth translates core_api.AuthFilter messages into parameterized
// SQL WHERE-clause fragments. Every servicer that accepts AuthFilters MUST
// route them through ApplyToQuery before issuing the underlying query.
package auth

import (
	"fmt"
	"regexp"
	"strings"

	pb "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// validKey rejects anything that could break out of a JSONB ->> 'key' literal.
var validKey = regexp.MustCompile(`^[A-Za-z0-9_.\-]+$`)

// ApplyToQuery converts filters into an SQL fragment and the corresponding
// args slice. The startIdx parameter is the next $N placeholder number
// (callers passing existing args use len(existingArgs)+1).
//
// Returns ("", nil, nil) when filters is empty.
//
// Multiple top-level filters are AND-joined; a single top-level filter is
// returned as a raw emit fragment with no extra outer parentheses.
//
// jsonbColumn must be a hard-coded column name and never user-supplied input.
func ApplyToQuery(filters []*pb.AuthFilter, jsonbColumn string, startIdx int) (string, []any, error) {
	if len(filters) == 0 {
		return "", nil, nil
	}
	var args []any
	parts := make([]string, 0, len(filters))
	idx := startIdx
	for _, f := range filters {
		frag, fargs, nextIdx, err := emit(f, jsonbColumn, idx)
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, frag)
		args = append(args, fargs...)
		idx = nextIdx
	}
	if len(parts) == 1 {
		return parts[0], args, nil
	}
	return "(" + strings.Join(parts, " AND ") + ")", args, nil
}

func emit(f *pb.AuthFilter, col string, idx int) (string, []any, int, error) {
	if f == nil {
		return "", nil, idx, fmt.Errorf("auth: nil filter")
	}
	switch v := f.Filter.(type) {
	case *pb.AuthFilter_Eq:
		if !validKey.MatchString(v.Eq.Key) {
			return "", nil, idx, fmt.Errorf("auth: invalid filter key %q", v.Eq.Key)
		}
		return fmt.Sprintf("((%s->>'%s') = $%d)", col, v.Eq.Key, idx),
			[]any{v.Eq.Match}, idx + 1, nil
	case *pb.AuthFilter_Contains:
		if !validKey.MatchString(v.Contains.Key) {
			return "", nil, idx, fmt.Errorf("auth: invalid filter key %q", v.Contains.Key)
		}
		return fmt.Sprintf("((%s->>'%s') = ANY($%d::text[]))", col, v.Contains.Key, idx),
			[]any{v.Contains.Matches}, idx + 1, nil
	case *pb.AuthFilter_OrFilter:
		return joinNested(v.OrFilter.Filters, col, idx, " OR ")
	case *pb.AuthFilter_AndFilter:
		return joinNested(v.AndFilter.Filters, col, idx, " AND ")
	default:
		return "", nil, idx, fmt.Errorf("auth: unknown filter type %T", f.Filter)
	}
}

// ValidateKey returns codes.InvalidArgument if key does not match the
// validKey regex (^[A-Za-z0-9_.\-]+$). Used by the Cache service to
// guard both the request key and the user_id extracted from context.
func ValidateKey(key string) error {
	if !validKey.MatchString(key) {
		return status.Errorf(codes.InvalidArgument,
			"invalid cache key: must match [A-Za-z0-9_.\\-]+, got %q", key)
	}
	return nil
}

func joinNested(filters []*pb.AuthFilter, col string, idx int, sep string) (string, []any, int, error) {
	if len(filters) == 0 {
		return "", nil, idx, fmt.Errorf("auth: empty nested filter")
	}
	var args []any
	parts := make([]string, 0, len(filters))
	for _, f := range filters {
		frag, fargs, nextIdx, err := emit(f, col, idx)
		if err != nil {
			return "", nil, idx, err
		}
		parts = append(parts, frag)
		args = append(args, fargs...)
		idx = nextIdx
	}
	return "(" + strings.Join(parts, sep) + ")", args, idx, nil
}
