package auth_test

import (
	"context"
	"testing"

	"github.com/duongnghia222/langsmith-deployment-go/internal/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestValidateKey_ValidKeys(t *testing.T) {
	for _, k := range []string{"mykey", "user_id", "key.name", "key-name", "abc123"} {
		if err := auth.ValidateKey(k); err != nil {
			t.Errorf("ValidateKey(%q) returned unexpected error: %v", k, err)
		}
	}
}

func TestValidateKey_InvalidKeys(t *testing.T) {
	for _, k := range []string{"bad key", "key!", "key/path", "", "key\nval"} {
		err := auth.ValidateKey(k)
		if err == nil {
			t.Errorf("ValidateKey(%q) expected error, got nil", k)
			continue
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("ValidateKey(%q) code = %v, want InvalidArgument", k, status.Code(err))
		}
	}
}

func TestUserIDFromContext_Present(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-user-id", "alice"))
	uid, err := auth.UserIDFromContext(ctx)
	if err != nil {
		t.Fatalf("UserIDFromContext: %v", err)
	}
	if uid != "alice" {
		t.Errorf("got %q, want %q", uid, "alice")
	}
}

func TestUserIDFromContext_Missing(t *testing.T) {
	_, err := auth.UserIDFromContext(context.Background())
	if err == nil {
		t.Fatal("expected error for context with no user_id")
	}
}

func TestUserIDFromContext_EmptyValue(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-user-id", ""))
	_, err := auth.UserIDFromContext(ctx)
	if err == nil {
		t.Fatal("expected error for empty user_id")
	}
}
