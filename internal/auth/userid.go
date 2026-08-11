package auth

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"
)

const userIDMetadataKey = "x-user-id"

// UserIDFromContext extracts the authenticated user ID from the incoming gRPC
// metadata. Returns an error if the "x-user-id" header is absent or empty.
//
// Convention: R5 introduces "x-user-id" as the canonical inbound identity
// header for LSD Cache.
func UserIDFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("no gRPC metadata in context")
	}
	vals := md.Get(userIDMetadataKey)
	if len(vals) == 0 || vals[0] == "" {
		return "", fmt.Errorf("missing or empty %q metadata header", userIDMetadataKey)
	}
	return vals[0], nil
}
