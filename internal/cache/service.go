package cache

import (
	"context"
	"errors"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/auth"
	"github.com/duongnghia222/langsmith-deployment-go/internal/stream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements coreapi.CacheServer backed by a Redis Cache.
type Service struct {
	coreapi.UnimplementedCacheServer
	cache *Cache
}

// NewService constructs a Cache gRPC service.
func NewService(c *Cache) *Service {
	return &Service{cache: c}
}

// Set stores a value in Redis under a namespaced key derived from the
// authenticated user ID and the request key.
func (s *Service) Set(ctx context.Context, req *coreapi.CacheSetRequest) (*emptypb.Empty, error) {
	if err := auth.ValidateKey(req.GetKey()); err != nil {
		return nil, err
	}
	userID, err := auth.UserIDFromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "missing user id: %v", err)
	}
	if err := auth.ValidateKey(userID); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id in context: %v", err)
	}

	var ttl time.Duration
	if req.Ttl != nil {
		ttl = req.Ttl.AsDuration()
	}

	namespacedKey := stream.CacheKey(userID, req.GetKey())
	if err := s.cache.Set(ctx, namespacedKey, req.GetValue(), ttl); err != nil {
		return nil, status.Errorf(codes.Internal, "cache set: %v", err)
	}
	return &emptypb.Empty{}, nil
}

// Get retrieves a value from Redis. On cache miss returns &CacheGetResponse{Value: nil}.
func (s *Service) Get(ctx context.Context, req *coreapi.CacheGetRequest) (*coreapi.CacheGetResponse, error) {
	if err := auth.ValidateKey(req.GetKey()); err != nil {
		return nil, err
	}
	userID, err := auth.UserIDFromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "missing user id: %v", err)
	}
	if err := auth.ValidateKey(userID); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id in context: %v", err)
	}

	namespacedKey := stream.CacheKey(userID, req.GetKey())
	val, err := s.cache.Get(ctx, namespacedKey)
	if errors.Is(err, ErrNotFound) {
		return &coreapi.CacheGetResponse{}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cache get: %v", err)
	}
	return &coreapi.CacheGetResponse{Value: val}, nil
}
