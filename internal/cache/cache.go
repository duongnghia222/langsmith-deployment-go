// Package cache provides a thin Redis wrapper for the LSD Cache gRPC service.
// Keys are expected to already be namespaced by the caller (see service.go).
// ErrNotFound is returned by Get when the key has no value in Redis.
package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// ErrNotFound is returned by Get when the requested key does not exist or has expired.
var ErrNotFound = errors.New("cache: key not found")

// Cache is a thin wrapper around a go-redis client that provides
// namespaced Set/Get operations with optional TTL support.
type Cache struct {
	rdb *goredis.Client
}

// NewCache constructs a Cache backed by the given go-redis client.
func NewCache(rdb *goredis.Client) *Cache {
	return &Cache{rdb: rdb}
}

// Set stores value under namespacedKey with the given TTL.
// A zero ttl means no expiration (the key persists until explicitly deleted).
func (c *Cache) Set(ctx context.Context, namespacedKey string, value []byte, ttl time.Duration) error {
	if err := c.rdb.Set(ctx, namespacedKey, value, ttl).Err(); err != nil {
		return fmt.Errorf("cache set %q: %w", namespacedKey, err)
	}
	return nil
}

// Get retrieves the value stored under namespacedKey.
// Returns ErrNotFound when the key is absent or expired.
// Returns a wrapped error on any other Redis failure.
func (c *Cache) Get(ctx context.Context, namespacedKey string) ([]byte, error) {
	val, err := c.rdb.Get(ctx, namespacedKey).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("cache get %q: %w", namespacedKey, err)
	}
	return val, nil
}
