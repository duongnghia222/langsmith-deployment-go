// Package redis wraps the go-redis client with LSD-specific construction.
//
// LSD uses Redis for pub/sub fan-out, BLPOP queue wake-up, stream
// entry-id replay, and ephemeral cache TTL. This package owns
// connection lifecycle; servicers receive a *Client and call its
// methods directly.
package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

type Config struct {
	URL      string
	PoolSize int
}

type Client struct {
	*goredis.Client
}

func New(ctx context.Context, cfg Config) (*Client, error) {
	opts, err := goredis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	if cfg.PoolSize > 0 {
		opts.PoolSize = cfg.PoolSize
	}
	c := goredis.NewClient(opts)
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Client{Client: c}, nil
}
