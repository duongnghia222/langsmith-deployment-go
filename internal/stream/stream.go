package stream

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Entry is a single Redis Streams entry with its ID and field map.
type Entry struct {
	ID     string
	Fields map[string]any
}

// Streamer wraps a go-redis client and exposes Redis Streams operations used
// by Runs.Publish, Runs.Stream, Runs.Enter, and Threads.Stream.
type Streamer struct {
	rdb *goredis.Client
}

// NewStreamer constructs a Streamer from a live go-redis client.
func NewStreamer(rdb *goredis.Client) *Streamer {
	return &Streamer{rdb: rdb}
}

// XAdd appends a message to the Redis stream at key.
// maxLen controls the approximate MAXLEN trim (use ~N semantics for efficiency).
// Returns the new entry ID assigned by Redis (e.g. "1715100000000-0").
func (s *Streamer) XAdd(ctx context.Context, key string, fields map[string]any, maxLen int64) (string, error) {
	args := &goredis.XAddArgs{
		Stream: key,
		MaxLen: maxLen,
		Approx: true,
		ID:     "*",
		Values: fields,
	}
	id, err := s.rdb.XAdd(ctx, args).Result()
	if err != nil {
		return "", err
	}
	return id, nil
}

// XReadFrom reads up to count entries from the stream at key, starting
// exclusively from fromID. Use "0-0" to read from the beginning.
// Use "$" to read only new entries (in blocking mode).
// blockMillis controls how long to wait when no entries are available:
//   - 0 means non-blocking (return immediately if empty).
//   - >0 means block for up to that many milliseconds.
//
// Returns an empty slice (not an error) when the timeout expires with no data.
func (s *Streamer) XReadFrom(ctx context.Context, key, fromID string, count int64, blockMillis int) ([]Entry, error) {
	args := &goredis.XReadArgs{
		Streams: []string{key, fromID},
		Count:   count,
		Block:   time.Duration(blockMillis) * time.Millisecond,
	}
	results, err := s.rdb.XRead(ctx, args).Result()
	if err != nil {
		// Redis returns redis.Nil when BLOCK times out with no data; treat as empty.
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	msgs := results[0].Messages
	out := make([]Entry, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, Entry{ID: m.ID, Fields: m.Values})
	}
	return out, nil
}
