package stream

import (
	"context"
	"sync"

	goredis "github.com/redis/go-redis/v9"
)

// Subscription wraps a go-redis PubSub connection.
// Call Channel() to receive messages and Close() to unsubscribe.
type Subscription struct {
	ps   *goredis.PubSub
	once sync.Once
}

// Channel returns the read-only message channel. Each *goredis.Message
// carries .Channel (the channel name) and .Payload (string payload).
func (s *Subscription) Channel() <-chan *goredis.Message {
	return s.ps.Channel()
}

// Close unsubscribes and releases the connection. Idempotent.
func (s *Subscription) Close() error {
	var err error
	s.once.Do(func() {
		err = s.ps.Close()
	})
	return err
}

// Subscribe starts a Redis Pub/Sub subscription on one or more channels.
// The returned Subscription is valid until Close() is called or ctx is done.
// The caller is responsible for calling Close() (typically via defer).
func (s *Streamer) Subscribe(ctx context.Context, channels ...string) (*Subscription, error) {
	ps := s.rdb.Subscribe(ctx, channels...)
	// Perform a receive to confirm the SUBSCRIBE acknowledgement from Redis before
	// returning to the caller. This prevents a race where Publish arrives before
	// the subscription is active.
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return nil, err
	}
	return &Subscription{ps: ps}, nil
}

// Publish sends payload to a Redis Pub/Sub channel.
// payload is published as a string (Redis Pub/Sub payloads are strings).
func (s *Streamer) Publish(ctx context.Context, channel string, payload []byte) error {
	return s.rdb.Publish(ctx, channel, payload).Err()
}
