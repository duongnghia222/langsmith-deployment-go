package stream_test

import (
	"context"
	"testing"
	"time"

	"github.com/duongnghia222/langsmith-deployment-go/internal/stream"
)

func TestPubSub_BasicPublishSubscribe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis testcontainer test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rdb := startRedis(t)
	s := stream.NewStreamer(rdb)

	sub, err := s.Subscribe(ctx, "chan:test:basic")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close() //nolint:errcheck

	// Give the subscription a moment to be established.
	time.Sleep(50 * time.Millisecond)

	if err := s.Publish(ctx, "chan:test:basic", []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-sub.Channel():
		if msg.Payload != "hello" {
			t.Errorf("payload = %q, want %q", msg.Payload, "hello")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for pub/sub message")
	}
}

func TestPubSub_MultiChannelFanIn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis testcontainer test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rdb := startRedis(t)
	s := stream.NewStreamer(rdb)

	sub, err := s.Subscribe(ctx, "chan:multi:a", "chan:multi:b")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close() //nolint:errcheck

	time.Sleep(50 * time.Millisecond)

	if err := s.Publish(ctx, "chan:multi:a", []byte("msgA")); err != nil {
		t.Fatalf("Publish to a: %v", err)
	}
	if err := s.Publish(ctx, "chan:multi:b", []byte("msgB")); err != nil {
		t.Fatalf("Publish to b: %v", err)
	}

	received := make(map[string]string)
	for i := 0; i < 2; i++ {
		select {
		case msg := <-sub.Channel():
			received[msg.Channel] = msg.Payload
		case <-ctx.Done():
			t.Fatalf("timeout after receiving %d messages", i)
		}
	}
	if received["chan:multi:a"] != "msgA" {
		t.Errorf("chan:multi:a payload = %q, want %q", received["chan:multi:a"], "msgA")
	}
	if received["chan:multi:b"] != "msgB" {
		t.Errorf("chan:multi:b payload = %q, want %q", received["chan:multi:b"], "msgB")
	}
}

func TestPubSub_CloseIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis testcontainer test in short mode")
	}
	ctx := context.Background()
	rdb := startRedis(t)
	s := stream.NewStreamer(rdb)

	sub, err := s.Subscribe(ctx, "chan:close:idempotent")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	// Second Close must not panic or return a surprising error.
	if err := sub.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
