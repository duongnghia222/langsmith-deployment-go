// Package stream owns the shared Redis Streams and Pub/Sub primitives used by
// Runs.Stream, Runs.Enter, Runs.Publish, and Threads.Stream.
package stream

import (
	"github.com/google/uuid"
)

// RunStreamKey returns the Redis Streams key for a run's event log.
// Producers: Runs.Publish. Consumers: Runs.Stream, Runs.Join.
func RunStreamKey(runID uuid.UUID) string {
	return "run:" + runID.String() + ":stream"
}

// ThreadStreamKey returns the Redis Streams key for a thread's event tail.
// Producers: Runs.Publish (mirror). Consumers: Threads.Stream.
func ThreadStreamKey(threadID uuid.UUID) string {
	return "thread:" + threadID.String() + ":events"
}

// RunControlChannel returns the Redis Pub/Sub channel for interrupt/rollback
// signals directed at the worker executing a run.
// Producers: Runs.Cancel (signal path). Consumers: Runs.Enter.
func RunControlChannel(runID uuid.UUID) string {
	return "run:" + runID.String() + ":control"
}

// RunTerminalChannel returns the Redis Pub/Sub channel for the one-shot
// "run finished" notification broadcast when a run reaches a terminal state.
// Producers: Runs.SetStatus / Runs.MarkDone. Consumers: Runs.Stream, Runs.Join.
func RunTerminalChannel(runID uuid.UUID) string {
	return "run:" + runID.String() + ":terminal"
}

// RunQueueKey returns the Redis list key used for BLPOP/RPUSH queue wake-up.
// Centralised here to avoid string literals scattered across runs/ and stream/.
func RunQueueKey() string {
	// Keep verbatim: LIST_RUN_QUEUE = "run:queue" (storage/redis.py:184).
	// Shared wake doorbell — Python direct-path workers BLPOP the same key,
	// so cross-runtime wake-ups work during mixed deployments.
	return "run:queue"
}

// CacheKey returns the namespaced Redis key for the Cache service (R5).
// Scoped per userID to prevent cross-tenant leakage.
func CacheKey(userID, key string) string {
	return "cache:" + userID + ":" + key
}
