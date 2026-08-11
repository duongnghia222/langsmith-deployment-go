package testdb

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MustInsertThread inserts a minimal thread row for use in integration tests.
// metadata defaults to '{}' if nil. Fails the test on duplicate thread_id.
func MustInsertThread(t *testing.T, ctx context.Context, pool *pgxpool.Pool, threadID string, metadata []byte) {
	t.Helper()
	if metadata == nil {
		metadata = []byte(`{}`)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO thread (thread_id, metadata, status, created_at, updated_at)
		 VALUES ($1::uuid, $2::jsonb, 'idle', now(), now())`,
		threadID, metadata,
	); err != nil {
		t.Fatalf("insert thread: %v", err)
	}
}

// MustInsertRun inserts a minimal run row for use in integration tests.
// The caller is responsible for ensuring the referenced threadID and assistantID
// already exist (FK constraints). Returns the new run's UUID as a string.
func MustInsertRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, threadID, assistantID, status string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(ctx,
		`INSERT INTO run (run_id, thread_id, assistant_id, status, kwargs, created_at, updated_at)
		 VALUES (gen_random_uuid(), $1::uuid, $2::uuid, $3, '{}'::jsonb, now(), now())
		 RETURNING run_id::text`,
		threadID, assistantID, status,
	).Scan(&id); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	return id
}

// MustInsertCron inserts a minimal cron row for use in integration tests.
// The caller is responsible for ensuring the referenced assistantID already exists
// (FK constraint). Returns the new cron's UUID as a string.
func MustInsertCron(t *testing.T, ctx context.Context, pool *pgxpool.Pool, assistantID, schedule string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(ctx,
		`INSERT INTO cron (cron_id, assistant_id, schedule, next_run_date, payload, metadata, created_at, updated_at)
		 VALUES (gen_random_uuid(), $1::uuid, $2, now() + interval '1 hour', '{}'::jsonb, '{}'::jsonb, now(), now())
		 RETURNING cron_id::text`,
		assistantID, schedule,
	).Scan(&id); err != nil {
		t.Fatalf("insert cron: %v", err)
	}
	return id
}

// MustInsertCronEnabled inserts a cron row that includes the enabled column
// added in migration 0000005. Use instead of MustInsertCron when the test DB
// has had 0000005 applied (all R3+ tests).
func MustInsertCronEnabled(t *testing.T, ctx context.Context, pool *pgxpool.Pool, assistantID, schedule string, enabled bool) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(ctx,
		`INSERT INTO cron (cron_id, assistant_id, schedule, next_run_date, payload, metadata, enabled, timezone, created_at, updated_at)
		 VALUES (gen_random_uuid(), $1::uuid, $2, now() + interval '1 hour', '{}'::jsonb, '{}'::jsonb, $3, '', now(), now())
		 RETURNING cron_id::text`,
		assistantID, schedule, enabled,
	).Scan(&id); err != nil {
		t.Fatalf("insert cron (enabled): %v", err)
	}
	return id
}

// MustInsertCronWithThread inserts a cron row linked to an existing thread.
// Used in tests that exercise thread_filters via JOIN.
func MustInsertCronWithThread(t *testing.T, ctx context.Context, pool *pgxpool.Pool, assistantID, threadID, schedule string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(ctx,
		`INSERT INTO cron (cron_id, assistant_id, thread_id, schedule, next_run_date, payload, metadata, created_at, updated_at)
		 VALUES (gen_random_uuid(), $1::uuid, $2::uuid, $3, now() + interval '1 hour', '{}'::jsonb, '{}'::jsonb, now(), now())
		 RETURNING cron_id::text`,
		assistantID, threadID, schedule,
	).Scan(&id); err != nil {
		t.Fatalf("insert cron with thread: %v", err)
	}
	return id
}

// MustInsertThreadWithMeta inserts a thread row with the given metadata JSON,
// returning its UUID as text. Metadata must be valid JSON (e.g. `{"role":"admin"}`).
func MustInsertThreadWithMeta(t *testing.T, ctx context.Context, pool *pgxpool.Pool, metadata []byte) string {
	t.Helper()
	if metadata == nil {
		metadata = []byte(`{}`)
	}
	var id string
	if err := pool.QueryRow(ctx,
		`INSERT INTO thread (thread_id, metadata, status, created_at, updated_at)
		 VALUES (gen_random_uuid(), $1::jsonb, 'idle', now(), now())
		 RETURNING thread_id::text`,
		metadata,
	).Scan(&id); err != nil {
		t.Fatalf("insert thread with meta: %v", err)
	}
	return id
}
