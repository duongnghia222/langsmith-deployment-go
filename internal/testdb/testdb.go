package testdb

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Start spins up an ephemeral pgvector/pg17 container, returns its DSN,
// and registers cleanup with the test.
func Start(t *testing.T, ctx context.Context) string {
	t.Helper()
	c, err := postgres.Run(ctx,
		"pgvector/pgvector:pg17",
		postgres.WithDatabase("lsd_test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("postgres.Run: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Terminate(context.Background())
	})
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("ConnectionString: %v", err)
	}
	return dsn
}

// SeedBaseSchema creates the upstream `thread` and `run` tables that LSD
// migrations expect to exist. Mirrors storage/migrations/0000028 and 0000029
// (without the CONCURRENTLY indexes — testcontainers run is single-shot).
func SeedBaseSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,

		// minimal assistant table to satisfy run.assistant_id FK
		`CREATE TABLE IF NOT EXISTS assistant (
			assistant_id uuid DEFAULT gen_random_uuid() NOT NULL,
			created_at   timestamptz DEFAULT now(),
			CONSTRAINT assistant_pkey PRIMARY KEY (assistant_id)
		)`,

		`CREATE TABLE IF NOT EXISTS thread (
			thread_id  uuid DEFAULT gen_random_uuid() NOT NULL,
			created_at timestamptz DEFAULT now(),
			updated_at timestamptz DEFAULT now(),
			metadata   jsonb DEFAULT '{}'::jsonb NOT NULL,
			status     text  DEFAULT 'idle'::text NOT NULL,
			config     jsonb DEFAULT '{}'::jsonb NOT NULL,
			"values"   jsonb,
			interrupts jsonb DEFAULT '{}'::jsonb NOT NULL,
			CONSTRAINT thread_pkey PRIMARY KEY (thread_id)
		)`,

		`CREATE TABLE IF NOT EXISTS run (
			run_id             uuid DEFAULT gen_random_uuid() NOT NULL,
			thread_id          uuid NOT NULL,
			assistant_id       uuid NOT NULL,
			created_at         timestamptz DEFAULT now(),
			updated_at         timestamptz DEFAULT now(),
			metadata           jsonb DEFAULT '{}'::jsonb NOT NULL,
			status             text  DEFAULT 'pending'::text NOT NULL,
			kwargs             jsonb NOT NULL,
			multitask_strategy text  DEFAULT 'reject'::text NOT NULL,
			CONSTRAINT run_pkey PRIMARY KEY (run_id),
			CONSTRAINT run_assistant_id_fkey FOREIGN KEY (assistant_id) REFERENCES assistant(assistant_id) ON DELETE CASCADE,
			CONSTRAINT run_thread_id_fkey    FOREIGN KEY (thread_id)    REFERENCES thread(thread_id)    ON DELETE CASCADE
		)`,

		`CREATE INDEX IF NOT EXISTS run_pending_idx ON run (created_at) WHERE status = 'pending'`,
		`CREATE INDEX IF NOT EXISTS run_thread_id_status_idx ON run (thread_id, status)`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatalf("seed exec %q: %v", s[:60], err)
		}
	}
}

// MustInsertAssistant inserts a full assistant row and returns its UUID as text.
// graphID is required; metadata defaults to '{}' if nil.
func MustInsertAssistant(t *testing.T, ctx context.Context, pool *pgxpool.Pool, graphID string, metadata []byte) string {
	t.Helper()
	if metadata == nil {
		metadata = []byte(`{}`)
	}
	var id string
	if err := pool.QueryRow(ctx,
		`INSERT INTO assistant (graph_id, metadata, config, context, "version", created_at, updated_at)
		 VALUES ($1, $2::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now())
		 RETURNING assistant_id::text`,
		graphID, metadata,
	).Scan(&id); err != nil {
		t.Fatalf("insert assistant: %v", err)
	}
	return id
}

// MustInsertAssistantVersion inserts a row into assistant_versions.
// The parent assistant row must already exist (use MustInsertAssistant first).
// metadata defaults to '{}' if nil.
func MustInsertAssistantVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, assistantID string, version int, graphID string, metadata []byte) {
	t.Helper()
	if metadata == nil {
		metadata = []byte(`{}`)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO assistant_versions (assistant_id, "version", graph_id, config, metadata, context, created_at)
		 VALUES ($1::uuid, $2, $3, '{}'::jsonb, $4::jsonb, '{}'::jsonb, now())`,
		assistantID, version, graphID, metadata,
	); err != nil {
		t.Fatalf("insert assistant_version: %v", err)
	}
}
