-- Port of storage/migrations/0000030_create_checkpoint.up.sql
-- All CREATE TABLE statements guarded with IF NOT EXISTS to allow LSD to migrate
-- against a database that was previously managed by the Python storage package.
-- CONCURRENTLY omitted from CREATE INDEX: golang-migrate wraps migrations in a
-- transaction and CREATE INDEX CONCURRENTLY cannot run inside a transaction
-- (same convention as 0000003_python_baseline.up.sql).

CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id            uuid NOT NULL,
    checkpoint_id        uuid NOT NULL,
    run_id               uuid NULL,
    parent_checkpoint_id uuid NULL,
    checkpoint           jsonb NOT NULL,
    metadata             jsonb DEFAULT '{}'::jsonb NOT NULL,
    checkpoint_ns        text DEFAULT ''::text NOT NULL,
    CONSTRAINT checkpoints_pkey PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id),
    CONSTRAINT checkpoints_run_id_fkey
        FOREIGN KEY (run_id) REFERENCES run(run_id) ON DELETE CASCADE,
    CONSTRAINT checkpoints_thread_id_fkey
        FOREIGN KEY (thread_id) REFERENCES thread(thread_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id     uuid NOT NULL,
    checkpoint_id uuid NOT NULL,
    task_id       uuid NOT NULL,
    idx           int4 NOT NULL,
    channel       text NOT NULL,
    type          text NOT NULL,
    blob          bytea NOT NULL,
    checkpoint_ns text DEFAULT ''::text NOT NULL,
    CONSTRAINT checkpoint_writes_pkey
        PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx),
    CONSTRAINT checkpoint_writes_thread_id_fkey
        FOREIGN KEY (thread_id) REFERENCES thread(thread_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id     uuid NOT NULL,
    channel       text NOT NULL,
    version       text NOT NULL,
    type          text NOT NULL,
    blob          bytea,
    checkpoint_ns text DEFAULT ''::text NOT NULL,
    CONSTRAINT checkpoint_blobs_pkey
        PRIMARY KEY (thread_id, checkpoint_ns, channel, version),
    CONSTRAINT checkpoint_blobs_thread_id_fkey
        FOREIGN KEY (thread_id) REFERENCES thread(thread_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS checkpoints_checkpoint_id_idx
    ON checkpoints USING btree (thread_id, checkpoint_id DESC);
CREATE INDEX IF NOT EXISTS checkpoints_run_id_idx
    ON checkpoints USING btree (run_id);

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '6')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
