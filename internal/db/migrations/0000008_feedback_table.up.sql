-- Port of storage/migrations/0000032_create_feedback.up.sql
-- RQ1: No standalone Feedback gRPC service is built in R5. Feedback writes are
-- folded into Runs.SetStatus per spec §7.8 option b.
--
-- NOTE: The feedback table, enum, and index are already created by
-- 0000003_python_baseline.up.sql (which consolidated storage/migrations/0000000-
-- 0000035). This migration is kept as a redundant safety net so each source
-- migration maps 1:1 to an LSD migration; it is a no-op against any DB that
-- has already run 0000003 (CREATE TABLE IF NOT EXISTS / DO $$ duplicate_object
-- guards). CONCURRENTLY dropped on the index to satisfy golang-migrate's
-- transaction wrapping.

DO $$
BEGIN
    CREATE TYPE feedback_rating AS ENUM ('thumbs_up', 'thumbs_down');
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
END $$;

CREATE TABLE IF NOT EXISTS feedback (
    run_id        uuid              NOT NULL,
    thread_id     uuid              NOT NULL,
    human_message text              NOT NULL,
    ai_message    text              NOT NULL,
    rating        feedback_rating   NOT NULL,
    feedback_text text,
    created_at    timestamptz       DEFAULT now(),
    updated_at    timestamptz       DEFAULT now(),
    metadata      jsonb,
    CONSTRAINT feedback_pkey PRIMARY KEY (run_id),
    CONSTRAINT feedback_run_id_fkey
        FOREIGN KEY (run_id) REFERENCES run(run_id) ON DELETE CASCADE,
    CONSTRAINT feedback_thread_id_fkey
        FOREIGN KEY (thread_id) REFERENCES thread(thread_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS feedback_rating_idx
    ON feedback USING btree (rating);

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '8')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
