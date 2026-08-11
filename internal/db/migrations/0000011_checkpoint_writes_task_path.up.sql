-- Port of storage/migrations/0000035_add_task_path_to_checkpoint_writes.up.sql
-- Required for langgraph-checkpoint-postgres 3.0.0 compatibility.
-- task_path is also stored in PutWritesRequest.task_path (checkpointer.proto).
-- Already inlined into 0000003_python_baseline; this migration is a no-op
-- safety net via ADD COLUMN IF NOT EXISTS.

ALTER TABLE checkpoint_writes
    ADD COLUMN IF NOT EXISTS task_path TEXT NOT NULL DEFAULT '';

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '11')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
