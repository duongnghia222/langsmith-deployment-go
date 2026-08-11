-- Port of storage/migrations/0000034_add_assistant_context_description.up.sql
-- ADD COLUMN IF NOT EXISTS is idempotent. Columns are already inlined into
-- the CREATE TABLE assistant / assistant_versions statements in
-- 0000003_python_baseline; this migration is a no-op safety net.

ALTER TABLE assistant
    ADD COLUMN IF NOT EXISTS context     jsonb DEFAULT '{}'::jsonb NOT NULL,
    ADD COLUMN IF NOT EXISTS description text;

ALTER TABLE assistant_versions
    ADD COLUMN IF NOT EXISTS context     jsonb DEFAULT '{}'::jsonb NOT NULL,
    ADD COLUMN IF NOT EXISTS description text;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '10')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
