-- Add on_run_completed column to cron. Stores the proto enum value as text
-- (one of the names exported by enum_cron_on_run_completed.proto) or NULL.
ALTER TABLE cron
    ADD COLUMN IF NOT EXISTS on_run_completed TEXT;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '12')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
