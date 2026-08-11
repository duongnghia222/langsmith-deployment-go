ALTER TABLE cron DROP COLUMN IF EXISTS on_run_completed;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '11')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
