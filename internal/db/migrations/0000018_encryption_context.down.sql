ALTER TABLE run
    DROP COLUMN IF EXISTS encryption_context;
ALTER TABLE cron
    DROP COLUMN IF EXISTS encryption_context;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '17')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
