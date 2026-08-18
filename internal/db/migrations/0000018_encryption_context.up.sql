ALTER TABLE run
    ADD COLUMN IF NOT EXISTS encryption_context jsonb;
ALTER TABLE cron
    ADD COLUMN IF NOT EXISTS encryption_context jsonb;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '18')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
