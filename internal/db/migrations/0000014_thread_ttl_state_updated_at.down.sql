DROP INDEX IF EXISTS thread_expires_at_idx;
ALTER TABLE thread
    DROP COLUMN IF EXISTS state_updated_at,
    DROP COLUMN IF EXISTS expires_at;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '13')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
