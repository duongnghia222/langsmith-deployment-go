ALTER TABLE thread
    ADD COLUMN IF NOT EXISTS expires_at        timestamptz,
    ADD COLUMN IF NOT EXISTS state_updated_at  timestamptz;

CREATE INDEX IF NOT EXISTS thread_expires_at_idx
    ON thread (expires_at)
    WHERE expires_at IS NOT NULL;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '14')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
