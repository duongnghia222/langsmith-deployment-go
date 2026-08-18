ALTER TABLE run
    ADD COLUMN IF NOT EXISTS attempt integer NOT NULL DEFAULT 0;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '17')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
