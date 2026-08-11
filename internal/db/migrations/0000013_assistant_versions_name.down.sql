ALTER TABLE assistant_versions
    DROP COLUMN IF EXISTS name;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '12')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
