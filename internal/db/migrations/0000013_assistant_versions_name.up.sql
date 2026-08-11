ALTER TABLE assistant_versions
    ADD COLUMN IF NOT EXISTS name text;

UPDATE assistant_versions av
SET name = a.name
FROM assistant a
WHERE av.assistant_id = a.assistant_id
  AND av.name IS NULL;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '13')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
