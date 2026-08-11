-- Adds enabled column to cron (R3: CreateCron/PatchCron expose this field).
-- Idempotent via ADD COLUMN IF NOT EXISTS.
ALTER TABLE cron
  ADD COLUMN IF NOT EXISTS enabled   BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS timezone  TEXT    NOT NULL DEFAULT '';

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '5')
  ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
