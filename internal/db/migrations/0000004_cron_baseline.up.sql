-- Cron table for scheduled run creation. Idempotent so it composes with
-- already-migrated databases that may have applied the Python equivalent.
CREATE TABLE IF NOT EXISTS cron (
  cron_id        UUID        PRIMARY KEY,
  thread_id      UUID,
  user_id        TEXT,
  assistant_id   UUID        NOT NULL,
  schedule       TEXT        NOT NULL,
  next_run_date  TIMESTAMPTZ,
  end_time       TIMESTAMPTZ,
  payload        JSONB       NOT NULL DEFAULT '{}'::jsonb,
  metadata       JSONB       NOT NULL DEFAULT '{}'::jsonb,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cron_next_run_date_idx
  ON cron (next_run_date)
  WHERE next_run_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS cron_thread_id_idx ON cron (thread_id);
CREATE INDEX IF NOT EXISTS cron_assistant_id_idx ON cron (assistant_id);

-- Fresh-DB compatibility: cron_assistant_id_fkey is best-effort.
DO $$
BEGIN
  ALTER TABLE cron
    ADD CONSTRAINT cron_assistant_id_fkey
    FOREIGN KEY (assistant_id) REFERENCES assistant(assistant_id) ON DELETE CASCADE;
EXCEPTION
  WHEN duplicate_object THEN NULL;
  WHEN undefined_table THEN NULL;
END $$;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '4')
  ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
