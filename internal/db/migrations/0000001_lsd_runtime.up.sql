-- Self-bootstrap: ensure run exists before altering it. On already-migrated
-- DBs (Python migrations applied), this is a no-op. On fresh DBs, this stub
-- is subsequently extended by 0000003_python_baseline's ALTER TABLE run
-- block, which adds the additional Python columns and FK constraints
-- idempotently (see 0000003 for the NOT NULL/DEFAULT caveat).
CREATE TABLE IF NOT EXISTS run (
  run_id UUID PRIMARY KEY,
  thread_id UUID,
  status TEXT
);

ALTER TABLE run
  ADD COLUMN IF NOT EXISTS lease_holder_id      UUID,
  ADD COLUMN IF NOT EXISTS lease_expires_at     TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS lease_generation     BIGINT      NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS cancel_requested_at  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS run_expired_lease_idx
  ON run (lease_expires_at)
  WHERE status = 'running' AND lease_expires_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS run_cancel_idx
  ON run (run_id) WHERE cancel_requested_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS lsd_meta (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '1')
  ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
