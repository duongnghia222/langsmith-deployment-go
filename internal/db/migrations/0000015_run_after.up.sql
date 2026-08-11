ALTER TABLE run
    ADD COLUMN IF NOT EXISTS run_after timestamptz;

-- Partial index supports the "ready to pick" predicate used by Next().
CREATE INDEX IF NOT EXISTS run_pending_ready_idx
    ON run (created_at, run_id)
    WHERE status = 'pending';

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '15')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
