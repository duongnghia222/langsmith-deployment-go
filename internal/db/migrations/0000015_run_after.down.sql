DROP INDEX IF EXISTS run_pending_ready_idx;
ALTER TABLE run
    DROP COLUMN IF EXISTS run_after;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '14')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
