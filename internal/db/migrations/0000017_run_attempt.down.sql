ALTER TABLE run
    DROP COLUMN IF EXISTS attempt;

-- 0000016 did not bump schema_version, so the last value actually written
-- by a prior migration is '15' (from 0000015_run_after).
INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '15')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
