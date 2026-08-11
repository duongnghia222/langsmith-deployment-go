DROP TABLE IF EXISTS lsd_meta;
DROP INDEX IF EXISTS run_cancel_idx;
DROP INDEX IF EXISTS run_expired_lease_idx;
ALTER TABLE run
  DROP COLUMN IF EXISTS cancel_requested_at,
  DROP COLUMN IF EXISTS lease_generation,
  DROP COLUMN IF EXISTS lease_expires_at,
  DROP COLUMN IF EXISTS lease_holder_id;
DROP TABLE IF EXISTS run;
