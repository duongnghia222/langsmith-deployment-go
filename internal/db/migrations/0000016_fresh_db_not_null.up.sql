-- Close the fresh-DB schema gap documented in 0000003_python_baseline:
-- columns added via ALTER TABLE ADD COLUMN are nullable (and run.run_id /
-- run.status lost their DEFAULTs) relative to the Python reference schema
-- (storage/migrations/0000029_create_run.up.sql, 0000027_create_assistant.up.sql,
-- 0000034_add_assistant_context_description.up.sql).
-- Backfill defaultable columns, then restore DEFAULTs and enforce NOT NULL.
-- On DBs migrated by the Python baseline these statements are no-ops.

-- ── run (reference: 0000029_create_run) ─────────────────────────────────────
ALTER TABLE run ALTER COLUMN run_id SET DEFAULT gen_random_uuid();

UPDATE run SET status = 'pending' WHERE status IS NULL;
ALTER TABLE run ALTER COLUMN status SET DEFAULT 'pending'::text;
ALTER TABLE run ALTER COLUMN status SET NOT NULL;

UPDATE run SET metadata = '{}'::jsonb WHERE metadata IS NULL;
ALTER TABLE run ALTER COLUMN metadata SET DEFAULT '{}'::jsonb;
ALTER TABLE run ALTER COLUMN metadata SET NOT NULL;

-- Reference has no DEFAULT on kwargs; backfill only.
UPDATE run SET kwargs = '{}'::jsonb WHERE kwargs IS NULL;
ALTER TABLE run ALTER COLUMN kwargs SET NOT NULL;

UPDATE run SET multitask_strategy = 'reject' WHERE multitask_strategy IS NULL;
ALTER TABLE run ALTER COLUMN multitask_strategy SET DEFAULT 'reject'::text;
ALTER TABLE run ALTER COLUMN multitask_strategy SET NOT NULL;

-- thread_id / assistant_id cannot be invented by backfill. LSD code paths
-- always supply both, so violating rows should not exist; fail loudly if any do.
ALTER TABLE run ALTER COLUMN thread_id SET NOT NULL;
ALTER TABLE run ALTER COLUMN assistant_id SET NOT NULL;

-- ── assistant (reference: 0000027_create_assistant + 0000034 columns) ───────
ALTER TABLE assistant ALTER COLUMN assistant_id SET DEFAULT gen_random_uuid();

UPDATE assistant SET config = '{}'::jsonb WHERE config IS NULL;
ALTER TABLE assistant ALTER COLUMN config SET DEFAULT '{}'::jsonb;
ALTER TABLE assistant ALTER COLUMN config SET NOT NULL;

UPDATE assistant SET metadata = '{}'::jsonb WHERE metadata IS NULL;
ALTER TABLE assistant ALTER COLUMN metadata SET DEFAULT '{}'::jsonb;
ALTER TABLE assistant ALTER COLUMN metadata SET NOT NULL;

UPDATE assistant SET "version" = 1 WHERE "version" IS NULL;
ALTER TABLE assistant ALTER COLUMN "version" SET DEFAULT 1;
ALTER TABLE assistant ALTER COLUMN "version" SET NOT NULL;

UPDATE assistant SET context = '{}'::jsonb WHERE context IS NULL;
ALTER TABLE assistant ALTER COLUMN context SET DEFAULT '{}'::jsonb;
ALTER TABLE assistant ALTER COLUMN context SET NOT NULL;

-- Reference has no DEFAULT on graph_id and no backfill is possible.
ALTER TABLE assistant ALTER COLUMN graph_id SET NOT NULL;
