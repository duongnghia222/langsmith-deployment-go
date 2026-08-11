-- Best-effort reversal: relax the constraints added by the up migration.
-- (On Python-baseline DBs these constraints pre-existed; dropping them here
-- weakens the schema relative to the reference, as down migrations do.)
ALTER TABLE run ALTER COLUMN status DROP NOT NULL;
ALTER TABLE run ALTER COLUMN metadata DROP NOT NULL;
ALTER TABLE run ALTER COLUMN kwargs DROP NOT NULL;
ALTER TABLE run ALTER COLUMN multitask_strategy DROP NOT NULL;
ALTER TABLE run ALTER COLUMN thread_id DROP NOT NULL;
ALTER TABLE run ALTER COLUMN assistant_id DROP NOT NULL;

ALTER TABLE assistant ALTER COLUMN config DROP NOT NULL;
ALTER TABLE assistant ALTER COLUMN metadata DROP NOT NULL;
ALTER TABLE assistant ALTER COLUMN "version" DROP NOT NULL;
ALTER TABLE assistant ALTER COLUMN context DROP NOT NULL;
ALTER TABLE assistant ALTER COLUMN graph_id DROP NOT NULL;
