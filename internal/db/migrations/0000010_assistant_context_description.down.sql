ALTER TABLE assistant_versions
    DROP COLUMN IF EXISTS description,
    DROP COLUMN IF EXISTS context;

ALTER TABLE assistant
    DROP COLUMN IF EXISTS description,
    DROP COLUMN IF EXISTS context;
