-- Worker IDs are not required to be UUIDs (they can be any identifier string).
ALTER TABLE run ALTER COLUMN lease_holder_id TYPE TEXT;
