ALTER TABLE run ALTER COLUMN lease_holder_id TYPE UUID USING lease_holder_id::uuid;
