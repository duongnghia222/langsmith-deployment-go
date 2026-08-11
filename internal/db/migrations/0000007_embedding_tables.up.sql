-- Port of storage/migrations/0000031_create_embedding.up.sql
-- RQ2: No gRPC Store service is built in R5. These tables are migrated so LSD
-- owns the full schema post-cutover. LangGraph's in-process AsyncPostgresStore
-- continues to access them directly.
-- pgvector extension is created by 0000003_python_baseline.up.sql.

CREATE TABLE IF NOT EXISTS langchain_pg_collection (
    uuid      uuid        NOT NULL,
    name      varchar     NOT NULL,
    cmetadata json,
    CONSTRAINT langchain_pg_collection_name_key UNIQUE (name),
    CONSTRAINT langchain_pg_collection_pkey PRIMARY KEY (uuid)
);

CREATE TABLE IF NOT EXISTS langchain_pg_embedding (
    id            varchar NOT NULL,
    collection_id uuid,
    embedding     vector,
    document      varchar,
    cmetadata     jsonb,
    CONSTRAINT langchain_pg_embedding_pkey PRIMARY KEY (id),
    CONSTRAINT langchain_pg_embedding_collection_id_fkey
        FOREIGN KEY (collection_id)
        REFERENCES langchain_pg_collection(uuid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS langchain_key_value_stores (
    namespace varchar NOT NULL,
    key       varchar NOT NULL,
    value     bytea   NOT NULL,
    CONSTRAINT langchain_key_value_stores_pkey PRIMARY KEY (namespace, key)
);

CREATE INDEX IF NOT EXISTS ix_cmetadata_gin
    ON langchain_pg_embedding USING gin (cmetadata jsonb_path_ops);
CREATE INDEX IF NOT EXISTS ix_langchain_key_value_stores_key
    ON langchain_key_value_stores USING btree (key);
CREATE INDEX IF NOT EXISTS ix_langchain_key_value_stores_namespace
    ON langchain_key_value_stores USING btree (namespace);

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '7')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
