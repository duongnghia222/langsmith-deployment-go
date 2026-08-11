-- 0000003_python_baseline.up.sql
--
-- Consolidates all storage/migrations/0000000-0000035 into a single idempotent
-- migration. Uses CREATE TABLE IF NOT EXISTS and CREATE INDEX IF NOT EXISTS so
-- it is a no-op on DBs that have already had the Python migrations applied
-- (which is every existing dev/staging/prod environment as of cutover).
-- The 0000034 (context, description) and 0000035 (task_path) columns are
-- inlined into the corresponding CREATE TABLE bodies rather than added via
-- ALTER TABLE; CREATE TABLE IF NOT EXISTS makes those bodies a no-op on
-- DBs that already have the tables. Tables that may exist with a partial
-- schema (assistant, thread, run — from test seeds or the 0000001 stub)
-- are reconciled with separate idempotent ALTER TABLE blocks below; see
-- the "Fresh-DB compatibility ALTERs" header for the full caveat list.
--
-- Source files (verify against storage/migrations/ if drift suspected):
--   0000000_setup_extensions
--   0000027_create_assistant
--   0000028_create_thread
--   0000029_create_run
--   0000030_create_checkpoint
--   0000031_create_embedding
--   0000032_create_feedback
--   0000033_create_feedback_analytical_views
--   0000034_add_assistant_context_description
--   0000035_add_task_path_to_checkpoint_writes
--
-- Original migrations 0000001-0000026 are pre-history; their effects are
-- captured in the consolidated CREATE TABLE statements below.

-- ---------------------------------------------------------------------------
-- Extensions (0000000_setup_extensions)
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS vector SCHEMA public;
CREATE EXTENSION IF NOT EXISTS btree_gin SCHEMA public;
CREATE EXTENSION IF NOT EXISTS ltree SCHEMA public;

-- ---------------------------------------------------------------------------
-- Enum (0000032_create_feedback — must precede the feedback table)
-- ---------------------------------------------------------------------------

DO $$ BEGIN
  CREATE TYPE feedback_rating AS ENUM ('thumbs_up', 'thumbs_down');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ---------------------------------------------------------------------------
-- assistant (0000027_create_assistant + inline 0000034 columns)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS assistant (
    assistant_id  uuid                        DEFAULT gen_random_uuid() NOT NULL,
    graph_id      text                        NOT NULL,
    created_at    timestamptz                 DEFAULT now(),
    updated_at    timestamptz                 DEFAULT now(),
    config        jsonb                       DEFAULT '{}'::jsonb NOT NULL,
    metadata      jsonb                       DEFAULT '{}'::jsonb NOT NULL,
    "version"     int4                        DEFAULT 1 NOT NULL,
    "name"        text,
    context       jsonb                       DEFAULT '{}'::jsonb NOT NULL,
    description   text,
    CONSTRAINT assistant_pkey PRIMARY KEY (assistant_id)
);

-- ===========================================================================
-- Fresh-DB compatibility ALTERs (R1 limitation, R2 will reconcile)
--
-- The blocks below patch up tables that may exist with a partial schema:
--   - assistant/thread/run from testdb.SeedBaseSchema (minimal columns)
--   - run from 0000001's self-bootstrap stub (run_id PK + lease columns)
--
-- Postgres ALTER TABLE cannot add a NOT NULL column without a table-level
-- default, so columns added below are nullable AND lose their DEFAULTs even
-- when the inline CREATE TABLE has them. On fresh DBs this means:
--
--   run.run_id              — PK only; loses DEFAULT gen_random_uuid()
--   run.thread_id           — nullable
--   run.status              — nullable; loses DEFAULT 'pending'
--   run.assistant_id        — nullable
--   run.kwargs              — nullable
--   run.multitask_strategy  — nullable (DEFAULT preserved)
--   assistant.graph_id      — nullable
--   assistant.config        — nullable (DEFAULT preserved)
--   assistant.metadata      — nullable (DEFAULT preserved)
--   assistant."version"     — nullable (DEFAULT preserved)
--   assistant.context       — nullable (DEFAULT preserved)
--
-- R2 closes this gap with `ALTER COLUMN ... SET NOT NULL` after backfill.
-- ===========================================================================

-- On DBs where assistant was seeded with a minimal schema (e.g. testdb
-- SeedBaseSchema), add the full column set idempotently.
ALTER TABLE assistant
    ADD COLUMN IF NOT EXISTS graph_id     text,
    ADD COLUMN IF NOT EXISTS updated_at   timestamptz DEFAULT now(),
    ADD COLUMN IF NOT EXISTS config       jsonb       DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS metadata     jsonb       DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS "version"    int4        DEFAULT 1,
    ADD COLUMN IF NOT EXISTS "name"       text,
    ADD COLUMN IF NOT EXISTS context      jsonb       DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS description  text;

-- ---------------------------------------------------------------------------
-- assistant_versions (0000027_create_assistant + inline 0000034 columns)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS assistant_versions (
    assistant_id  uuid                        NOT NULL,
    "version"     int4                        DEFAULT 1 NOT NULL,
    graph_id      text                        NOT NULL,
    config        jsonb                       DEFAULT '{}'::jsonb NOT NULL,
    metadata      jsonb                       DEFAULT '{}'::jsonb NOT NULL,
    created_at    timestamptz                 DEFAULT now(),
    context       jsonb                       DEFAULT '{}'::jsonb NOT NULL,
    description   text,
    CONSTRAINT assistant_versions_pkey PRIMARY KEY (assistant_id, "version"),
    CONSTRAINT assistant_versions_assistant_id_fkey
        FOREIGN KEY (assistant_id) REFERENCES assistant(assistant_id) ON DELETE CASCADE
);

-- ---------------------------------------------------------------------------
-- langchain_pg_collection (0000031_create_embedding)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS langchain_pg_collection (
    "uuid"     uuid         NOT NULL,
    "name"     varchar      NOT NULL,
    cmetadata  json,
    CONSTRAINT langchain_pg_collection_pkey PRIMARY KEY ("uuid"),
    CONSTRAINT langchain_pg_collection_name_key UNIQUE ("name")
);

-- ---------------------------------------------------------------------------
-- thread (0000028_create_thread)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS thread (
    thread_id   uuid        DEFAULT gen_random_uuid() NOT NULL,
    created_at  timestamptz DEFAULT now(),
    updated_at  timestamptz DEFAULT now(),
    metadata    jsonb       DEFAULT '{}'::jsonb NOT NULL,
    status      text        DEFAULT 'idle'::text NOT NULL,
    config      jsonb       DEFAULT '{}'::jsonb NOT NULL,
    "values"    jsonb,
    interrupts  jsonb       DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT thread_pkey PRIMARY KEY (thread_id)
);

-- ---------------------------------------------------------------------------
-- langchain_pg_embedding (0000031_create_embedding)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS langchain_pg_embedding (
    id             varchar    NOT NULL,
    collection_id  uuid,
    embedding      vector,
    "document"     varchar,
    cmetadata      jsonb,
    CONSTRAINT langchain_pg_embedding_pkey PRIMARY KEY (id),
    CONSTRAINT langchain_pg_embedding_collection_id_fkey
        FOREIGN KEY (collection_id) REFERENCES langchain_pg_collection("uuid") ON DELETE CASCADE
);

-- ---------------------------------------------------------------------------
-- langchain_key_value_stores (0000031_create_embedding)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS langchain_key_value_stores (
    namespace  varchar  NOT NULL,
    key        varchar  NOT NULL,
    value      bytea    NOT NULL,
    CONSTRAINT langchain_key_value_stores_pkey PRIMARY KEY (namespace, key)
);

-- ---------------------------------------------------------------------------
-- run (0000029_create_run)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS run (
    run_id              uuid        DEFAULT gen_random_uuid() NOT NULL,
    thread_id           uuid        NOT NULL,
    assistant_id        uuid        NOT NULL,
    created_at          timestamptz DEFAULT now(),
    updated_at          timestamptz DEFAULT now(),
    metadata            jsonb       DEFAULT '{}'::jsonb NOT NULL,
    status              text        DEFAULT 'pending'::text NOT NULL,
    kwargs              jsonb       NOT NULL,
    multitask_strategy  text        DEFAULT 'reject'::text NOT NULL,
    CONSTRAINT run_pkey PRIMARY KEY (run_id),
    CONSTRAINT run_assistant_id_fkey
        FOREIGN KEY (assistant_id) REFERENCES assistant(assistant_id) ON DELETE CASCADE,
    CONSTRAINT run_thread_id_fkey
        FOREIGN KEY (thread_id) REFERENCES thread(thread_id) ON DELETE CASCADE
);

-- On fresh DBs, 0000001 created a stub run table that lacks the full column
-- set. Add missing columns and constraints idempotently so that the indexes
-- below succeed regardless of whether run came from the stub or the Python
-- migrations.
ALTER TABLE run
    ADD COLUMN IF NOT EXISTS assistant_id       uuid,
    ADD COLUMN IF NOT EXISTS created_at         timestamptz DEFAULT now(),
    ADD COLUMN IF NOT EXISTS updated_at         timestamptz DEFAULT now(),
    ADD COLUMN IF NOT EXISTS metadata           jsonb       DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS kwargs             jsonb,
    ADD COLUMN IF NOT EXISTS multitask_strategy text        DEFAULT 'reject'::text;

DO $$ BEGIN
    ALTER TABLE run ADD CONSTRAINT run_assistant_id_fkey
        FOREIGN KEY (assistant_id) REFERENCES assistant(assistant_id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE run ADD CONSTRAINT run_thread_id_fkey
        FOREIGN KEY (thread_id) REFERENCES thread(thread_id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ---------------------------------------------------------------------------
-- checkpoints (0000030_create_checkpoint)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id              uuid   NOT NULL,
    checkpoint_id          uuid   NOT NULL,
    run_id                 uuid   NULL,
    parent_checkpoint_id   uuid   NULL,
    "checkpoint"           jsonb  NOT NULL,
    metadata               jsonb  DEFAULT '{}'::jsonb NOT NULL,
    checkpoint_ns          text   DEFAULT ''::text NOT NULL,
    CONSTRAINT checkpoints_pkey PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id),
    CONSTRAINT checkpoints_run_id_fkey
        FOREIGN KEY (run_id) REFERENCES run(run_id) ON DELETE CASCADE,
    CONSTRAINT checkpoints_thread_id_fkey
        FOREIGN KEY (thread_id) REFERENCES thread(thread_id) ON DELETE CASCADE
);

-- ---------------------------------------------------------------------------
-- checkpoint_writes (0000030_create_checkpoint + inline 0000035 task_path)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id      uuid   NOT NULL,
    checkpoint_id  uuid   NOT NULL,
    task_id        uuid   NOT NULL,
    idx            int4   NOT NULL,
    channel        text   NOT NULL,
    "type"         text   NOT NULL,
    "blob"         bytea  NOT NULL,
    checkpoint_ns  text   DEFAULT ''::text NOT NULL,
    task_path      text   NOT NULL DEFAULT '',
    CONSTRAINT checkpoint_writes_pkey
        PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx),
    CONSTRAINT checkpoint_writes_thread_id_fkey
        FOREIGN KEY (thread_id) REFERENCES thread(thread_id) ON DELETE CASCADE
);

-- ---------------------------------------------------------------------------
-- checkpoint_blobs (0000030_create_checkpoint)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id      uuid   NOT NULL,
    channel        text   NOT NULL,
    "version"      text   NOT NULL,
    "type"         text   NOT NULL,
    "blob"         bytea,
    checkpoint_ns  text   DEFAULT ''::text NOT NULL,
    CONSTRAINT checkpoint_blobs_pkey
        PRIMARY KEY (thread_id, checkpoint_ns, channel, "version"),
    CONSTRAINT checkpoint_blobs_thread_id_fkey
        FOREIGN KEY (thread_id) REFERENCES thread(thread_id) ON DELETE CASCADE
);

-- ---------------------------------------------------------------------------
-- feedback (0000032_create_feedback)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS feedback (
    run_id         uuid             NOT NULL,
    thread_id      uuid             NOT NULL,
    human_message  text             NOT NULL,
    ai_message     text             NOT NULL,
    rating         feedback_rating  NOT NULL,
    feedback_text  text,
    created_at     timestamptz      DEFAULT now(),
    updated_at     timestamptz      DEFAULT now(),
    metadata       jsonb,
    CONSTRAINT feedback_pkey PRIMARY KEY (run_id),
    CONSTRAINT feedback_run_id_fkey
        FOREIGN KEY (run_id) REFERENCES run(run_id) ON DELETE CASCADE,
    CONSTRAINT feedback_thread_id_fkey
        FOREIGN KEY (thread_id) REFERENCES thread(thread_id) ON DELETE CASCADE
);

-- ---------------------------------------------------------------------------
-- Indexes (0000027, 0000028, 0000029, 0000030, 0000031, 0000032)
-- CONCURRENTLY omitted: golang-migrate wraps migrations in a transaction and
-- CREATE INDEX CONCURRENTLY cannot run inside a transaction.
-- ---------------------------------------------------------------------------

-- assistant indexes
CREATE INDEX IF NOT EXISTS assistant_graph_id_idx
    ON assistant USING btree (graph_id, created_at DESC);

CREATE INDEX IF NOT EXISTS assistant_metadata_idx
    ON assistant USING gin (metadata jsonb_path_ops);

-- thread indexes
CREATE INDEX IF NOT EXISTS thread_metadata_idx
    ON thread USING gin (metadata jsonb_path_ops);

CREATE INDEX IF NOT EXISTS thread_status_idx
    ON thread USING btree (status, created_at DESC);

CREATE INDEX IF NOT EXISTS thread_values_idx
    ON thread USING gin ("values" jsonb_path_ops);

-- run indexes
CREATE INDEX IF NOT EXISTS run_assistant_id_idx
    ON run USING btree (assistant_id);

CREATE INDEX IF NOT EXISTS run_metadata_idx
    ON run USING gin (metadata jsonb_path_ops);

CREATE INDEX IF NOT EXISTS run_pending_idx
    ON run USING btree (created_at)
    WHERE (status = 'pending'::text);

CREATE INDEX IF NOT EXISTS run_thread_id_status_idx
    ON run USING btree (thread_id, status);

-- checkpoints indexes
CREATE INDEX IF NOT EXISTS checkpoints_checkpoint_id_idx
    ON checkpoints USING btree (thread_id, checkpoint_id DESC);

CREATE INDEX IF NOT EXISTS checkpoints_run_id_idx
    ON checkpoints USING btree (run_id);

-- langchain_pg_embedding indexes
CREATE INDEX IF NOT EXISTS ix_cmetadata_gin
    ON langchain_pg_embedding USING gin (cmetadata jsonb_path_ops);

-- langchain_key_value_stores indexes
CREATE INDEX IF NOT EXISTS ix_langchain_key_value_stores_key
    ON langchain_key_value_stores USING btree (key);

CREATE INDEX IF NOT EXISTS ix_langchain_key_value_stores_namespace
    ON langchain_key_value_stores USING btree (namespace);

-- feedback indexes
CREATE INDEX IF NOT EXISTS feedback_rating_idx
    ON feedback USING btree (rating);

-- ---------------------------------------------------------------------------
-- Views (0000033_create_feedback_analytical_views)
-- All use CREATE OR REPLACE VIEW — already idempotent.
-- Order: thread_view → thread_analysis → feedback_view → feedback_analysis
--        → thread_feedback_analysis
-- ---------------------------------------------------------------------------

-- Thread view with messages
create or replace view thread_view as
with message_data as (
    select
        thread_id,
        jsonb_array_elements(values -> 'messages') as msg
    from thread
    where values is not NULL
),

filtered_messages as (
    select
        thread_id,
        msg ->> 'type' as msg_type,
        case
            when msg ->> 'type' = 'human' then msg ->> 'content'
            when
                msg ->> 'type' = 'ai'
                and (msg -> 'tool_calls' is NULL or msg -> 'tool_calls' = '[]')
                and (msg ->> 'content' is not NULL and msg ->> 'content' != '')
                then msg ->> 'content'
        end as message_content
    from message_data
    where
        (msg ->> 'type' = 'human')
        or (
            msg ->> 'type' = 'ai' and (msg -> 'tool_calls' is NULL or msg -> 'tool_calls' = '[]')
            and (msg ->> 'content' is not NULL and msg ->> 'content' != '')
        )
),

messages_agg as (
    select
        thread_id,
        array_agg(
            message_content
            order by msg_num
        ) as messages
    from (select
        thread_id,
        row_number() over (
            partition by thread_id
            order by msg_num
        ) as msg_num,
        message_content
    from (select
        fm.thread_id,
        row_number() over () as msg_num,
        fm.message_content
    from filtered_messages as fm
    where fm.message_content is not NULL) as numbered_messages) as ordered_messages
    group by thread_id
),

human_messages_agg as (
    select
        thread_id,
        array_agg(
            message_content
            order by msg_num
        ) as human_messages
    from (select
        thread_id,
        row_number() over (
            partition by thread_id
            order by msg_num
        ) as msg_num,
        message_content
    from (
        select
            fm.thread_id,
            row_number() over () as msg_num,
            fm.message_content
        from filtered_messages as fm
        where
            fm.message_content is not NULL
            and fm.msg_type = 'human'
    ) as numbered_human_messages) as ordered_human_messages
    group by thread_id
),

ai_messages_agg as (
    select
        thread_id,
        array_agg(
            message_content
            order by msg_num
        ) as ai_messages
    from (select
        thread_id,
        row_number() over (
            partition by thread_id
            order by msg_num
        ) as msg_num,
        message_content
    from (
        select
            fm.thread_id,
            row_number() over () as msg_num,
            fm.message_content
        from filtered_messages as fm
        where
            fm.message_content is not NULL
            and fm.msg_type = 'ai'
    ) as numbered_ai_messages) as ordered_ai_messages
    group by thread_id
)

select
    t.thread_id,
    t.created_at,
    t.updated_at,
    t.metadata,
    t.status,
    t.config,
    t.values,
    t.interrupts,
    coalesce(m.messages, array[]::text []) as messages,
    coalesce(h.human_messages, array[]::text []) as human_messages,
    coalesce(a.ai_messages, array[]::text []) as ai_messages
from thread as t
left join
    messages_agg as m on t.thread_id = m.thread_id
left join
    human_messages_agg as h on t.thread_id = h.thread_id
left join
    ai_messages_agg as a on t.thread_id = a.thread_id;


-- Thread view analysis
create or replace view thread_analysis as
select
    metadata::jsonb ->> 'graph_id' as graph_id,
    date(created_at) as day,
    count(thread_id) as total_threads,
    count(case when status = 'idle' then 1 end) as idle_threads,
    count(case when status = 'error' then 1 end) as error_threads,
    count(case when status = 'interrupted' then 1 end) as interrupted_threads,
    sum(array_length(messages, 1)) as total_messages,
    sum(array_length(human_messages, 1)) as human_messages,
    sum(array_length(ai_messages, 1)) as ai_messages,
    round(avg(nullif(array_length(messages, 1), 0)), 2) as average_thread_length,
    min(nullif(array_length(messages, 1), 0)) as shortest_thread_length,
    max(array_length(messages, 1)) as longest_thread_length
from thread_view
group by graph_id, day;


-- Feedback view with feedback comment and expected answer
create or replace view feedback_view as
select
    graph_id,
    run_id,
    thread_id,
    human_message,
    ai_message,
    rating,
    feedback_text,
    -- Final trimmed feedback_comment
    split_part(trimmed_text, E'\n', 1) as feedback_comment,
    -- Final trimmed feedback_expected_answer
    case
        when trimmed_text ~ E'\n'
            then
                nullif(
                    trim(both E' \t\n\r' from regexp_replace(
                        trimmed_text,
                        E'^[^\n]*\n+', -- remove first line + all consecutive newlines after it
                        ''
                    )),
                    ''
                )
    end as feedback_expected_answer,
    created_at,
    updated_at
from (select
    a.graph_id,
    f.run_id,
    f.thread_id,
    f.human_message,
    f.ai_message,
    f.rating,
    f.feedback_text,
    f.created_at,
    f.updated_at,
    -- Trim leading/trailing spaces, tabs, newlines, etc. from the raw text
    trim(both E' \t\n\r' from f.feedback_text) as trimmed_text
from feedback as f
inner join run as r on f.run_id = r.run_id
inner join assistant as a on r.assistant_id = a.assistant_id) as feedback_graph;


-- Feedback view analysis
create or replace view feedback_analysis as
select
    graph_id,
    date(created_at) as day,
    -- Total ratings
    count(case when rating is not NULL then 1 end) as rating_count,
    -- Ratings with and without feedback
    count(case when rating is not NULL and feedback_text is not NULL then 1 end) as rating_w_feedback,
    count(case when rating is not NULL and feedback_text is NULL then 1 end) as rating_wo_feedback,
    -- Thumbs up counts
    count(case when rating = 'thumbs_up' then 1 end) as thumbs_up_count,
    count(case when rating = 'thumbs_up' and feedback_text is not NULL then 1 end) as thumbs_up_w_feedback,
    count(case when rating = 'thumbs_up' and feedback_text is NULL then 1 end) as thumbs_up_wo_feedback,
    -- Thumbs down counts
    count(case when rating = 'thumbs_down' then 1 end) as thumbs_down_count,
    count(case when rating = 'thumbs_down' and feedback_text is not NULL then 1 end) as thumbs_down_w_feedback,
    count(case when rating = 'thumbs_down' and feedback_text is NULL then 1 end) as thumbs_down_wo_feedback,

    -- Rate calculations
    case
        when count(case when rating is not NULL then 1 end) > 0
            then round((
                count(case when rating = 'thumbs_up' then 1 end)::numeric
                / count(case when rating is not NULL then 1 end)
            ) * 100, 2)
        else 0
    end as thumbs_up_rate,

    case
        when count(case when rating is not NULL then 1 end) > 0
            then round((
                count(case when rating = 'thumbs_up' and feedback_text is not NULL then 1 end)::numeric
                / count(case when rating is not NULL then 1 end)
            ) * 100, 2)
        else 0
    end as thumbs_up_w_feedback_rate,

    case
        when count(case when rating is not NULL then 1 end) > 0
            then round((
                count(case when rating = 'thumbs_up' and feedback_text is NULL then 1 end)::numeric
                / count(case when rating is not NULL then 1 end)
            ) * 100, 2)
        else 0
    end as thumbs_up_wo_feedback_rate,

    case
        when count(case when rating is not NULL then 1 end) > 0
            then round((
                count(case when rating = 'thumbs_down' then 1 end)::numeric
                / count(case when rating is not NULL then 1 end)
            ) * 100, 2)
        else 0
    end as thumbs_down_rate,

    case
        when count(case when rating is not NULL then 1 end) > 0
            then round((
                count(case when rating = 'thumbs_down' and feedback_text is not NULL then 1 end)::numeric
                / count(case when rating is not NULL then 1 end)
            ) * 100, 2)
        else 0
    end as thumbs_down_w_feedback_rate,

    case
        when count(case when rating is not NULL then 1 end) > 0
            then round((
                count(case when rating = 'thumbs_down' and feedback_text is NULL then 1 end)::numeric
                / count(case when rating is not NULL then 1 end)
            ) * 100, 2)
        else 0
    end as thumbs_down_wo_feedback_rate
from feedback_view
group by graph_id, day;


-- Thread and Feedback view analysis
create or replace view thread_feedback_analysis as
select
    t.graph_id,
    t.day,
    -- Thread metrics
    t.total_threads,
    t.idle_threads,
    t.error_threads,
    t.interrupted_threads,
    t.total_messages,
    t.human_messages,
    t.ai_messages,
    t.average_thread_length,
    t.shortest_thread_length,
    t.longest_thread_length,
    -- Feedback counts
    f.rating_count,
    f.rating_w_feedback,
    f.rating_wo_feedback,
    f.thumbs_up_count,
    f.thumbs_up_w_feedback,
    f.thumbs_up_wo_feedback,
    f.thumbs_down_count,
    f.thumbs_down_w_feedback,
    f.thumbs_down_wo_feedback,
    -- Feedback rates from feedback_analysis
    f.thumbs_up_rate,
    f.thumbs_up_w_feedback_rate,
    f.thumbs_up_wo_feedback_rate,
    f.thumbs_down_rate,
    f.thumbs_down_w_feedback_rate,
    f.thumbs_down_wo_feedback_rate,
    -- Updated feedback_rate calculation (based on ai_messages)
    case
        when t.ai_messages > 0 then round((f.rating_count::numeric / t.ai_messages) * 100, 2)
        else 0
    end as rating_rate,
    case
        when t.ai_messages > 0 then round((f.rating_w_feedback::numeric / t.ai_messages) * 100, 2)
        else 0
    end as feedback_rate
from thread_analysis as t
left join
    feedback_analysis as f on t.graph_id = f.graph_id and t.day = f.day;
