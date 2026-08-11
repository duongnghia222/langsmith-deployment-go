-- Port of storage/migrations/0000033_create_feedback_analytical_views.up.sql
-- CREATE OR REPLACE VIEW is naturally idempotent.
--
-- NOTE: These views are already created by 0000003_python_baseline.up.sql.
-- This migration is kept as a 1:1 mirror of the source storage migration; it
-- safely re-runs because CREATE OR REPLACE VIEW does not error on re-execution.

-- Thread view with messages
CREATE OR REPLACE VIEW thread_view AS
WITH message_data AS (
    SELECT
        thread_id,
        jsonb_array_elements(values -> 'messages') AS msg
    FROM thread
    WHERE values IS NOT NULL
),
filtered_messages AS (
    SELECT
        thread_id,
        msg ->> 'type' AS msg_type,
        CASE
            WHEN msg ->> 'type' = 'human' THEN msg ->> 'content'
            WHEN
                msg ->> 'type' = 'ai'
                AND (msg -> 'tool_calls' IS NULL OR msg -> 'tool_calls' = '[]')
                AND (msg ->> 'content' IS NOT NULL AND msg ->> 'content' != '')
                THEN msg ->> 'content'
        END AS message_content
    FROM message_data
    WHERE
        (msg ->> 'type' = 'human')
        OR (
            msg ->> 'type' = 'ai'
            AND (msg -> 'tool_calls' IS NULL OR msg -> 'tool_calls' = '[]')
            AND (msg ->> 'content' IS NOT NULL AND msg ->> 'content' != '')
        )
),
messages_agg AS (
    SELECT
        thread_id,
        array_agg(message_content ORDER BY msg_num) AS messages
    FROM (
        SELECT
            thread_id,
            row_number() OVER (PARTITION BY thread_id ORDER BY msg_num) AS msg_num,
            message_content
        FROM (
            SELECT
                fm.thread_id,
                row_number() OVER () AS msg_num,
                fm.message_content
            FROM filtered_messages AS fm
            WHERE fm.message_content IS NOT NULL
        ) AS numbered_messages
    ) AS ordered_messages
    GROUP BY thread_id
),
human_messages_agg AS (
    SELECT
        thread_id,
        array_agg(message_content ORDER BY msg_num) AS human_messages
    FROM (
        SELECT
            thread_id,
            row_number() OVER (PARTITION BY thread_id ORDER BY msg_num) AS msg_num,
            message_content
        FROM (
            SELECT
                fm.thread_id,
                row_number() OVER () AS msg_num,
                fm.message_content
            FROM filtered_messages AS fm
            WHERE fm.message_content IS NOT NULL AND fm.msg_type = 'human'
        ) AS numbered_human_messages
    ) AS ordered_human_messages
    GROUP BY thread_id
),
ai_messages_agg AS (
    SELECT
        thread_id,
        array_agg(message_content ORDER BY msg_num) AS ai_messages
    FROM (
        SELECT
            thread_id,
            row_number() OVER (PARTITION BY thread_id ORDER BY msg_num) AS msg_num,
            message_content
        FROM (
            SELECT
                fm.thread_id,
                row_number() OVER () AS msg_num,
                fm.message_content
            FROM filtered_messages AS fm
            WHERE fm.message_content IS NOT NULL AND fm.msg_type = 'ai'
        ) AS numbered_ai_messages
    ) AS ordered_ai_messages
    GROUP BY thread_id
)
SELECT
    t.thread_id,
    t.created_at,
    t.updated_at,
    t.metadata,
    t.status,
    t.config,
    t.values,
    t.interrupts,
    COALESCE(m.messages, ARRAY[]::text[]) AS messages,
    COALESCE(h.human_messages, ARRAY[]::text[]) AS human_messages,
    COALESCE(a.ai_messages, ARRAY[]::text[]) AS ai_messages
FROM thread AS t
LEFT JOIN messages_agg AS m ON t.thread_id = m.thread_id
LEFT JOIN human_messages_agg AS h ON t.thread_id = h.thread_id
LEFT JOIN ai_messages_agg AS a ON t.thread_id = a.thread_id;


-- Thread analysis view
CREATE OR REPLACE VIEW thread_analysis AS
SELECT
    metadata::jsonb ->> 'graph_id' AS graph_id,
    date(created_at) AS day,
    count(thread_id) AS total_threads,
    count(CASE WHEN status = 'idle' THEN 1 END) AS idle_threads,
    count(CASE WHEN status = 'error' THEN 1 END) AS error_threads,
    count(CASE WHEN status = 'interrupted' THEN 1 END) AS interrupted_threads,
    sum(array_length(messages, 1)) AS total_messages,
    sum(array_length(human_messages, 1)) AS human_messages,
    sum(array_length(ai_messages, 1)) AS ai_messages,
    round(avg(nullif(array_length(messages, 1), 0)), 2) AS average_thread_length,
    min(nullif(array_length(messages, 1), 0)) AS shortest_thread_length,
    max(array_length(messages, 1)) AS longest_thread_length
FROM thread_view
GROUP BY graph_id, day;


-- Feedback view
CREATE OR REPLACE VIEW feedback_view AS
SELECT
    graph_id,
    run_id,
    thread_id,
    human_message,
    ai_message,
    rating,
    feedback_text,
    split_part(trimmed_text, E'\n', 1) AS feedback_comment,
    CASE
        WHEN trimmed_text ~ E'\n'
            THEN nullif(
                trim(both E' \t\n\r' FROM regexp_replace(
                    trimmed_text,
                    E'^[^\n]*\n+',
                    ''
                )),
                ''
            )
    END AS feedback_expected_answer,
    created_at,
    updated_at
FROM (
    SELECT
        a.graph_id,
        f.run_id,
        f.thread_id,
        f.human_message,
        f.ai_message,
        f.rating,
        f.feedback_text,
        f.created_at,
        f.updated_at,
        trim(both E' \t\n\r' FROM f.feedback_text) AS trimmed_text
    FROM feedback AS f
    INNER JOIN run AS r ON f.run_id = r.run_id
    INNER JOIN assistant AS a ON r.assistant_id = a.assistant_id
) AS feedback_graph;


-- Feedback analysis view
CREATE OR REPLACE VIEW feedback_analysis AS
SELECT
    graph_id,
    date(created_at) AS day,
    count(CASE WHEN rating IS NOT NULL THEN 1 END) AS rating_count,
    count(CASE WHEN rating IS NOT NULL AND feedback_text IS NOT NULL THEN 1 END) AS rating_w_feedback,
    count(CASE WHEN rating IS NOT NULL AND feedback_text IS NULL THEN 1 END) AS rating_wo_feedback,
    count(CASE WHEN rating = 'thumbs_up' THEN 1 END) AS thumbs_up_count,
    count(CASE WHEN rating = 'thumbs_up' AND feedback_text IS NOT NULL THEN 1 END) AS thumbs_up_w_feedback,
    count(CASE WHEN rating = 'thumbs_up' AND feedback_text IS NULL THEN 1 END) AS thumbs_up_wo_feedback,
    count(CASE WHEN rating = 'thumbs_down' THEN 1 END) AS thumbs_down_count,
    count(CASE WHEN rating = 'thumbs_down' AND feedback_text IS NOT NULL THEN 1 END) AS thumbs_down_w_feedback,
    count(CASE WHEN rating = 'thumbs_down' AND feedback_text IS NULL THEN 1 END) AS thumbs_down_wo_feedback,
    CASE
        WHEN count(CASE WHEN rating IS NOT NULL THEN 1 END) > 0
            THEN round((count(CASE WHEN rating = 'thumbs_up' THEN 1 END)::numeric
                / count(CASE WHEN rating IS NOT NULL THEN 1 END)) * 100, 2)
        ELSE 0
    END AS thumbs_up_rate,
    CASE
        WHEN count(CASE WHEN rating IS NOT NULL THEN 1 END) > 0
            THEN round((count(CASE WHEN rating = 'thumbs_up' AND feedback_text IS NOT NULL THEN 1 END)::numeric
                / count(CASE WHEN rating IS NOT NULL THEN 1 END)) * 100, 2)
        ELSE 0
    END AS thumbs_up_w_feedback_rate,
    CASE
        WHEN count(CASE WHEN rating IS NOT NULL THEN 1 END) > 0
            THEN round((count(CASE WHEN rating = 'thumbs_up' AND feedback_text IS NULL THEN 1 END)::numeric
                / count(CASE WHEN rating IS NOT NULL THEN 1 END)) * 100, 2)
        ELSE 0
    END AS thumbs_up_wo_feedback_rate,
    CASE
        WHEN count(CASE WHEN rating IS NOT NULL THEN 1 END) > 0
            THEN round((count(CASE WHEN rating = 'thumbs_down' THEN 1 END)::numeric
                / count(CASE WHEN rating IS NOT NULL THEN 1 END)) * 100, 2)
        ELSE 0
    END AS thumbs_down_rate,
    CASE
        WHEN count(CASE WHEN rating IS NOT NULL THEN 1 END) > 0
            THEN round((count(CASE WHEN rating = 'thumbs_down' AND feedback_text IS NOT NULL THEN 1 END)::numeric
                / count(CASE WHEN rating IS NOT NULL THEN 1 END)) * 100, 2)
        ELSE 0
    END AS thumbs_down_w_feedback_rate,
    CASE
        WHEN count(CASE WHEN rating IS NOT NULL THEN 1 END) > 0
            THEN round((count(CASE WHEN rating = 'thumbs_down' AND feedback_text IS NULL THEN 1 END)::numeric
                / count(CASE WHEN rating IS NOT NULL THEN 1 END)) * 100, 2)
        ELSE 0
    END AS thumbs_down_wo_feedback_rate
FROM feedback_view
GROUP BY graph_id, day;


-- Combined thread + feedback analysis view
CREATE OR REPLACE VIEW thread_feedback_analysis AS
SELECT
    t.graph_id,
    t.day,
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
    f.rating_count,
    f.rating_w_feedback,
    f.rating_wo_feedback,
    f.thumbs_up_count,
    f.thumbs_up_w_feedback,
    f.thumbs_up_wo_feedback,
    f.thumbs_down_count,
    f.thumbs_down_w_feedback,
    f.thumbs_down_wo_feedback,
    f.thumbs_up_rate,
    f.thumbs_up_w_feedback_rate,
    f.thumbs_up_wo_feedback_rate,
    f.thumbs_down_rate,
    f.thumbs_down_w_feedback_rate,
    f.thumbs_down_wo_feedback_rate,
    CASE
        WHEN t.ai_messages > 0
            THEN round((f.rating_count::numeric / t.ai_messages) * 100, 2)
        ELSE 0
    END AS rating_rate,
    CASE
        WHEN t.ai_messages > 0
            THEN round((f.rating_w_feedback::numeric / t.ai_messages) * 100, 2)
        ELSE 0
    END AS feedback_rate
FROM thread_analysis AS t
LEFT JOIN feedback_analysis AS f ON t.graph_id = f.graph_id AND t.day = f.day;

INSERT INTO lsd_meta (key, value) VALUES ('schema_version', '9')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
