-- Reverse 0000003_python_baseline. Drops in reverse dependency order.
-- WARNING: destructive. Only run in dev environments.

DROP VIEW IF EXISTS thread_feedback_analysis;
DROP VIEW IF EXISTS feedback_analysis;
DROP VIEW IF EXISTS feedback_view;
DROP VIEW IF EXISTS thread_analysis;
DROP VIEW IF EXISTS thread_view;
DROP TABLE IF EXISTS feedback;
DROP TABLE IF EXISTS langchain_key_value_stores;
DROP TABLE IF EXISTS langchain_pg_embedding;
DROP TABLE IF EXISTS langchain_pg_collection;
DROP TABLE IF EXISTS checkpoint_writes;
DROP TABLE IF EXISTS checkpoint_blobs;
DROP TABLE IF EXISTS checkpoints;
DROP TABLE IF EXISTS run;
DROP TABLE IF EXISTS thread;
DROP TABLE IF EXISTS assistant_versions;
DROP TABLE IF EXISTS assistant;
DROP TYPE IF EXISTS feedback_rating;
-- Extensions: keep (other tools may use them).
