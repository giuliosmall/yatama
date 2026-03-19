-- 001_create_tables.down.sql
-- Rolls back the initial schema. Drop task_history first due to FK dependency on tasks.

DROP TABLE IF EXISTS task_history;
DROP TABLE IF EXISTS tasks;
