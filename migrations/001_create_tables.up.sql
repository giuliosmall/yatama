-- 001_create_tables.up.sql
-- Creates the tasks and task_history tables for the background task management system.

CREATE TABLE tasks (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT        NOT NULL,
    type            TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'queued',
    priority        INT         NOT NULL DEFAULT 0,
    retry_count     INT         NOT NULL DEFAULT 0,
    max_retries     INT         NOT NULL DEFAULT 3,
    timeout_seconds INT         NOT NULL DEFAULT 30,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE task_history (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id     UUID        NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    status      TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Partial index for the worker polling query: claims highest-priority queued task first.
CREATE INDEX idx_tasks_claim ON tasks (priority DESC, created_at ASC) WHERE status = 'queued';

-- Composite index for fetching history rows by task, ordered chronologically.
CREATE INDEX idx_task_history_task_id ON task_history (task_id, occurred_at ASC);
