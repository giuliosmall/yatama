-- 002_create_dead_letter_queue.up.sql
-- Adds the dead_letter_queue table for tasks that exhaust all retries.

CREATE TABLE dead_letter_queue (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    original_task_id    UUID        NOT NULL REFERENCES tasks(id),
    name                TEXT        NOT NULL,
    type                TEXT        NOT NULL,
    payload             JSONB       NOT NULL DEFAULT '{}',
    priority            INT         NOT NULL DEFAULT 0,
    retry_count         INT         NOT NULL DEFAULT 0,
    max_retries         INT         NOT NULL DEFAULT 3,
    timeout_seconds     INT         NOT NULL DEFAULT 30,
    error_message       TEXT        NOT NULL DEFAULT '',
    original_created_at TIMESTAMPTZ NOT NULL,
    dead_lettered_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dlq_dead_lettered_at ON dead_letter_queue (dead_lettered_at DESC);
CREATE INDEX idx_dlq_type ON dead_letter_queue (type);
