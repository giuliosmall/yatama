-- 005_optimize_for_scale.down.sql
-- Reverts the scale optimizations: unpartitions task_history, drops indexes.

DROP INDEX IF EXISTS idx_tasks_terminal_status;

-- Collect data from the partitioned table.
CREATE TABLE task_history_partitioned_backup AS SELECT * FROM task_history;

-- Drop the partitioned table and all partitions.
DROP TABLE task_history;

-- Recreate the original non-partitioned table.
CREATE TABLE task_history (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id     UUID        NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    status      TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Restore data.
INSERT INTO task_history (id, task_id, status, occurred_at)
SELECT id, task_id, status, occurred_at FROM task_history_partitioned_backup;

DROP TABLE task_history_partitioned_backup;

-- Recreate the original index.
CREATE INDEX idx_task_history_task_id ON task_history (task_id, occurred_at ASC);
