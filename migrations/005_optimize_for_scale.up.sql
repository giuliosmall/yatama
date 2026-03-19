-- 005_optimize_for_scale.up.sql
-- Optimizations for high-throughput (1M+ tasks/sec) operation:
-- 1. Partition task_history by month for faster writes and cheaper cleanup.
-- 2. Add index for worker idempotency checks on terminal task states.
-- 3. Drop the FK constraint on task_history to eliminate lock contention
--    during bulk inserts. Referential integrity is maintained by application logic.

-- ---------------------------------------------------------------------------
-- 1. Convert task_history to a partitioned table
-- ---------------------------------------------------------------------------

-- Rename the existing table so we can recreate it as partitioned.
ALTER TABLE task_history RENAME TO task_history_old;

-- Drop the old indexes (they reference the old table).
DROP INDEX IF EXISTS idx_task_history_task_id;

-- Create the new partitioned table (no PK with gen_random_uuid on partitioned
-- tables — use a composite key or no PK; for an append-only history table
-- a simple index is more practical than a UUID PK).
CREATE TABLE task_history (
    id          UUID        DEFAULT gen_random_uuid(),
    task_id     UUID        NOT NULL,
    status      TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (occurred_at);

-- Create partitions covering a reasonable window. In production, a cron job
-- or pg_partman would create future partitions automatically.
CREATE TABLE task_history_2026_03 PARTITION OF task_history
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE task_history_2026_04 PARTITION OF task_history
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE task_history_2026_05 PARTITION OF task_history
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE task_history_2026_06 PARTITION OF task_history
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

-- A default partition catches any rows outside the defined ranges.
CREATE TABLE task_history_default PARTITION OF task_history DEFAULT;

-- Migrate existing data into the new partitioned table.
INSERT INTO task_history (id, task_id, status, occurred_at)
SELECT id, task_id, status, occurred_at FROM task_history_old;

-- Drop the old table.
DROP TABLE task_history_old;

-- Recreate the composite index on the partitioned table.
-- PostgreSQL automatically creates per-partition indexes.
CREATE INDEX idx_task_history_task_id ON task_history (task_id, occurred_at ASC);

-- ---------------------------------------------------------------------------
-- 2. Index for worker idempotency checks
-- ---------------------------------------------------------------------------
-- Workers check if a task is already in a terminal state before executing.
-- This partial index makes that lookup a cheap index-only scan.
CREATE INDEX idx_tasks_terminal_status
    ON tasks (id)
    WHERE status IN ('succeeded', 'failed', 'dead_lettered');

-- ---------------------------------------------------------------------------
-- 3. Connection pool sizing hint
-- ---------------------------------------------------------------------------
-- No DDL needed, but note: configure pgxpool.MaxConns = 25, MinConns = 5,
-- and use PgBouncer in transaction mode between the app and PostgreSQL.
