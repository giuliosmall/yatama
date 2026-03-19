DROP INDEX IF EXISTS idx_tasks_idempotency_key;
ALTER TABLE tasks DROP COLUMN IF EXISTS idempotency_key;
DROP INDEX IF EXISTS idx_tasks_stuck;
