-- Partial index for the reaper: efficiently finds tasks stuck in "running".
CREATE INDEX idx_tasks_stuck ON tasks (updated_at ASC) WHERE status = 'running';

-- Optional caller-supplied idempotency key for deduplication.
ALTER TABLE tasks ADD COLUMN idempotency_key TEXT;

-- Partial unique index: only non-NULL keys are checked for uniqueness.
CREATE UNIQUE INDEX idx_tasks_idempotency_key ON tasks (idempotency_key) WHERE idempotency_key IS NOT NULL;
