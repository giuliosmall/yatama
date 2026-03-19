-- 002_create_dead_letter_queue.down.sql
-- Rolls back the dead-letter queue schema.

DROP INDEX IF EXISTS idx_dlq_type;
DROP INDEX IF EXISTS idx_dlq_dead_lettered_at;
DROP TABLE IF EXISTS dead_letter_queue;
