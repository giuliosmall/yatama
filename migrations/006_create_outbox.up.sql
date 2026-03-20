-- 006_create_outbox.up.sql
-- Transactional outbox for reliable task dispatch to Kafka.
-- Tasks and their outbox entries are inserted in the same PG transaction,
-- guaranteeing at-least-once delivery to the message broker.

CREATE TABLE outbox (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id      UUID        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- Partial index for the publisher poll query: only unpublished rows.
CREATE INDEX idx_outbox_unpublished ON outbox (created_at ASC)
    WHERE published_at IS NULL;
