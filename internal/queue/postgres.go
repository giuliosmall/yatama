package queue

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"github.com/giulio/task-manager/internal/task"
)

// PostgresProducer implements Producer as a no-op because the task is already
// persisted by Repository.CreateTask before Enqueue is called. This adapter
// exists so the API layer can use the Producer interface uniformly regardless
// of whether the backend is PostgreSQL or Kafka.
type PostgresProducer struct{}

// NewPostgresProducer returns a Producer that does nothing on Enqueue.
func NewPostgresProducer() *PostgresProducer {
	return &PostgresProducer{}
}

func (p *PostgresProducer) Enqueue(_ context.Context, _ Message) error { return nil }
func (p *PostgresProducer) Close() error                               { return nil }

// PostgresConsumer implements Consumer by polling Repository.ClaimTask.
// It bridges the existing PostgreSQL-based queue to the Consumer interface.
type PostgresConsumer struct {
	repo         task.Repository
	pollInterval time.Duration
}

// NewPostgresConsumer creates a Consumer that polls the repository for queued
// tasks at the given interval (default 1s).
func NewPostgresConsumer(repo task.Repository, pollInterval time.Duration) *PostgresConsumer {
	if pollInterval <= 0 {
		pollInterval = 1 * time.Second
	}
	return &PostgresConsumer{
		repo:         repo,
		pollInterval: pollInterval,
	}
}

// Consume starts a goroutine that polls ClaimTask and sends claimed tasks to
// the returned channel. The goroutine exits when ctx is cancelled.
func (c *PostgresConsumer) Consume(ctx context.Context) (<-chan Delivery, error) {
	ch := make(chan Delivery)

	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			t, err := c.repo.ClaimTask(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("postgres consumer: failed to claim task", "error", err)
				if !c.sleep(ctx, c.pollInterval) {
					return
				}
				continue
			}

			if t == nil {
				jitter := time.Duration(rand.Int63n(int64(500 * time.Millisecond)))
				if !c.sleep(ctx, c.pollInterval+jitter) {
					return
				}
				continue
			}

			msg := taskToMessage(t)
			d := Delivery{
				Message: msg,
				Ack:     func() error { return nil }, // PG already committed in ClaimTask
				Nack: func() error {
					return c.repo.RequeueTask(ctx, t.ID)
				},
			}

			select {
			case ch <- d:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

func (c *PostgresConsumer) Close() error { return nil }

// sleep pauses for d but returns false immediately if ctx is cancelled.
func (c *PostgresConsumer) sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// taskToMessage converts a task.Task to a queue.Message.
func taskToMessage(t *task.Task) Message {
	var idemKey string
	if t.IdempotencyKey != nil {
		idemKey = *t.IdempotencyKey
	}
	return Message{
		TaskID:         t.ID,
		Name:           t.Name,
		Type:           t.Type,
		Payload:        t.Payload,
		Priority:       t.Priority,
		MaxRetries:     t.MaxRetries,
		TimeoutSeconds: t.TimeoutSeconds,
		RetryCount:     t.RetryCount,
		IdempotencyKey: idemKey,
	}
}
