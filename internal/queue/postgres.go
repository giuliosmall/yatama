package queue

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
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
func (p *PostgresProducer) Flush(_ context.Context) error              { return nil }
func (p *PostgresProducer) Close() error                               { return nil }

// PostgresConsumer implements Consumer by polling Repository.ClaimTask.
// It bridges the existing PostgreSQL-based queue to the Consumer interface.
// Multiple polling goroutines run concurrently to maximize throughput.
type PostgresConsumer struct {
	repo         task.Repository
	pollInterval time.Duration
	pollers      int // number of concurrent polling goroutines
}

// NewPostgresConsumer creates a Consumer that polls the repository for queued
// tasks. pollers controls how many concurrent goroutines call ClaimTask.
// Use pollers >= your worker concurrency for best throughput.
func NewPostgresConsumer(repo task.Repository, pollInterval time.Duration, pollers int) *PostgresConsumer {
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}
	if pollers <= 0 {
		pollers = 16
	}
	return &PostgresConsumer{
		repo:         repo,
		pollInterval: pollInterval,
		pollers:      pollers,
	}
}

// Consume starts multiple polling goroutines that call ClaimTask concurrently
// and feed claimed tasks into the returned channel.
func (c *PostgresConsumer) Consume(ctx context.Context) (<-chan Delivery, error) {
	ch := make(chan Delivery, c.pollers*2)
	var wg sync.WaitGroup

	for i := 0; i < c.pollers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.poll(ctx, ch)
		}()
	}

	// Close the channel when all pollers exit.
	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch, nil
}

func (c *PostgresConsumer) poll(ctx context.Context, ch chan<- Delivery) {
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
			// No tasks available — back off with jitter.
			jitter := time.Duration(rand.Int63n(int64(200 * time.Millisecond)))
			if !c.sleep(ctx, c.pollInterval+jitter) {
				return
			}
			continue
		}

		// Task claimed — deliver immediately, no sleep.
		msg := taskToMessage(t)
		d := Delivery{
			Message: msg,
			Ack:     func() error { return nil },
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
