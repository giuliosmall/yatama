// Package queue defines the interfaces for task queue backends.
// It decouples task dispatch (enqueue) and consumption (dequeue) from the
// underlying transport, allowing PostgreSQL and Kafka implementations to be
// swapped transparently.
package queue

import (
	"context"

	"github.com/google/uuid"
)

// Message carries the data needed to dispatch and execute a task.
type Message struct {
	TaskID         uuid.UUID
	Name           string
	Type           string
	Payload        map[string]string
	Priority       int
	MaxRetries     int
	TimeoutSeconds int
	RetryCount     int
	IdempotencyKey string
	Meta           map[string]string // backend-specific metadata (partition, offset, etc.)
}

// Delivery wraps a Message with acknowledgement callbacks.
// After processing, the consumer must call either Ack (success) or Nack
// (failure / redeliver).
type Delivery struct {
	Message Message
	Ack     func() error // confirm processing complete
	Nack    func() error // reject; message will be redelivered
}

// Producer publishes tasks to the queue. Used by API pods.
type Producer interface {
	Enqueue(ctx context.Context, msg Message) error
	Close() error
}

// Consumer receives tasks from the queue. Used by worker pods.
type Consumer interface {
	// Consume returns a channel that yields Delivery values until the
	// context is cancelled or Close is called.
	Consume(ctx context.Context) (<-chan Delivery, error)
	Close() error
}
