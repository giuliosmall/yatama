package task

import (
	"time"

	"github.com/google/uuid"
)

// Task represents a unit of background work stored in the tasks table.
type Task struct {
	ID             uuid.UUID         `json:"id"`
	Name           string            `json:"name"`
	Type           string            `json:"type"`
	Payload        map[string]string `json:"payload"`
	Status         string            `json:"status"`
	Priority       int               `json:"priority"`
	RetryCount     int               `json:"retry_count"`
	MaxRetries     int               `json:"max_retries"`
	TimeoutSeconds int               `json:"timeout_seconds"`
	IdempotencyKey *string           `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	RunAfter       time.Time         `json:"run_after"`
}

// TaskHistory records a single status change for a task.
type TaskHistory struct {
	ID         uuid.UUID `json:"id"`
	TaskID     uuid.UUID `json:"task_id"`
	Status     string    `json:"status"`
	OccurredAt time.Time `json:"occurred_at"`
}

// DeadLetterTask represents a task that has been moved to the dead-letter queue
// after exhausting all retries.
type DeadLetterTask struct {
	ID                uuid.UUID         `json:"id"`
	OriginalTaskID    uuid.UUID         `json:"original_task_id"`
	Name              string            `json:"name"`
	Type              string            `json:"type"`
	Payload           map[string]string `json:"payload"`
	Priority          int               `json:"priority"`
	RetryCount        int               `json:"retry_count"`
	MaxRetries        int               `json:"max_retries"`
	TimeoutSeconds    int               `json:"timeout_seconds"`
	ErrorMessage      string            `json:"error_message"`
	OriginalCreatedAt time.Time         `json:"original_created_at"`
	DeadLetteredAt    time.Time         `json:"dead_lettered_at"`
}

// CreateTaskRequest carries the parameters needed to enqueue a new task.
type CreateTaskRequest struct {
	Name           string            `json:"name"`
	Type           string            `json:"type"`
	Payload        map[string]string `json:"payload"`
	Priority       int               `json:"priority"`
	MaxRetries     int               `json:"max_retries"`
	TimeoutSeconds int               `json:"timeout_seconds"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
}
