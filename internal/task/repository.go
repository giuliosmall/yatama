package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrTaskNotFound is returned when a task lookup yields no rows.
var ErrTaskNotFound = errors.New("task not found")

// ErrDLQTaskNotFound is returned when a DLQ task lookup yields no rows.
var ErrDLQTaskNotFound = errors.New("dlq task not found")

// ErrIdempotencyConflict is returned alongside the existing task when a
// CreateTask call detects a duplicate idempotency_key.
var ErrIdempotencyConflict = errors.New("idempotency key conflict")

// Repository defines the persistence operations for tasks and their history.
type Repository interface {
	// CreateTask inserts a new task and its initial "queued" history row
	// within a single transaction. It returns the fully-populated Task.
	CreateTask(ctx context.Context, req CreateTaskRequest) (*Task, error)

	// GetTask retrieves a single task by its primary key.
	// Returns ErrTaskNotFound if the ID does not exist.
	GetTask(ctx context.Context, id uuid.UUID) (*Task, error)

	// GetTaskHistory returns all history rows for a given task,
	// ordered chronologically by occurred_at ascending.
	GetTaskHistory(ctx context.Context, taskID uuid.UUID) ([]TaskHistory, error)

	// ClaimTask atomically selects the highest-priority queued task using
	// SELECT FOR UPDATE SKIP LOCKED, sets its status to "running", inserts
	// a history row, and returns the claimed task. Returns (nil, nil) when
	// no queued tasks are available.
	ClaimTask(ctx context.Context) (*Task, error)

	// UpdateTaskStatus changes a task's status and records the transition
	// in task_history, all within one transaction.
	UpdateTaskStatus(ctx context.Context, id uuid.UUID, status string) error

	// RequeueTask sets a task back to "queued", increments retry_count,
	// and records the transition in task_history, all in one transaction.
	RequeueTask(ctx context.Context, id uuid.UUID) error

	// MoveTaskToDLQ copies a task into the dead_letter_queue table,
	// updates the task status to "dead_lettered", and inserts a history row,
	// all within a single transaction.
	MoveTaskToDLQ(ctx context.Context, id uuid.UUID, errorMessage string) error

	// ListDLQTasks returns all entries from the dead_letter_queue table,
	// ordered by dead_lettered_at descending (most recent first).
	ListDLQTasks(ctx context.Context) ([]DeadLetterTask, error)

	// GetDLQTask retrieves a single dead-letter queue entry by its primary key.
	// Returns ErrDLQTaskNotFound if the ID does not exist.
	GetDLQTask(ctx context.Context, id uuid.UUID) (*DeadLetterTask, error)

	// RetryDLQTask resets the original task to "queued" with retry_count=0,
	// inserts a history row, deletes the DLQ entry, and returns the refreshed task,
	// all within a single transaction.
	RetryDLQTask(ctx context.Context, dlqID uuid.UUID) (*Task, error)

	// StaleTasks finds tasks stuck in "running" beyond their timeout_seconds
	// and either requeues them (if retries remain) or moves them to the DLQ.
	// Returns the count of requeued and dead-lettered tasks.
	ReapStaleTasks(ctx context.Context) (requeued int, deadLettered int, err error)
}

// PostgresRepository implements Repository using pgx/v5 and a connection pool.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository constructs a PostgresRepository backed by the given pool.
func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// ---------------------------------------------------------------------------
// Column list shared across queries to guarantee scan order consistency.
// ---------------------------------------------------------------------------

const taskColumns = `id, name, type, payload, status, priority, retry_count, max_retries, timeout_seconds, idempotency_key, created_at, updated_at, run_after`

// scanTask scans a single row into a Task, assuming columns are in taskColumns order.
func scanTask(row pgx.Row) (*Task, error) {
	var t Task
	var payloadBytes []byte

	err := row.Scan(
		&t.ID,
		&t.Name,
		&t.Type,
		&payloadBytes,
		&t.Status,
		&t.Priority,
		&t.RetryCount,
		&t.MaxRetries,
		&t.TimeoutSeconds,
		&t.IdempotencyKey,
		&t.CreatedAt,
		&t.UpdatedAt,
		&t.RunAfter,
	)
	if err != nil {
		return nil, err
	}

	if payloadBytes != nil {
		if err := json.Unmarshal(payloadBytes, &t.Payload); err != nil {
			return nil, fmt.Errorf("scanTask: unmarshal payload: %w", err)
		}
	}

	return &t, nil
}

// ---------------------------------------------------------------------------
// Repository method implementations
// ---------------------------------------------------------------------------

// CreateTask inserts a new task and a "queued" history row in one transaction.
// If req.IdempotencyKey is non-empty and a task with that key already exists,
// the existing task is returned alongside ErrIdempotencyConflict.
func (r *PostgresRepository) CreateTask(ctx context.Context, req CreateTaskRequest) (*Task, error) {
	// Apply defaults matching the DB schema when the caller omits these fields.
	if req.MaxRetries == 0 {
		req.MaxRetries = 3
	}
	if req.TimeoutSeconds == 0 {
		req.TimeoutSeconds = 30
	}

	payloadBytes, err := json.Marshal(req.Payload)
	if err != nil {
		return nil, fmt.Errorf("CreateTask: marshal payload: %w", err)
	}

	// Determine the idempotency_key SQL value: NULL when empty, TEXT otherwise.
	var idempotencyKey any
	if req.IdempotencyKey != "" {
		idempotencyKey = req.IdempotencyKey
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("CreateTask: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	row := tx.QueryRow(ctx,
		`INSERT INTO tasks (name, type, payload, priority, max_retries, timeout_seconds, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING `+taskColumns,
		req.Name, req.Type, payloadBytes, req.Priority, req.MaxRetries, req.TimeoutSeconds, idempotencyKey,
	)

	t, err := scanTask(row)
	if err != nil {
		// Check for unique violation on idempotency_key (PG error code 23505).
		var pgErr *pgconn.PgError
		if req.IdempotencyKey != "" && errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// The INSERT was rolled back; fetch the existing task by key.
			tx.Rollback(ctx) //nolint:errcheck

			existingRow := r.pool.QueryRow(ctx,
				`SELECT `+taskColumns+` FROM tasks WHERE idempotency_key = $1`,
				req.IdempotencyKey,
			)
			existing, scanErr := scanTask(existingRow)
			if scanErr != nil {
				return nil, fmt.Errorf("CreateTask: fetch existing by idempotency_key: %w", scanErr)
			}
			return existing, ErrIdempotencyConflict
		}
		return nil, fmt.Errorf("CreateTask: scan task: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO task_history (task_id, status) VALUES ($1, $2)`,
		t.ID, t.Status,
	)
	if err != nil {
		return nil, fmt.Errorf("CreateTask: insert history: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("CreateTask: commit: %w", err)
	}

	return t, nil
}

// GetTask fetches a task by ID. Returns ErrTaskNotFound when the row does not exist.
func (r *PostgresRepository) GetTask(ctx context.Context, id uuid.UUID) (*Task, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE id = $1`, id,
	)

	t, err := scanTask(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		return nil, fmt.Errorf("GetTask: %w", err)
	}

	return t, nil
}

// GetTaskHistory returns all history entries for a task, ordered by occurred_at.
func (r *PostgresRepository) GetTaskHistory(ctx context.Context, taskID uuid.UUID) ([]TaskHistory, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, task_id, status, occurred_at
		 FROM task_history
		 WHERE task_id = $1
		 ORDER BY occurred_at ASC`,
		taskID,
	)
	if err != nil {
		return nil, fmt.Errorf("GetTaskHistory: query: %w", err)
	}
	defer rows.Close()

	var history []TaskHistory
	for rows.Next() {
		var h TaskHistory
		if err := rows.Scan(&h.ID, &h.TaskID, &h.Status, &h.OccurredAt); err != nil {
			return nil, fmt.Errorf("GetTaskHistory: scan: %w", err)
		}
		history = append(history, h)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetTaskHistory: rows iteration: %w", err)
	}

	return history, nil
}

// ClaimTask atomically locks the highest-priority queued task, sets it to
// "running", records the transition in history, and returns the claimed task.
// Returns (nil, nil) when the queue is empty or all queued tasks are already
// locked by other workers.
func (r *PostgresRepository) ClaimTask(ctx context.Context) (*Task, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("ClaimTask: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Lock exactly one queued row, skipping any that are already locked.
	row := tx.QueryRow(ctx,
		`SELECT `+taskColumns+`
		 FROM tasks
		 WHERE status = 'queued' AND run_after <= now()
		 ORDER BY priority DESC, created_at ASC
		 LIMIT 1
		 FOR UPDATE SKIP LOCKED`,
	)

	t, err := scanTask(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No queued tasks available — not an error.
			return nil, nil
		}
		return nil, fmt.Errorf("ClaimTask: select: %w", err)
	}

	// Transition the task to running.
	tag, err := tx.Exec(ctx,
		`UPDATE tasks SET status = 'running', updated_at = now() WHERE id = $1`,
		t.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("ClaimTask: update status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("ClaimTask: locked row disappeared")
	}
	t.Status = "running"

	// Record the "running" transition.
	_, err = tx.Exec(ctx,
		`INSERT INTO task_history (task_id, status) VALUES ($1, $2)`,
		t.ID, "running",
	)
	if err != nil {
		return nil, fmt.Errorf("ClaimTask: insert history: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("ClaimTask: commit: %w", err)
	}

	return t, nil
}

// UpdateTaskStatus changes a task's status and inserts a history row,
// all within a single transaction.
func (r *PostgresRepository) UpdateTaskStatus(ctx context.Context, id uuid.UUID, status string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("UpdateTaskStatus: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx,
		`UPDATE tasks SET status = $1, updated_at = now() WHERE id = $2`,
		status, id,
	)
	if err != nil {
		return fmt.Errorf("UpdateTaskStatus: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrTaskNotFound
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO task_history (task_id, status) VALUES ($1, $2)`,
		id, status,
	)
	if err != nil {
		return fmt.Errorf("UpdateTaskStatus: insert history: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("UpdateTaskStatus: commit: %w", err)
	}

	return nil
}

// RequeueTask sets a task back to "queued", increments retry_count, and
// inserts a history row, all within a single transaction.
func (r *PostgresRepository) RequeueTask(ctx context.Context, id uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("RequeueTask: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx,
		`UPDATE tasks
		 SET status = 'queued',
		     retry_count = retry_count + 1,
		     updated_at = now(),
		     run_after = now() + LEAST(make_interval(secs => 5 * power(2, retry_count)), interval '5 minutes')
		 WHERE id = $1`,
		id,
	)
	if err != nil {
		return fmt.Errorf("RequeueTask: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrTaskNotFound
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO task_history (task_id, status) VALUES ($1, $2)`,
		id, "queued",
	)
	if err != nil {
		return fmt.Errorf("RequeueTask: insert history: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("RequeueTask: commit: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Reaper
// ---------------------------------------------------------------------------

// ReapStaleTasks finds tasks stuck in "running" beyond their timeout_seconds,
// then either requeues them (if retries remain) or moves them to the DLQ.
// Each task is processed in its own transaction so a single failure doesn't
// block the rest.
func (r *PostgresRepository) ReapStaleTasks(ctx context.Context) (requeued int, deadLettered int, err error) {
	// Find stuck task IDs. Each task is then processed in its own
	// transaction, so we only need a snapshot of the stuck IDs here.
	rows, err := r.pool.Query(ctx,
		`SELECT id, retry_count, max_retries
		 FROM tasks
		 WHERE status = 'running'
		   AND updated_at + make_interval(secs => timeout_seconds) < now()`,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("ReapStaleTasks: query stuck tasks: %w", err)
	}

	type stuckTask struct {
		id         uuid.UUID
		retryCount int
		maxRetries int
	}
	var stuck []stuckTask
	for rows.Next() {
		var s stuckTask
		if err := rows.Scan(&s.id, &s.retryCount, &s.maxRetries); err != nil {
			rows.Close()
			return 0, 0, fmt.Errorf("ReapStaleTasks: scan: %w", err)
		}
		stuck = append(stuck, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("ReapStaleTasks: rows iteration: %w", err)
	}

	for _, s := range stuck {
		if ctx.Err() != nil {
			return requeued, deadLettered, ctx.Err()
		}

		if s.retryCount < s.maxRetries {
			// Requeue without backoff — immediate re-eligibility.
			if err := r.reapRequeue(ctx, s.id); err != nil {
				return requeued, deadLettered, fmt.Errorf("ReapStaleTasks: requeue %s: %w", s.id, err)
			}
			requeued++
		} else {
			if err := r.MoveTaskToDLQ(ctx, s.id, "reaped: task stuck in running state beyond timeout"); err != nil {
				return requeued, deadLettered, fmt.Errorf("ReapStaleTasks: dlq %s: %w", s.id, err)
			}
			deadLettered++
		}
	}

	return requeued, deadLettered, nil
}

// reapRequeue sets a stuck task back to "queued" with run_after = now()
// (immediate re-eligibility, no backoff), increments retry_count, and
// inserts a history row, all in one transaction.
func (r *PostgresRepository) reapRequeue(ctx context.Context, id uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("reapRequeue: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Only requeue if the task is still running (a worker may have
	// legitimately completed it between the reaper SELECT and now).
	tag, err := tx.Exec(ctx,
		`UPDATE tasks
		 SET status = 'queued',
		     retry_count = retry_count + 1,
		     updated_at = now(),
		     run_after = now()
		 WHERE
		     id = $1
		     AND status = 'running'`, // avoid race condition if the worker already set the task to succeeded or failed, the UPDATE affects zero rows
		id,
	)
	if err != nil {
		return fmt.Errorf("reapRequeue: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Task was already handled by a worker — not an error.
		return nil
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO task_history (task_id, status) VALUES ($1, $2)`,
		id, "queued",
	)
	if err != nil {
		return fmt.Errorf("reapRequeue: insert history: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("reapRequeue: commit: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Dead-letter queue operations
// ---------------------------------------------------------------------------

// MoveTaskToDLQ copies a task into the dead_letter_queue table, updates the
// task status to "dead_lettered", and inserts a history row, all within a
// single transaction.
func (r *PostgresRepository) MoveTaskToDLQ(ctx context.Context, id uuid.UUID, errorMessage string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("MoveTaskToDLQ: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Copy task data into the dead-letter queue.
	_, err = tx.Exec(ctx,
		`INSERT INTO dead_letter_queue (original_task_id, name, type, payload, priority, retry_count, max_retries, timeout_seconds, error_message, original_created_at)
		 SELECT id, name, type, payload, priority, retry_count, max_retries, timeout_seconds, $2, created_at
		 FROM tasks WHERE id = $1`,
		id, errorMessage,
	)
	if err != nil {
		return fmt.Errorf("MoveTaskToDLQ: insert dlq: %w", err)
	}

	// Update the original task's status to "dead_lettered".
	tag, err := tx.Exec(ctx,
		`UPDATE tasks SET status = 'dead_lettered', updated_at = now() WHERE id = $1`,
		id,
	)
	if err != nil {
		return fmt.Errorf("MoveTaskToDLQ: update status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrTaskNotFound
	}

	// Record the transition in task history.
	_, err = tx.Exec(ctx,
		`INSERT INTO task_history (task_id, status) VALUES ($1, $2)`,
		id, "dead_lettered",
	)
	if err != nil {
		return fmt.Errorf("MoveTaskToDLQ: insert history: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("MoveTaskToDLQ: commit: %w", err)
	}

	return nil
}

// ListDLQTasks returns all entries from the dead_letter_queue table,
// ordered by dead_lettered_at descending (most recent first).
func (r *PostgresRepository) ListDLQTasks(ctx context.Context) ([]DeadLetterTask, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, original_task_id, name, type, payload, priority, retry_count, max_retries, timeout_seconds, error_message, original_created_at, dead_lettered_at
		 FROM dead_letter_queue
		 ORDER BY dead_lettered_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("ListDLQTasks: query: %w", err)
	}
	defer rows.Close()

	var tasks []DeadLetterTask
	for rows.Next() {
		var t DeadLetterTask
		var payloadBytes []byte
		if err := rows.Scan(
			&t.ID, &t.OriginalTaskID, &t.Name, &t.Type, &payloadBytes,
			&t.Priority, &t.RetryCount, &t.MaxRetries, &t.TimeoutSeconds,
			&t.ErrorMessage, &t.OriginalCreatedAt, &t.DeadLetteredAt,
		); err != nil {
			return nil, fmt.Errorf("ListDLQTasks: scan: %w", err)
		}
		if payloadBytes != nil {
			if err := json.Unmarshal(payloadBytes, &t.Payload); err != nil {
				return nil, fmt.Errorf("ListDLQTasks: unmarshal payload: %w", err)
			}
		}
		tasks = append(tasks, t)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ListDLQTasks: rows iteration: %w", err)
	}

	return tasks, nil
}

// GetDLQTask retrieves a single dead-letter queue entry by its primary key.
// Returns ErrDLQTaskNotFound if the ID does not exist.
func (r *PostgresRepository) GetDLQTask(ctx context.Context, id uuid.UUID) (*DeadLetterTask, error) {
	var t DeadLetterTask
	var payloadBytes []byte

	err := r.pool.QueryRow(ctx,
		`SELECT id, original_task_id, name, type, payload, priority, retry_count, max_retries, timeout_seconds, error_message, original_created_at, dead_lettered_at
		 FROM dead_letter_queue
		 WHERE id = $1`,
		id,
	).Scan(
		&t.ID, &t.OriginalTaskID, &t.Name, &t.Type, &payloadBytes,
		&t.Priority, &t.RetryCount, &t.MaxRetries, &t.TimeoutSeconds,
		&t.ErrorMessage, &t.OriginalCreatedAt, &t.DeadLetteredAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrDLQTaskNotFound
		}
		return nil, fmt.Errorf("GetDLQTask: %w", err)
	}

	if payloadBytes != nil {
		if err := json.Unmarshal(payloadBytes, &t.Payload); err != nil {
			return nil, fmt.Errorf("GetDLQTask: unmarshal payload: %w", err)
		}
	}

	return &t, nil
}

// RetryDLQTask resets the original task to "queued" with retry_count=0,
// inserts a history row, deletes the DLQ entry, and returns the refreshed task,
// all within a single transaction.
func (r *PostgresRepository) RetryDLQTask(ctx context.Context, dlqID uuid.UUID) (*Task, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("RetryDLQTask: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Look up the DLQ entry to find the original task ID.
	var originalTaskID uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT original_task_id FROM dead_letter_queue WHERE id = $1`,
		dlqID,
	).Scan(&originalTaskID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrDLQTaskNotFound
		}
		return nil, fmt.Errorf("RetryDLQTask: select dlq: %w", err)
	}

	// Reset the original task to "queued" with retry_count = 0.
	tag, err := tx.Exec(ctx,
		`UPDATE tasks SET status = 'queued', retry_count = 0, updated_at = now(), run_after = now() WHERE id = $1`,
		originalTaskID,
	)
	if err != nil {
		return nil, fmt.Errorf("RetryDLQTask: update task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrTaskNotFound
	}

	// Record the transition in task history.
	_, err = tx.Exec(ctx,
		`INSERT INTO task_history (task_id, status) VALUES ($1, $2)`,
		originalTaskID, "queued",
	)
	if err != nil {
		return nil, fmt.Errorf("RetryDLQTask: insert history: %w", err)
	}

	// Delete the DLQ entry.
	_, err = tx.Exec(ctx,
		`DELETE FROM dead_letter_queue WHERE id = $1`,
		dlqID,
	)
	if err != nil {
		return nil, fmt.Errorf("RetryDLQTask: delete dlq: %w", err)
	}

	// Fetch the refreshed task.
	row := tx.QueryRow(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE id = $1`,
		originalTaskID,
	)
	t, err := scanTask(row)
	if err != nil {
		return nil, fmt.Errorf("RetryDLQTask: scan refreshed task: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("RetryDLQTask: commit: %w", err)
	}

	return t, nil
}
