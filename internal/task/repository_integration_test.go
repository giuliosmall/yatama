//go:build integration

package task_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/giulio/task-manager/internal/task"
)

// --------------------------------------------------------------------------
// Test container setup
// --------------------------------------------------------------------------

const (
	pgUser     = "testuser"
	pgPassword = "testpass"
	pgDatabase = "taskmanager_test"
)

// setupPostgres starts a PostgreSQL container, applies the migration, and
// returns a connected pgxpool.Pool. The container is terminated when the
// test (and its subtests) finish.
func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(pgDatabase),
		postgres.WithUsername(pgUser),
		postgres.WithPassword(pgPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("warning: failed to terminate postgres container: %v", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	t.Logf("postgres container connection string: %s", connStr)

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create pgxpool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	// Apply migration.
	migration, err := os.ReadFile("../../migrations/001_create_tables.up.sql")
	if err != nil {
		t.Fatalf("failed to read migration file: %v", err)
	}

	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("failed to apply migration: %v", err)
	}

	// Apply DLQ migration.
	migration002, err := os.ReadFile("../../migrations/002_create_dead_letter_queue.up.sql")
	if err != nil {
		t.Fatalf("failed to read migration 002 file: %v", err)
	}

	if _, err := pool.Exec(ctx, string(migration002)); err != nil {
		t.Fatalf("failed to apply migration 002: %v", err)
	}

	// Apply run_after migration.
	migration003, err := os.ReadFile("../../migrations/003_add_run_after.up.sql")
	if err != nil {
		t.Fatalf("failed to read migration 003 file: %v", err)
	}

	if _, err := pool.Exec(ctx, string(migration003)); err != nil {
		t.Fatalf("failed to apply migration 003: %v", err)
	}

	// Apply reaper + idempotency migration.
	migration004, err := os.ReadFile("../../migrations/004_add_reaper_and_idempotency.up.sql")
	if err != nil {
		t.Fatalf("failed to read migration 004 file: %v", err)
	}

	if _, err := pool.Exec(ctx, string(migration004)); err != nil {
		t.Fatalf("failed to apply migration 004: %v", err)
	}

	return pool
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

func TestPostgresRepository_CreateTask(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	req := task.CreateTaskRequest{
		Name:           "integration-test-email",
		Type:           "send_email",
		Payload:        map[string]string{"to": "integration@example.com", "subject": "Hello"},
		Priority:       5,
		MaxRetries:     3,
		TimeoutSeconds: 60,
	}

	created, err := repo.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	if created.ID == uuid.Nil {
		t.Fatal("expected non-nil task ID")
	}
	if created.Name != req.Name {
		t.Fatalf("expected Name=%q, got %q", req.Name, created.Name)
	}
	if created.Type != req.Type {
		t.Fatalf("expected Type=%q, got %q", req.Type, created.Type)
	}
	if created.Status != "queued" {
		t.Fatalf("expected Status=queued, got %q", created.Status)
	}
	if created.Priority != req.Priority {
		t.Fatalf("expected Priority=%d, got %d", req.Priority, created.Priority)
	}
	if created.MaxRetries != req.MaxRetries {
		t.Fatalf("expected MaxRetries=%d, got %d", req.MaxRetries, created.MaxRetries)
	}
	if created.TimeoutSeconds != req.TimeoutSeconds {
		t.Fatalf("expected TimeoutSeconds=%d, got %d", req.TimeoutSeconds, created.TimeoutSeconds)
	}
	if created.Payload["to"] != "integration@example.com" {
		t.Fatalf("expected Payload[to]=integration@example.com, got %q", created.Payload["to"])
	}
	if created.CreatedAt.IsZero() {
		t.Fatal("expected non-zero CreatedAt")
	}
	if created.RunAfter.IsZero() {
		t.Fatal("expected non-zero RunAfter")
	}
	if time.Since(created.RunAfter) > 5*time.Second {
		t.Fatalf("expected RunAfter close to now, got %v", created.RunAfter)
	}
}

func TestPostgresRepository_GetTask(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	// Create a task first.
	req := task.CreateTaskRequest{
		Name:           "get-task-test",
		Type:           "run_query",
		Payload:        map[string]string{"query": "SELECT 1"},
		Priority:       1,
		MaxRetries:     0,
		TimeoutSeconds: 10,
	}

	created, err := repo.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	// Retrieve by ID.
	got, err := repo.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}

	if got.ID != created.ID {
		t.Fatalf("expected ID=%s, got %s", created.ID, got.ID)
	}
	if got.Name != created.Name {
		t.Fatalf("expected Name=%q, got %q", created.Name, got.Name)
	}
	if got.Status != "queued" {
		t.Fatalf("expected Status=queued, got %q", got.Status)
	}
}

func TestPostgresRepository_GetTask_NotFound(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	nonexistent := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	_, err := repo.GetTask(ctx, nonexistent)
	if err == nil {
		t.Fatal("expected ErrTaskNotFound, got nil")
	}
	if err != task.ErrTaskNotFound {
		t.Fatalf("expected ErrTaskNotFound, got: %v", err)
	}
}

func TestPostgresRepository_GetTaskHistory_AfterCreation(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	req := task.CreateTaskRequest{
		Name:           "history-test",
		Type:           "send_email",
		Payload:        map[string]string{"to": "history@example.com"},
		Priority:       0,
		MaxRetries:     1,
		TimeoutSeconds: 30,
	}

	created, err := repo.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	history, err := repo.GetTaskHistory(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTaskHistory failed: %v", err)
	}

	if len(history) != 1 {
		t.Fatalf("expected 1 history entry after creation, got %d", len(history))
	}
	if history[0].Status != "queued" {
		t.Fatalf("expected first history status=queued, got %q", history[0].Status)
	}
	if history[0].TaskID != created.ID {
		t.Fatalf("expected history TaskID=%s, got %s", created.ID, history[0].TaskID)
	}
	if history[0].OccurredAt.IsZero() {
		t.Fatal("expected non-zero OccurredAt")
	}
}

func TestPostgresRepository_ClaimTask(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	// Create a queued task.
	req := task.CreateTaskRequest{
		Name:           "claim-test",
		Type:           "run_query",
		Payload:        map[string]string{"query": "SELECT 42"},
		Priority:       10,
		MaxRetries:     2,
		TimeoutSeconds: 15,
	}

	created, err := repo.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	// Claim the task.
	claimed, err := repo.ClaimTask(ctx)
	if err != nil {
		t.Fatalf("ClaimTask failed: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected a claimed task, got nil")
	}
	if claimed.ID != created.ID {
		t.Fatalf("expected claimed ID=%s, got %s", created.ID, claimed.ID)
	}
	if claimed.Status != "running" {
		t.Fatalf("expected claimed Status=running, got %q", claimed.Status)
	}

	// Verify the task in the database is now running.
	got, err := repo.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTask after claim failed: %v", err)
	}
	if got.Status != "running" {
		t.Fatalf("expected persisted Status=running, got %q", got.Status)
	}

	// Verify a "running" history entry was added.
	history, err := repo.GetTaskHistory(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTaskHistory after claim failed: %v", err)
	}
	if len(history) < 2 {
		t.Fatalf("expected at least 2 history entries, got %d", len(history))
	}

	foundRunning := false
	for _, h := range history {
		if h.Status == "running" {
			foundRunning = true
			break
		}
	}
	if !foundRunning {
		t.Fatal("expected a 'running' history entry after ClaimTask")
	}
}

func TestPostgresRepository_ClaimTask_NoQueuedTasks(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	// No tasks in the database: ClaimTask should return (nil, nil).
	claimed, err := repo.ClaimTask(ctx)
	if err != nil {
		t.Fatalf("ClaimTask failed: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected nil when no tasks are queued, got task %s", claimed.ID)
	}
}

func TestPostgresRepository_ClaimTask_PriorityOrdering(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	// Create a low-priority task first, then a high-priority one.
	lowPriReq := task.CreateTaskRequest{
		Name:           "low-priority",
		Type:           "send_email",
		Payload:        map[string]string{},
		Priority:       1,
		MaxRetries:     0,
		TimeoutSeconds: 10,
	}
	_, err := repo.CreateTask(ctx, lowPriReq)
	if err != nil {
		t.Fatalf("CreateTask (low) failed: %v", err)
	}

	highPriReq := task.CreateTaskRequest{
		Name:           "high-priority",
		Type:           "send_email",
		Payload:        map[string]string{},
		Priority:       100,
		MaxRetries:     0,
		TimeoutSeconds: 10,
	}
	highPriTask, err := repo.CreateTask(ctx, highPriReq)
	if err != nil {
		t.Fatalf("CreateTask (high) failed: %v", err)
	}

	// ClaimTask should pick the higher-priority task.
	claimed, err := repo.ClaimTask(ctx)
	if err != nil {
		t.Fatalf("ClaimTask failed: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected a claimed task, got nil")
	}
	if claimed.ID != highPriTask.ID {
		t.Fatalf("expected high-priority task (ID=%s), got %s", highPriTask.ID, claimed.ID)
	}
}

func TestPostgresRepository_UpdateTaskStatus(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	// Create and claim a task so its status is "running".
	req := task.CreateTaskRequest{
		Name:           "update-status-test",
		Type:           "send_email",
		Payload:        map[string]string{"to": "update@example.com"},
		Priority:       0,
		MaxRetries:     0,
		TimeoutSeconds: 10,
	}

	created, err := repo.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	// Claim it (queued -> running).
	_, err = repo.ClaimTask(ctx)
	if err != nil {
		t.Fatalf("ClaimTask failed: %v", err)
	}

	// Update from running -> succeeded.
	if err := repo.UpdateTaskStatus(ctx, created.ID, "succeeded"); err != nil {
		t.Fatalf("UpdateTaskStatus failed: %v", err)
	}

	// Verify the status changed.
	got, err := repo.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTask after UpdateTaskStatus failed: %v", err)
	}
	if got.Status != "succeeded" {
		t.Fatalf("expected Status=succeeded, got %q", got.Status)
	}

	// Verify the full history: queued -> running -> succeeded.
	history, err := repo.GetTaskHistory(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTaskHistory failed: %v", err)
	}

	expectedStatuses := []string{"queued", "running", "succeeded"}
	if len(history) != len(expectedStatuses) {
		t.Fatalf("expected %d history entries, got %d", len(expectedStatuses), len(history))
	}
	for i, want := range expectedStatuses {
		if history[i].Status != want {
			t.Fatalf("history[%d]: expected status=%q, got %q", i, want, history[i].Status)
		}
	}
}

func TestPostgresRepository_UpdateTaskStatus_NotFound(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	nonexistent := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	err := repo.UpdateTaskStatus(ctx, nonexistent, "failed")
	if err == nil {
		t.Fatal("expected ErrTaskNotFound, got nil")
	}
	if err != task.ErrTaskNotFound {
		t.Fatalf("expected ErrTaskNotFound, got: %v", err)
	}
}

// --------------------------------------------------------------------------
// Exponential Backoff Tests
// --------------------------------------------------------------------------

func TestPostgresRepository_RequeueTask_ExponentialBackoff(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	// Create a task.
	req := task.CreateTaskRequest{
		Name:           "backoff-test",
		Type:           "send_email",
		Payload:        map[string]string{"to": "backoff@example.com"},
		Priority:       5,
		MaxRetries:     3,
		TimeoutSeconds: 30,
	}

	created, err := repo.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	// Claim it (queued -> running).
	_, err = repo.ClaimTask(ctx)
	if err != nil {
		t.Fatalf("ClaimTask failed: %v", err)
	}

	// Requeue it (running -> queued with backoff).
	if err := repo.RequeueTask(ctx, created.ID); err != nil {
		t.Fatalf("RequeueTask failed: %v", err)
	}

	// Fetch the task and verify RunAfter is roughly 5 seconds in the future.
	got, err := repo.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTask after RequeueTask failed: %v", err)
	}

	delay := time.Until(got.RunAfter)
	if delay < 3*time.Second || delay > 8*time.Second {
		t.Fatalf("expected RunAfter ~5s in the future, got delay=%v (RunAfter=%v)", delay, got.RunAfter)
	}

	// ClaimTask should return nil because the task is not yet eligible.
	claimed, err := repo.ClaimTask(ctx)
	if err != nil {
		t.Fatalf("ClaimTask failed: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected nil from ClaimTask (task not yet eligible), got task %s", claimed.ID)
	}
}

func TestPostgresRepository_RequeueTask_BackoffCap(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	// Create a task.
	req := task.CreateTaskRequest{
		Name:           "backoff-cap-test",
		Type:           "send_email",
		Payload:        map[string]string{"to": "cap@example.com"},
		Priority:       5,
		MaxRetries:     20,
		TimeoutSeconds: 30,
	}

	created, err := repo.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	// Claim it (queued -> running).
	_, err = repo.ClaimTask(ctx)
	if err != nil {
		t.Fatalf("ClaimTask failed: %v", err)
	}

	// Set retry_count = 10 via raw SQL to simulate many retries.
	// Without the cap, backoff would be 5 * 2^10 = 5120 seconds.
	_, err = pool.Exec(ctx, `UPDATE tasks SET retry_count = 10 WHERE id = $1`, created.ID)
	if err != nil {
		t.Fatalf("failed to set retry_count: %v", err)
	}

	// Requeue the task.
	if err := repo.RequeueTask(ctx, created.ID); err != nil {
		t.Fatalf("RequeueTask failed: %v", err)
	}

	// Fetch the task and verify RunAfter is capped at ~5 minutes.
	got, err := repo.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTask after RequeueTask failed: %v", err)
	}

	delay := time.Until(got.RunAfter)
	if delay < 4*time.Minute {
		t.Fatalf("expected RunAfter at least ~4min in the future (capped), got delay=%v", delay)
	}
	if delay > 6*time.Minute {
		t.Fatalf("expected RunAfter at most ~6min in the future (capped at 5min), got delay=%v", delay)
	}
}

func TestPostgresRepository_RetryDLQTask_ImmediateRunAfter(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	// Create a task, claim it, and move it to DLQ.
	created := createAndExhaustTask(t, repo, ctx)
	if err := repo.MoveTaskToDLQ(ctx, created.ID, "immediate retry test"); err != nil {
		t.Fatalf("MoveTaskToDLQ failed: %v", err)
	}

	// List DLQ tasks to get the DLQ ID.
	dlqTasks, err := repo.ListDLQTasks(ctx)
	if err != nil {
		t.Fatalf("ListDLQTasks failed: %v", err)
	}
	if len(dlqTasks) == 0 {
		t.Fatal("expected at least 1 DLQ task")
	}
	dlqID := dlqTasks[0].ID

	// Retry the DLQ task.
	retried, err := repo.RetryDLQTask(ctx, dlqID)
	if err != nil {
		t.Fatalf("RetryDLQTask failed: %v", err)
	}

	// Verify RunAfter is close to now (within 5 seconds).
	if retried.RunAfter.IsZero() {
		t.Fatal("expected non-zero RunAfter on retried task")
	}
	if time.Since(retried.RunAfter) > 5*time.Second {
		t.Fatalf("expected RunAfter close to now, got %v (age=%v)", retried.RunAfter, time.Since(retried.RunAfter))
	}

	// Verify the task is immediately claimable.
	claimed, err := repo.ClaimTask(ctx)
	if err != nil {
		t.Fatalf("ClaimTask failed: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected task to be immediately claimable after DLQ retry, got nil")
	}
	if claimed.ID != created.ID {
		t.Fatalf("expected claimed task ID=%s, got %s", created.ID, claimed.ID)
	}
}

// --------------------------------------------------------------------------
// DLQ Tests
// --------------------------------------------------------------------------

// createAndExhaustTask is a helper that creates a task, claims it, and exhausts
// its retries so it can be moved to the DLQ.
func createAndExhaustTask(t *testing.T, repo *task.PostgresRepository, ctx context.Context) *task.Task {
	t.Helper()

	req := task.CreateTaskRequest{
		Name:           "dlq-test-task",
		Type:           "run_query",
		Payload:        map[string]string{"query": "SELECT 1"},
		Priority:       5,
		MaxRetries:     1,
		TimeoutSeconds: 10,
	}

	created, err := repo.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	// Claim the task (queued -> running).
	_, err = repo.ClaimTask(ctx)
	if err != nil {
		t.Fatalf("ClaimTask failed: %v", err)
	}

	return created
}

func TestPostgresRepository_MoveTaskToDLQ(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	created := createAndExhaustTask(t, repo, ctx)

	// Move to DLQ.
	err := repo.MoveTaskToDLQ(ctx, created.ID, "connection refused")
	if err != nil {
		t.Fatalf("MoveTaskToDLQ failed: %v", err)
	}

	// Verify the task status is now "dead_lettered".
	got, err := repo.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTask after MoveTaskToDLQ failed: %v", err)
	}
	if got.Status != "dead_lettered" {
		t.Fatalf("expected Status=dead_lettered, got %q", got.Status)
	}

	// Verify task history includes "dead_lettered".
	history, err := repo.GetTaskHistory(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTaskHistory failed: %v", err)
	}

	foundDL := false
	for _, h := range history {
		if h.Status == "dead_lettered" {
			foundDL = true
			break
		}
	}
	if !foundDL {
		t.Fatal("expected a 'dead_lettered' history entry")
	}
}

func TestPostgresRepository_ListDLQTasks(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	// Initially empty.
	tasks, err := repo.ListDLQTasks(ctx)
	if err != nil {
		t.Fatalf("ListDLQTasks failed: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected 0 DLQ tasks, got %d", len(tasks))
	}

	// Create and move a task to DLQ.
	created := createAndExhaustTask(t, repo, ctx)
	if err := repo.MoveTaskToDLQ(ctx, created.ID, "test error"); err != nil {
		t.Fatalf("MoveTaskToDLQ failed: %v", err)
	}

	// Should now have 1 entry.
	tasks, err = repo.ListDLQTasks(ctx)
	if err != nil {
		t.Fatalf("ListDLQTasks failed: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 DLQ task, got %d", len(tasks))
	}
	if tasks[0].OriginalTaskID != created.ID {
		t.Fatalf("expected OriginalTaskID=%s, got %s", created.ID, tasks[0].OriginalTaskID)
	}
	if tasks[0].ErrorMessage != "test error" {
		t.Fatalf("expected ErrorMessage='test error', got %q", tasks[0].ErrorMessage)
	}
	if tasks[0].Name != "dlq-test-task" {
		t.Fatalf("expected Name='dlq-test-task', got %q", tasks[0].Name)
	}
}

func TestPostgresRepository_GetDLQTask(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	created := createAndExhaustTask(t, repo, ctx)
	if err := repo.MoveTaskToDLQ(ctx, created.ID, "get test"); err != nil {
		t.Fatalf("MoveTaskToDLQ failed: %v", err)
	}

	// List to get the DLQ ID.
	dlqTasks, err := repo.ListDLQTasks(ctx)
	if err != nil {
		t.Fatalf("ListDLQTasks failed: %v", err)
	}
	if len(dlqTasks) == 0 {
		t.Fatal("expected at least 1 DLQ task")
	}

	dlqID := dlqTasks[0].ID

	// Get by ID.
	got, err := repo.GetDLQTask(ctx, dlqID)
	if err != nil {
		t.Fatalf("GetDLQTask failed: %v", err)
	}
	if got.ID != dlqID {
		t.Fatalf("expected ID=%s, got %s", dlqID, got.ID)
	}
	if got.ErrorMessage != "get test" {
		t.Fatalf("expected ErrorMessage='get test', got %q", got.ErrorMessage)
	}

	// Not found.
	nonexistent := uuid.MustParse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
	_, err = repo.GetDLQTask(ctx, nonexistent)
	if err != task.ErrDLQTaskNotFound {
		t.Fatalf("expected ErrDLQTaskNotFound, got: %v", err)
	}
}

func TestPostgresRepository_RetryDLQTask(t *testing.T) {
	pool := setupPostgres(t)
	repo := task.NewPostgresRepository(pool)
	ctx := context.Background()

	created := createAndExhaustTask(t, repo, ctx)
	if err := repo.MoveTaskToDLQ(ctx, created.ID, "retry test"); err != nil {
		t.Fatalf("MoveTaskToDLQ failed: %v", err)
	}

	// Get the DLQ entry ID.
	dlqTasks, err := repo.ListDLQTasks(ctx)
	if err != nil {
		t.Fatalf("ListDLQTasks failed: %v", err)
	}
	if len(dlqTasks) == 0 {
		t.Fatal("expected at least 1 DLQ task")
	}
	dlqID := dlqTasks[0].ID

	// Retry.
	retried, err := repo.RetryDLQTask(ctx, dlqID)
	if err != nil {
		t.Fatalf("RetryDLQTask failed: %v", err)
	}
	if retried.ID != created.ID {
		t.Fatalf("expected retried task ID=%s, got %s", created.ID, retried.ID)
	}
	if retried.Status != "queued" {
		t.Fatalf("expected Status=queued, got %q", retried.Status)
	}
	if retried.RetryCount != 0 {
		t.Fatalf("expected RetryCount=0, got %d", retried.RetryCount)
	}

	// DLQ entry should be gone.
	_, err = repo.GetDLQTask(ctx, dlqID)
	if err != task.ErrDLQTaskNotFound {
		t.Fatalf("expected ErrDLQTaskNotFound after retry, got: %v", err)
	}

	// Verify task history includes the "queued" re-entry.
	history, err := repo.GetTaskHistory(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTaskHistory failed: %v", err)
	}

	lastStatus := history[len(history)-1].Status
	if lastStatus != "queued" {
		t.Fatalf("expected last history status='queued', got %q", lastStatus)
	}

	// Retry nonexistent DLQ ID should return ErrDLQTaskNotFound.
	nonexistent := uuid.MustParse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
	_, err = repo.RetryDLQTask(ctx, nonexistent)
	if err != task.ErrDLQTaskNotFound {
		t.Fatalf("expected ErrDLQTaskNotFound, got: %v", err)
	}
}
