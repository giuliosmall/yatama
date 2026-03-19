package queue

import (
	"context"
	"testing"
	"time"

	"github.com/giulio/task-manager/internal/task"
	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// JSONSerializer
// ---------------------------------------------------------------------------

func TestJSONSerializer_RoundTrip(t *testing.T) {
	t.Parallel()

	original := Message{
		TaskID:         uuid.New(),
		Name:           "welcome-email",
		Type:           "send_email",
		Payload:        map[string]string{"to": "alice@example.com", "subject": "Hi"},
		Priority:       10,
		MaxRetries:     5,
		TimeoutSeconds: 120,
		RetryCount:     2,
		IdempotencyKey: "key-abc-123",
		Meta:           map[string]string{"partition": "3"},
	}

	s := JSONSerializer{}

	data, err := s.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: unexpected error: %v", err)
	}

	got, err := s.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: unexpected error: %v", err)
	}

	// Compare all fields except Meta, which is not serialized by wireMessage.
	if got.TaskID != original.TaskID {
		t.Errorf("TaskID: got %v, want %v", got.TaskID, original.TaskID)
	}
	if got.Name != original.Name {
		t.Errorf("Name: got %q, want %q", got.Name, original.Name)
	}
	if got.Type != original.Type {
		t.Errorf("Type: got %q, want %q", got.Type, original.Type)
	}
	if got.Priority != original.Priority {
		t.Errorf("Priority: got %d, want %d", got.Priority, original.Priority)
	}
	if got.MaxRetries != original.MaxRetries {
		t.Errorf("MaxRetries: got %d, want %d", got.MaxRetries, original.MaxRetries)
	}
	if got.TimeoutSeconds != original.TimeoutSeconds {
		t.Errorf("TimeoutSeconds: got %d, want %d", got.TimeoutSeconds, original.TimeoutSeconds)
	}
	if got.RetryCount != original.RetryCount {
		t.Errorf("RetryCount: got %d, want %d", got.RetryCount, original.RetryCount)
	}
	if got.IdempotencyKey != original.IdempotencyKey {
		t.Errorf("IdempotencyKey: got %q, want %q", got.IdempotencyKey, original.IdempotencyKey)
	}
	if len(got.Payload) != len(original.Payload) {
		t.Fatalf("Payload length: got %d, want %d", len(got.Payload), len(original.Payload))
	}
	for k, v := range original.Payload {
		if got.Payload[k] != v {
			t.Errorf("Payload[%q]: got %q, want %q", k, got.Payload[k], v)
		}
	}
}

func TestJSONSerializer_UnmarshalInvalidJSON(t *testing.T) {
	t.Parallel()

	s := JSONSerializer{}
	_, err := s.Unmarshal([]byte(`{not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestJSONSerializer_UnmarshalInvalidUUID(t *testing.T) {
	t.Parallel()

	s := JSONSerializer{}
	_, err := s.Unmarshal([]byte(`{"task_id":"not-a-uuid","name":"x","type":"y"}`))
	if err == nil {
		t.Fatal("expected error for invalid UUID, got nil")
	}
}

// ---------------------------------------------------------------------------
// PostgresProducer
// ---------------------------------------------------------------------------

func TestPostgresProducer_Enqueue_ReturnsNil(t *testing.T) {
	t.Parallel()

	p := NewPostgresProducer()
	err := p.Enqueue(context.Background(), Message{TaskID: uuid.New()})
	if err != nil {
		t.Fatalf("Enqueue: expected nil, got %v", err)
	}
}

func TestPostgresProducer_Close_ReturnsNil(t *testing.T) {
	t.Parallel()

	p := NewPostgresProducer()
	err := p.Close()
	if err != nil {
		t.Fatalf("Close: expected nil, got %v", err)
	}
}

func TestPostgresProducer_ImplementsProducer(t *testing.T) {
	t.Parallel()

	var _ Producer = (*PostgresProducer)(nil)
}

// ---------------------------------------------------------------------------
// PostgresConsumer
// ---------------------------------------------------------------------------

// mockRepo implements task.Repository with a configurable ClaimTask function.
// Only ClaimTask and RequeueTask are used by PostgresConsumer; the rest panic.
type mockRepo struct {
	claimFunc   func(ctx context.Context) (*task.Task, error)
	requeueFunc func(ctx context.Context, id uuid.UUID) error
}

func (m *mockRepo) ClaimTask(ctx context.Context) (*task.Task, error) {
	if m.claimFunc != nil {
		return m.claimFunc(ctx)
	}
	return nil, nil
}

func (m *mockRepo) RequeueTask(ctx context.Context, id uuid.UUID) error {
	if m.requeueFunc != nil {
		return m.requeueFunc(ctx, id)
	}
	return nil
}

// Stubs for the rest of task.Repository — not exercised by consumer tests.
func (m *mockRepo) CreateTask(_ context.Context, _ task.CreateTaskRequest) (*task.Task, error) {
	panic("not implemented")
}
func (m *mockRepo) GetTask(_ context.Context, _ uuid.UUID) (*task.Task, error) {
	panic("not implemented")
}
func (m *mockRepo) GetTaskHistory(_ context.Context, _ uuid.UUID) ([]task.TaskHistory, error) {
	panic("not implemented")
}
func (m *mockRepo) UpdateTaskStatus(_ context.Context, _ uuid.UUID, _ string) error {
	panic("not implemented")
}
func (m *mockRepo) MoveTaskToDLQ(_ context.Context, _ uuid.UUID, _ string) error {
	panic("not implemented")
}
func (m *mockRepo) ListDLQTasks(_ context.Context) ([]task.DeadLetterTask, error) {
	panic("not implemented")
}
func (m *mockRepo) GetDLQTask(_ context.Context, _ uuid.UUID) (*task.DeadLetterTask, error) {
	panic("not implemented")
}
func (m *mockRepo) RetryDLQTask(_ context.Context, _ uuid.UUID) (*task.Task, error) {
	panic("not implemented")
}
func (m *mockRepo) ReapStaleTasks(_ context.Context) (int, int, error) {
	panic("not implemented")
}
func (m *mockRepo) CreateTaskBatch(_ context.Context, _ []task.CreateTaskRequest) ([]uuid.UUID, error) {
	panic("not implemented")
}

func TestPostgresConsumer_Consume_ReturnsChannel(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{}
	c := NewPostgresConsumer(repo, 50*time.Millisecond, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := c.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: unexpected error: %v", err)
	}
	if ch == nil {
		t.Fatal("Consume: expected non-nil channel")
	}

	// Clean up: cancel the context so the goroutine exits.
	cancel()
}

func TestPostgresConsumer_Consume_ContextCancellationClosesChannel(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		claimFunc: func(ctx context.Context) (*task.Task, error) {
			// Return nil (no tasks available) until context is cancelled.
			return nil, nil
		},
	}
	c := NewPostgresConsumer(repo, 10*time.Millisecond, 1)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := c.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: unexpected error: %v", err)
	}

	// Cancel the context; the goroutine should exit and close the channel.
	cancel()

	// Wait for the channel to be closed (with a timeout to avoid hanging).
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed after context cancellation, got a delivery")
		}
		// Channel is closed as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel to close after context cancellation")
	}
}

func TestPostgresConsumer_DefaultPollInterval(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{}
	c := NewPostgresConsumer(repo, 0, 0) // 0s default to 50ms, 0 pollers default to 16
	if c.pollInterval != 50*time.Millisecond {
		t.Errorf("pollInterval: got %v, want 50ms", c.pollInterval)
	}
	if c.pollers != 16 {
		t.Errorf("pollers: got %d, want 16", c.pollers)
	}
}

func TestPostgresConsumer_ImplementsConsumer(t *testing.T) {
	t.Parallel()

	var _ Consumer = (*PostgresConsumer)(nil)
}

// ---------------------------------------------------------------------------
// taskToMessage
// ---------------------------------------------------------------------------

func TestTaskToMessage_AllFields(t *testing.T) {
	t.Parallel()

	idemKey := "my-key-123"
	tk := &task.Task{
		ID:             uuid.New(),
		Name:           "report-gen",
		Type:           "run_query",
		Payload:        map[string]string{"sql": "SELECT 1"},
		Status:         "running",
		Priority:       5,
		RetryCount:     1,
		MaxRetries:     3,
		TimeoutSeconds: 30,
		IdempotencyKey: &idemKey,
	}

	msg := taskToMessage(tk)

	if msg.TaskID != tk.ID {
		t.Errorf("TaskID: got %v, want %v", msg.TaskID, tk.ID)
	}
	if msg.Name != tk.Name {
		t.Errorf("Name: got %q, want %q", msg.Name, tk.Name)
	}
	if msg.Type != tk.Type {
		t.Errorf("Type: got %q, want %q", msg.Type, tk.Type)
	}
	if msg.Priority != tk.Priority {
		t.Errorf("Priority: got %d, want %d", msg.Priority, tk.Priority)
	}
	if msg.MaxRetries != tk.MaxRetries {
		t.Errorf("MaxRetries: got %d, want %d", msg.MaxRetries, tk.MaxRetries)
	}
	if msg.TimeoutSeconds != tk.TimeoutSeconds {
		t.Errorf("TimeoutSeconds: got %d, want %d", msg.TimeoutSeconds, tk.TimeoutSeconds)
	}
	if msg.RetryCount != tk.RetryCount {
		t.Errorf("RetryCount: got %d, want %d", msg.RetryCount, tk.RetryCount)
	}
	if msg.IdempotencyKey != idemKey {
		t.Errorf("IdempotencyKey: got %q, want %q", msg.IdempotencyKey, idemKey)
	}
	if len(msg.Payload) != len(tk.Payload) {
		t.Fatalf("Payload length: got %d, want %d", len(msg.Payload), len(tk.Payload))
	}
	for k, v := range tk.Payload {
		if msg.Payload[k] != v {
			t.Errorf("Payload[%q]: got %q, want %q", k, msg.Payload[k], v)
		}
	}
}

func TestTaskToMessage_NilIdempotencyKey(t *testing.T) {
	t.Parallel()

	tk := &task.Task{
		ID:             uuid.New(),
		Name:           "job",
		Type:           "send_email",
		Payload:        map[string]string{},
		IdempotencyKey: nil, // explicitly nil
	}

	msg := taskToMessage(tk)

	if msg.IdempotencyKey != "" {
		t.Errorf("IdempotencyKey: got %q, want empty string for nil key", msg.IdempotencyKey)
	}
}
