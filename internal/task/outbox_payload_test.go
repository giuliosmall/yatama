package task

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestOutboxPayload_MarshalRoundtrip(t *testing.T) {
	t.Parallel()

	original := OutboxPayload{
		TaskID:         uuid.New(),
		Name:           "weekly-report",
		Type:           "run_query",
		Payload:        map[string]string{"sql": "SELECT 1", "db": "analytics"},
		Priority:       7,
		MaxRetries:     3,
		TimeoutSeconds: 120,
		IdempotencyKey: "idem-key-42",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: unexpected error: %v", err)
	}

	var got OutboxPayload
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: unexpected error: %v", err)
	}

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
