package queue

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// Serializer marshals and unmarshals queue Messages.
// The default implementation uses JSON; swap to protobuf for higher throughput.
type Serializer interface {
	Marshal(msg Message) ([]byte, error)
	Unmarshal(data []byte) (Message, error)
}

// JSONSerializer is the default Serializer using encoding/json.
type JSONSerializer struct{}

// wireMessage is the JSON-friendly representation of a Message.
// UUIDs are serialized as strings.
type wireMessage struct {
	TaskID         string            `json:"task_id"`
	Name           string            `json:"name"`
	Type           string            `json:"type"`
	Payload        map[string]string `json:"payload"`
	Priority       int               `json:"priority"`
	MaxRetries     int               `json:"max_retries"`
	TimeoutSeconds int               `json:"timeout_seconds"`
	RetryCount     int               `json:"retry_count"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
}

func (JSONSerializer) Marshal(msg Message) ([]byte, error) {
	w := wireMessage{
		TaskID:         msg.TaskID.String(),
		Name:           msg.Name,
		Type:           msg.Type,
		Payload:        msg.Payload,
		Priority:       msg.Priority,
		MaxRetries:     msg.MaxRetries,
		TimeoutSeconds: msg.TimeoutSeconds,
		RetryCount:     msg.RetryCount,
		IdempotencyKey: msg.IdempotencyKey,
	}
	return json.Marshal(w)
}

func (JSONSerializer) Unmarshal(data []byte) (Message, error) {
	var w wireMessage
	if err := json.Unmarshal(data, &w); err != nil {
		return Message{}, fmt.Errorf("unmarshal message: %w", err)
	}

	id, err := uuid.Parse(w.TaskID)
	if err != nil {
		return Message{}, fmt.Errorf("parse task_id %q: %w", w.TaskID, err)
	}

	return Message{
		TaskID:         id,
		Name:           w.Name,
		Type:           w.Type,
		Payload:        w.Payload,
		Priority:       w.Priority,
		MaxRetries:     w.MaxRetries,
		TimeoutSeconds: w.TimeoutSeconds,
		RetryCount:     w.RetryCount,
		IdempotencyKey: w.IdempotencyKey,
	}, nil
}
