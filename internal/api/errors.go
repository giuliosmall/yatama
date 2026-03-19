package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// APIError represents a structured error response with a machine-readable code.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"-"`
}

func (e *APIError) Error() string { return e.Message }

// Pre-defined API errors.
var (
	ErrInvalidJSONBody = &APIError{Code: "INVALID_JSON_BODY", Message: "invalid JSON body", Status: http.StatusBadRequest}
	ErrNameRequired    = &APIError{Code: "NAME_REQUIRED", Message: "name is required", Status: http.StatusBadRequest}
	ErrTypeRequired    = &APIError{Code: "TYPE_REQUIRED", Message: "type is required", Status: http.StatusBadRequest}
	ErrPayloadNull     = &APIError{Code: "PAYLOAD_NULL", Message: "payload must not be null", Status: http.StatusBadRequest}
	ErrInvalidTaskID   = &APIError{Code: "INVALID_TASK_ID", Message: "invalid task id", Status: http.StatusBadRequest}
	ErrInvalidDLQTaskID = &APIError{Code: "INVALID_DLQ_TASK_ID", Message: "invalid dlq task id", Status: http.StatusBadRequest}
	ErrTaskNotFound    = &APIError{Code: "TASK_NOT_FOUND", Message: "task not found", Status: http.StatusNotFound}
	ErrDLQTaskNotFound = &APIError{Code: "DLQ_TASK_NOT_FOUND", Message: "dlq task not found", Status: http.StatusNotFound}
	ErrInternal        = &APIError{Code: "INTERNAL_ERROR", Message: "internal server error", Status: http.StatusInternalServerError}
)

// UnknownTaskTypeError returns an APIError for an unrecognised task type.
func UnknownTaskTypeError(taskType string) *APIError {
	return &APIError{
		Code:    "UNKNOWN_TASK_TYPE",
		Message: fmt.Sprintf("unknown task type %q", taskType),
		Status:  http.StatusBadRequest,
	}
}

// writeAPIError writes a structured JSON error response.
func writeAPIError(w http.ResponseWriter, apiErr *APIError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(apiErr.Status)
	_ = json.NewEncoder(w).Encode(apiErr) //nolint:errcheck
}
