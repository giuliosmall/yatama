package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/giulio/task-manager/internal/queue"
	"github.com/giulio/task-manager/internal/task"
)

// =========================================================================
// Mock repository
// =========================================================================

// mockRepository is a hand-written fake that implements task.Repository.
// It uses canned data keyed by a fixed UUID so tests are deterministic.
type mockRepository struct {
	// knownID is the UUID that GetTask will recognise.
	knownID uuid.UUID
	// createErr, if non-nil, is returned by CreateTask.
	createErr error
	// getErr, if non-nil, is returned by GetTask (overrides knownID logic).
	getErr error
	// DLQ fields
	dlqKnownID     uuid.UUID
	dlqTasks       []task.DeadLetterTask
	dlqGetResult   *task.DeadLetterTask
	dlqRetryResult *task.Task
}

func newMockRepository() *mockRepository {
	return &mockRepository{
		knownID:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		dlqKnownID: uuid.MustParse("33333333-3333-3333-3333-333333333333"),
	}
}

func (m *mockRepository) UpdateTaskStatusOnly(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (m *mockRepository) RequeueTaskOnly(_ context.Context, _ uuid.UUID) error { return nil }
func (m *mockRepository) MoveTaskToDLQOnly(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (m *mockRepository) CreateTaskBatch(_ context.Context, reqs []task.CreateTaskRequest) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(reqs))
	for i := range reqs {
		ids[i] = uuid.New()
	}
	return ids, nil
}

func (m *mockRepository) CreateTask(_ context.Context, req task.CreateTaskRequest) (*task.Task, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}
	return &task.Task{
		ID:             m.knownID,
		Name:           req.Name,
		Type:           req.Type,
		Payload:        req.Payload,
		Status:         "queued",
		Priority:       req.Priority,
		MaxRetries:     req.MaxRetries,
		TimeoutSeconds: req.TimeoutSeconds,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
		RunAfter:       time.Now(),
	}, nil
}

func (m *mockRepository) GetTask(_ context.Context, id uuid.UUID) (*task.Task, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if id != m.knownID {
		return nil, task.ErrTaskNotFound
	}
	return &task.Task{
		ID:             m.knownID,
		Name:           "test-task",
		Type:           "send_email",
		Payload:        map[string]string{"to": "alice@example.com"},
		Status:         "queued",
		Priority:       5,
		MaxRetries:     3,
		TimeoutSeconds: 30,
		CreatedAt:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		RunAfter:       time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}, nil
}

func (m *mockRepository) GetTaskHistory(_ context.Context, taskID uuid.UUID) ([]task.TaskHistory, error) {
	if taskID != m.knownID {
		return nil, nil
	}
	return []task.TaskHistory{
		{
			ID:         uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			TaskID:     m.knownID,
			Status:     "queued",
			OccurredAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}, nil
}

func (m *mockRepository) ClaimTask(_ context.Context) (*task.Task, error) {
	return nil, nil // not exercised by handler tests
}

func (m *mockRepository) UpdateTaskStatus(_ context.Context, _ uuid.UUID, _ string) error {
	return nil // not exercised by handler tests
}

func (m *mockRepository) RequeueTask(_ context.Context, _ uuid.UUID) error {
	return nil
}

func (m *mockRepository) MoveTaskToDLQ(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (m *mockRepository) ListDLQTasks(_ context.Context) ([]task.DeadLetterTask, error) {
	return m.dlqTasks, nil
}

func (m *mockRepository) GetDLQTask(_ context.Context, id uuid.UUID) (*task.DeadLetterTask, error) {
	if m.dlqGetResult != nil && m.dlqGetResult.ID == id {
		return m.dlqGetResult, nil
	}
	return nil, task.ErrDLQTaskNotFound
}

func (m *mockRepository) RetryDLQTask(_ context.Context, id uuid.UUID) (*task.Task, error) {
	if m.dlqRetryResult != nil && id == m.dlqKnownID {
		return m.dlqRetryResult, nil
	}
	return nil, task.ErrDLQTaskNotFound
}

func (m *mockRepository) ReapStaleTasks(_ context.Context) (int, int, error) {
	return 0, 0, nil // not exercised by handler tests
}

// =========================================================================
// Helper: assert structured API error
// =========================================================================

func assertAPIError(t *testing.T, rec *httptest.ResponseRecorder, wantCode, wantMessage string) {
	t.Helper()
	var resp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp["code"] != wantCode {
		t.Fatalf("expected code %q, got %q", wantCode, resp["code"])
	}
	if resp["message"] != wantMessage {
		t.Fatalf("expected message %q, got %q", wantMessage, resp["message"])
	}
}

// =========================================================================
// Helper: build chi route context for URL params
// =========================================================================

// withChiURLParam adds a chi URL parameter to the request context so that
// chi.URLParam(r, key) returns the given value inside the handler.
func withChiURLParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, value)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

// =========================================================================
// HealthCheck
// =========================================================================

func TestHealthCheck_Returns200(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	h.HealthCheck(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", ct)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}
}

// =========================================================================
// CreateTask
// =========================================================================

func TestCreateTask_ValidInput_Returns201(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	registry := task.NewRegistry()
	h := NewHandler(mock, registry, queue.NewPostgresProducer(), NewDrainState())

	payload := map[string]any{
		"name":            "send welcome email",
		"type":            "send_email",
		"payload":         map[string]string{"to": "bob@example.com", "subject": "Welcome"},
		"priority":        10,
		"max_retries":     5,
		"timeout_seconds": 60,
	}
	body, _ := json.Marshal(payload)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	h.CreateTask(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["id"] != mock.knownID.String() {
		t.Fatalf("expected id=%s, got %q", mock.knownID, resp["id"])
	}
}

func TestCreateTask_MissingName_Returns400(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	payload := map[string]any{
		"type":    "send_email",
		"payload": map[string]string{"to": "bob@example.com"},
	}
	body, _ := json.Marshal(payload)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))

	h.CreateTask(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	assertAPIError(t, rec, "NAME_REQUIRED", "name is required")
}

func TestCreateTask_MissingType_Returns400(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	payload := map[string]any{
		"name":    "my task",
		"payload": map[string]string{"key": "val"},
	}
	body, _ := json.Marshal(payload)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))

	h.CreateTask(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	assertAPIError(t, rec, "TYPE_REQUIRED", "type is required")
}

func TestCreateTask_UnknownType_Returns400(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	payload := map[string]any{
		"name":    "my task",
		"type":    "nonexistent_type",
		"payload": map[string]string{},
	}
	body, _ := json.Marshal(payload)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))

	h.CreateTask(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	assertAPIError(t, rec, "UNKNOWN_TASK_TYPE", `unknown task type "nonexistent_type"`)
}

func TestCreateTask_NullPayload_Returns400(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	// JSON with payload explicitly null.
	body := []byte(`{"name":"task","type":"send_email","payload":null}`)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))

	h.CreateTask(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	assertAPIError(t, rec, "PAYLOAD_NULL", "payload must not be null")
}

func TestCreateTask_MalformedJSON_Returns400(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader([]byte(`{not json`)))

	h.CreateTask(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	assertAPIError(t, rec, "INVALID_JSON_BODY", "invalid JSON body")
}

// =========================================================================
// GetTask
// =========================================================================

func TestGetTask_ExistingTask_Returns200(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	h := NewHandler(mock, task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+mock.knownID.String(), nil)
	req = withChiURLParam(req, "id", mock.knownID.String())

	h.GetTask(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", ct)
	}

	var body task.Task
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body.ID != mock.knownID {
		t.Fatalf("expected ID=%s, got %s", mock.knownID, body.ID)
	}
	if body.Name != "test-task" {
		t.Fatalf("expected Name=test-task, got %q", body.Name)
	}
	if body.Status != "queued" {
		t.Fatalf("expected Status=queued, got %q", body.Status)
	}
}

func TestGetTask_NonExistentTask_Returns404(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())
	unknownID := uuid.MustParse("99999999-9999-9999-9999-999999999999")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+unknownID.String(), nil)
	req = withChiURLParam(req, "id", unknownID.String())

	h.GetTask(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d; body: %s", rec.Code, rec.Body.String())
	}

	assertAPIError(t, rec, "TASK_NOT_FOUND", "task not found")
}

func TestGetTask_InvalidUUID_Returns400(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tasks/not-a-uuid", nil)
	req = withChiURLParam(req, "id", "not-a-uuid")

	h.GetTask(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	assertAPIError(t, rec, "INVALID_TASK_ID", "invalid task id")
}

// =========================================================================
// GetTaskHistory
// =========================================================================

func TestGetTaskHistory_ExistingTask_Returns200(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	h := NewHandler(mock, task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+mock.knownID.String()+"/history", nil)
	req = withChiURLParam(req, "id", mock.knownID.String())

	h.GetTaskHistory(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var history []task.TaskHistory
	if err := json.NewDecoder(rec.Body).Decode(&history); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(history))
	}
	if history[0].Status != "queued" {
		t.Fatalf("expected first history status=queued, got %q", history[0].Status)
	}
	if history[0].TaskID != mock.knownID {
		t.Fatalf("expected history task_id=%s, got %s", mock.knownID, history[0].TaskID)
	}
}

func TestGetTaskHistory_NonExistentTask_Returns404(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())
	unknownID := uuid.MustParse("99999999-9999-9999-9999-999999999999")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+unknownID.String()+"/history", nil)
	req = withChiURLParam(req, "id", unknownID.String())

	h.GetTaskHistory(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d; body: %s", rec.Code, rec.Body.String())
	}

	assertAPIError(t, rec, "TASK_NOT_FOUND", "task not found")
}

func TestGetTaskHistory_InvalidUUID_Returns400(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tasks/bad-id/history", nil)
	req = withChiURLParam(req, "id", "bad-id")

	h.GetTaskHistory(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

// =========================================================================
// Full create-then-get flow
// =========================================================================

func TestCreateAndGetTask_FullFlow(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	registry := task.NewRegistry()
	h := NewHandler(mock, registry, queue.NewPostgresProducer(), NewDrainState())

	// Step 1: Create a task.
	createPayload := map[string]any{
		"name":            "welcome-email",
		"type":            "send_email",
		"payload":         map[string]string{"to": "carol@example.com"},
		"priority":        3,
		"max_retries":     2,
		"timeout_seconds": 45,
	}
	body, _ := json.Marshal(createPayload)

	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")

	h.CreateTask(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d; body: %s", createRec.Code, createRec.Body.String())
	}

	var createResp map[string]string
	_ = json.NewDecoder(createRec.Body).Decode(&createResp)
	taskID := createResp["id"]

	// Step 2: Retrieve the task by ID.
	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/tasks/"+taskID, nil)
	getReq = withChiURLParam(getReq, "id", taskID)

	h.GetTask(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("get: expected 200, got %d; body: %s", getRec.Code, getRec.Body.String())
	}

	var taskResp task.Task
	_ = json.NewDecoder(getRec.Body).Decode(&taskResp)

	if taskResp.ID.String() != taskID {
		t.Fatalf("get: expected ID=%s, got %s", taskID, taskResp.ID)
	}
}

// =========================================================================
// ListDLQTasks
// =========================================================================

func TestListDLQTasks_Empty_Returns200WithEmptyArray(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	h := NewHandler(mock, task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dlq/tasks", nil)

	h.ListDLQTasks(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body []task.DeadLetterTask
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("expected empty array, got %d items", len(body))
	}
}

func TestListDLQTasks_WithEntries_Returns200(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	mock.dlqTasks = []task.DeadLetterTask{
		{
			ID:             mock.dlqKnownID,
			OriginalTaskID: mock.knownID,
			Name:           "dead-task",
			Type:           "send_email",
			Payload:        map[string]string{"to": "fail@example.com"},
			ErrorMessage:   "connection refused",
			RetryCount:     3,
			MaxRetries:     3,
		},
	}
	h := NewHandler(mock, task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dlq/tasks", nil)

	h.ListDLQTasks(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body []task.DeadLetterTask
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("expected 1 item, got %d", len(body))
	}
	if body[0].ErrorMessage != "connection refused" {
		t.Fatalf("expected error_message='connection refused', got %q", body[0].ErrorMessage)
	}
}

// =========================================================================
// GetDLQTask
// =========================================================================

func TestGetDLQTask_Existing_Returns200(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	mock.dlqGetResult = &task.DeadLetterTask{
		ID:             mock.dlqKnownID,
		OriginalTaskID: mock.knownID,
		Name:           "dead-task",
		Type:           "send_email",
		ErrorMessage:   "timeout exceeded",
	}
	h := NewHandler(mock, task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dlq/tasks/"+mock.dlqKnownID.String(), nil)
	req = withChiURLParam(req, "id", mock.dlqKnownID.String())

	h.GetDLQTask(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var body task.DeadLetterTask
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body.ID != mock.dlqKnownID {
		t.Fatalf("expected ID=%s, got %s", mock.dlqKnownID, body.ID)
	}
}

func TestGetDLQTask_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	h := NewHandler(mock, task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())
	unknownID := uuid.MustParse("99999999-9999-9999-9999-999999999999")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dlq/tasks/"+unknownID.String(), nil)
	req = withChiURLParam(req, "id", unknownID.String())

	h.GetDLQTask(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}

	assertAPIError(t, rec, "DLQ_TASK_NOT_FOUND", "dlq task not found")
}

func TestGetDLQTask_InvalidUUID_Returns400(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dlq/tasks/not-a-uuid", nil)
	req = withChiURLParam(req, "id", "not-a-uuid")

	h.GetDLQTask(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	assertAPIError(t, rec, "INVALID_DLQ_TASK_ID", "invalid dlq task id")
}

// =========================================================================
// RetryDLQTask
// =========================================================================

func TestRetryDLQTask_Success_Returns200(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	mock.dlqRetryResult = &task.Task{
		ID:     mock.knownID,
		Name:   "retried-task",
		Type:   "send_email",
		Status: "queued",
	}
	h := NewHandler(mock, task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/dlq/tasks/"+mock.dlqKnownID.String()+"/retry", nil)
	req = withChiURLParam(req, "id", mock.dlqKnownID.String())

	h.RetryDLQTask(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["id"] != mock.knownID.String() {
		t.Fatalf("expected id=%s, got %q", mock.knownID, resp["id"])
	}
	if resp["status"] != "queued" {
		t.Fatalf("expected status=queued, got %q", resp["status"])
	}
}

func TestRetryDLQTask_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	h := NewHandler(mock, task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())
	unknownID := uuid.MustParse("99999999-9999-9999-9999-999999999999")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/dlq/tasks/"+unknownID.String()+"/retry", nil)
	req = withChiURLParam(req, "id", unknownID.String())

	h.RetryDLQTask(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}

	assertAPIError(t, rec, "DLQ_TASK_NOT_FOUND", "dlq task not found")
}

func TestRetryDLQTask_InvalidUUID_Returns400(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/dlq/tasks/bad-id/retry", nil)
	req = withChiURLParam(req, "id", "bad-id")

	h.RetryDLQTask(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

// =========================================================================
// Router integration (via chi.Router)
// =========================================================================

func TestRouter_EndToEnd(t *testing.T) {
	t.Parallel()

	mock := newMockRepository()
	registry := task.NewRegistry()
	h := NewHandler(mock, registry, queue.NewPostgresProducer(), NewDrainState())
	router := NewRouter(h)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{
			name:       "GET /health returns 200",
			method:     http.MethodGet,
			path:       "/health",
			wantStatus: http.StatusOK,
		},
		{
			name:       "POST /tasks with valid body returns 201",
			method:     http.MethodPost,
			path:       "/tasks",
			body:       `{"name":"test","type":"send_email","payload":{"to":"a@b.com"}}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "GET /tasks/{id} existing returns 200",
			method:     http.MethodGet,
			path:       "/tasks/" + mock.knownID.String(),
			wantStatus: http.StatusOK,
		},
		{
			name:       "GET /tasks/{id}/history existing returns 200",
			method:     http.MethodGet,
			path:       "/tasks/" + mock.knownID.String() + "/history",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GET /tasks/{id} nonexistent returns 404",
			method:     http.MethodGet,
			path:       "/tasks/99999999-9999-9999-9999-999999999999",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GET /dlq/tasks returns 200",
			method:     http.MethodGet,
			path:       "/dlq/tasks",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GET /dlq/tasks/{id} nonexistent returns 404",
			method:     http.MethodGet,
			path:       "/dlq/tasks/99999999-9999-9999-9999-999999999999",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "POST /dlq/tasks/{id}/retry nonexistent returns 404",
			method:     http.MethodPost,
			path:       "/dlq/tasks/99999999-9999-9999-9999-999999999999/retry",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GET /ready returns 200",
			method:     http.MethodGet,
			path:       "/ready",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var bodyReader *bytes.Reader
			if tt.body != "" {
				bodyReader = bytes.NewReader([]byte(tt.body))
			} else {
				bodyReader = bytes.NewReader(nil)
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, bodyReader)
			if tt.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}

			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d; body: %s", tt.wantStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

// =========================================================================
// ReadinessCheck
// =========================================================================

func TestReadinessCheck_WhenReady_Returns200(t *testing.T) {
	t.Parallel()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), NewDrainState())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)

	h.ReadinessCheck(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "ready" {
		t.Fatalf("expected status=ready, got %q", body["status"])
	}
}

func TestReadinessCheck_WhenDraining_Returns503(t *testing.T) {
	t.Parallel()

	drain := NewDrainState()
	drain.StartDraining()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), drain)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)

	h.ReadinessCheck(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "draining" {
		t.Fatalf("expected status=draining, got %q", body["status"])
	}
}

func TestHealthCheck_StillReturns200_WhenDraining(t *testing.T) {
	t.Parallel()

	drain := NewDrainState()
	drain.StartDraining()

	h := NewHandler(newMockRepository(), task.NewRegistry(), queue.NewPostgresProducer(), drain)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	h.HealthCheck(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}
}

// =========================================================================
// DrainState
// =========================================================================

func TestDrainState_InitiallyNotDraining(t *testing.T) {
	t.Parallel()

	d := NewDrainState()
	if d.IsDraining() {
		t.Fatal("expected IsDraining to be false initially")
	}
}

func TestDrainState_StartDraining_FlipsFlag(t *testing.T) {
	t.Parallel()

	d := NewDrainState()
	d.StartDraining()
	if !d.IsDraining() {
		t.Fatal("expected IsDraining to be true after StartDraining")
	}
}

func TestDrainState_Idempotent(t *testing.T) {
	t.Parallel()

	d := NewDrainState()
	d.StartDraining()
	d.StartDraining() // calling twice should not panic or change state
	if !d.IsDraining() {
		t.Fatal("expected IsDraining to remain true after second StartDraining")
	}
}
