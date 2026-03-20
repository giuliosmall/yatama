package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/giulio/task-manager/internal/queue"
	"github.com/giulio/task-manager/internal/task"
	"github.com/giulio/task-manager/internal/worker"
)

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

var (
	tasksCreatedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tasks_created_total",
			Help: "Total number of tasks created, labeled by task type.",
		},
		[]string{"type"},
	)

	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests, labeled by method, path, and status.",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds, labeled by method and path.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)
)

func init() {
	prometheus.MustRegister(tasksCreatedTotal, httpRequestsTotal, httpRequestDuration)
}

// ---------------------------------------------------------------------------
// DrainState
// ---------------------------------------------------------------------------

// DrainState tracks whether the server is draining (shutting down). It is
// goroutine-safe and used by the readiness probe to signal Kubernetes that
// this pod should stop receiving traffic.
type DrainState struct {
	draining atomic.Bool
}

// NewDrainState creates a DrainState that is initially not draining.
func NewDrainState() *DrainState {
	return &DrainState{}
}

// StartDraining flips the drain flag. Once called, IsDraining returns true.
func (d *DrainState) StartDraining() {
	d.draining.Store(true)
}

// IsDraining reports whether StartDraining has been called.
func (d *DrainState) IsDraining() bool {
	return d.draining.Load()
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

// Handler holds the dependencies for HTTP endpoint handlers.
type Handler struct {
	repo     task.Repository
	registry *task.Registry
	producer queue.Producer
	drain    *DrainState
}

// NewHandler constructs a Handler with the given repository, task registry,
// queue producer, and drain state for readiness probe support.
func NewHandler(repo task.Repository, registry *task.Registry, producer queue.Producer, drain *DrainState) *Handler {
	return &Handler{
		repo:     repo,
		registry: registry,
		producer: producer,
		drain:    drain,
	}
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

// HealthCheck returns a simple liveness probe response.
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ReadinessCheck returns 200 when the server is ready to accept traffic, or
// 503 when it is draining (SIGTERM received, waiting for Kubernetes to
// remove this pod from endpoints).
func (h *Handler) ReadinessCheck(w http.ResponseWriter, r *http.Request) {
	if h.drain.IsDraining() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// CreateTask handles POST /tasks. It validates the request, verifies the task
// type exists in the registry, persists the task via the repository, and
// returns 201 with the new task's ID.
func (h *Handler) CreateTask(w http.ResponseWriter, r *http.Request) {
	var req task.CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, ErrInvalidJSONBody)
		return
	}

	// Validate required fields.
	if req.Name == "" {
		writeAPIError(w, ErrNameRequired)
		return
	}
	if req.Type == "" {
		writeAPIError(w, ErrTypeRequired)
		return
	}
	if _, ok := h.registry.Get(req.Type); !ok {
		writeAPIError(w, UnknownTaskTypeError(req.Type))
		return
	}
	if req.Payload == nil {
		writeAPIError(w, ErrPayloadNull)
		return
	}

	t, err := h.repo.CreateTask(r.Context(), req)
	if err != nil {
		if errors.Is(err, task.ErrIdempotencyConflict) {
			writeJSON(w, http.StatusOK, map[string]string{"id": t.ID.String()})
			return
		}
		slog.Error("failed to create task", "error", err)
		writeAPIError(w, ErrInternal)
		return
	}

	// The outbox publisher handles Kafka dispatch — the outbox row was
	// inserted in the same transaction as the task by repo.CreateTask.

	tasksCreatedTotal.WithLabelValues(req.Type).Inc()

	writeJSON(w, http.StatusCreated, map[string]string{"id": t.ID.String()})
}

// CreateTaskBatch handles POST /tasks/batch. It creates multiple tasks in a
// single transaction for higher throughput. Accepts a JSON array of task
// requests and returns an array of created IDs.
func (h *Handler) CreateTaskBatch(w http.ResponseWriter, r *http.Request) {
	var reqs []task.CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		writeAPIError(w, ErrInvalidJSONBody)
		return
	}

	if len(reqs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "EMPTY_BATCH", "message": "batch must not be empty"})
		return
	}
	if len(reqs) > 1000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BATCH_TOO_LARGE", "message": "batch size must not exceed 1000"})
		return
	}

	// Validate each request.
	for i, req := range reqs {
		if req.Name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"code": "NAME_REQUIRED", "message": fmt.Sprintf("task[%d]: name is required", i)})
			return
		}
		if req.Type == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"code": "TYPE_REQUIRED", "message": fmt.Sprintf("task[%d]: type is required", i)})
			return
		}
		if _, ok := h.registry.Get(req.Type); !ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"code": "UNKNOWN_TASK_TYPE", "message": fmt.Sprintf("task[%d]: unknown task type %q", i, req.Type)})
			return
		}
		if req.Payload == nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"code": "PAYLOAD_NULL", "message": fmt.Sprintf("task[%d]: payload must not be null", i)})
			return
		}
	}

	ids, err := h.repo.CreateTaskBatch(r.Context(), reqs)
	if err != nil {
		slog.Error("failed to create task batch", "error", err, "count", len(reqs))
		writeAPIError(w, ErrInternal)
		return
	}

	// The outbox publisher handles Kafka dispatch — outbox rows were
	// inserted in the same transaction by repo.CreateTaskBatch.

	tasksCreatedTotal.WithLabelValues("batch").Add(float64(len(ids)))

	idStrings := make([]string, len(ids))
	for i, id := range ids {
		idStrings[i] = id.String()
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ids": idStrings, "count": len(ids)})
}

// GetTask handles GET /tasks/{id}. It returns the task as JSON, or 404 if the
// task does not exist.
func (h *Handler) GetTask(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeAPIError(w, ErrInvalidTaskID)
		return
	}

	t, err := h.repo.GetTask(r.Context(), id)
	if err != nil {
		if errors.Is(err, task.ErrTaskNotFound) {
			writeAPIError(w, ErrTaskNotFound)
			return
		}
		slog.Error("failed to get task", "error", err, "task_id", id)
		writeAPIError(w, ErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, t)
}

// GetTaskHistory handles GET /tasks/{id}/history. It returns the task's
// history entries as a JSON array, or 404 if the task does not exist.
func (h *Handler) GetTaskHistory(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeAPIError(w, ErrInvalidTaskID)
		return
	}

	// Verify the task exists before fetching history.
	if _, err := h.repo.GetTask(r.Context(), id); err != nil {
		if errors.Is(err, task.ErrTaskNotFound) {
			writeAPIError(w, ErrTaskNotFound)
			return
		}
		slog.Error("failed to get task for history lookup", "error", err, "task_id", id)
		writeAPIError(w, ErrInternal)
		return
	}

	history, err := h.repo.GetTaskHistory(r.Context(), id)
	if err != nil {
		slog.Error("failed to get task history", "error", err, "task_id", id)
		writeAPIError(w, ErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, history)
}

// ---------------------------------------------------------------------------
// DLQ Endpoints
// ---------------------------------------------------------------------------

// ListDLQTasks handles GET /dlq/tasks. It returns all dead-letter queue
// entries as a JSON array.
func (h *Handler) ListDLQTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := h.repo.ListDLQTasks(r.Context())
	if err != nil {
		slog.Error("failed to list DLQ tasks", "error", err)
		writeAPIError(w, ErrInternal)
		return
	}

	if tasks == nil {
		tasks = []task.DeadLetterTask{}
	}

	writeJSON(w, http.StatusOK, tasks)
}

// GetDLQTask handles GET /dlq/tasks/{id}. It returns a single DLQ entry,
// or 404 if the entry does not exist.
func (h *Handler) GetDLQTask(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeAPIError(w, ErrInvalidDLQTaskID)
		return
	}

	t, err := h.repo.GetDLQTask(r.Context(), id)
	if err != nil {
		if errors.Is(err, task.ErrDLQTaskNotFound) {
			writeAPIError(w, ErrDLQTaskNotFound)
			return
		}
		slog.Error("failed to get DLQ task", "error", err, "dlq_task_id", id)
		writeAPIError(w, ErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, t)
}

// RetryDLQTask handles POST /dlq/tasks/{id}/retry. It moves the task back
// to the main queue and returns the refreshed task's id and status.
func (h *Handler) RetryDLQTask(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeAPIError(w, ErrInvalidDLQTaskID)
		return
	}

	t, err := h.repo.RetryDLQTask(r.Context(), id)
	if err != nil {
		if errors.Is(err, task.ErrDLQTaskNotFound) {
			writeAPIError(w, ErrDLQTaskNotFound)
			return
		}
		slog.Error("failed to retry DLQ task", "error", err, "dlq_task_id", id)
		writeAPIError(w, ErrInternal)
		return
	}

	worker.DLQDepthDec()
	writeJSON(w, http.StatusOK, map[string]string{"id": t.ID.String(), "status": t.Status})
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

// writeJSON writes v as a JSON response with the given HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) //nolint:errcheck
}



// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

// statusWriter wraps http.ResponseWriter to capture the response status code.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.status = code
	sw.ResponseWriter.WriteHeader(code)
}

// slogMiddleware logs every HTTP request with method, path, status, and
// duration using structured logging.
func slogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(sw, r)

		duration := time.Since(start)
		attrs := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration", duration.String(),
		}

		switch {
		case sw.status >= 500:
			slog.Error("request completed", attrs...)
		case sw.status >= 400:
			slog.Warn("request completed", attrs...)
		default:
			slog.Info("request completed", attrs...)
		}
	})
}

// metricsMiddleware records Prometheus counters and histograms for every HTTP
// request.
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(sw, r)

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(sw.status)

		// Use chi's route pattern (e.g. "/tasks/{id}") instead of the raw
		// URL path to avoid high-cardinality labels from UUIDs.
		path := chi.RouteContext(r.Context()).RoutePattern()
		if path == "" {
			path = r.URL.Path
		}

		httpRequestsTotal.WithLabelValues(r.Method, path, status).Inc()
		httpRequestDuration.WithLabelValues(r.Method, path).Observe(duration)
	})
}
