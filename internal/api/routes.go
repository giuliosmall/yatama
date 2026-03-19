package api

import (
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRouter creates a chi.Router with all middleware and routes registered.
// The returned router is ready to be passed to an http.Server.
func NewRouter(h *Handler) chi.Router {
	r := chi.NewRouter()

	// Middleware applied to every request.
	r.Use(slogMiddleware)
	r.Use(metricsMiddleware)

	// Infrastructure endpoints.
	r.Get("/health", h.HealthCheck)
	r.Get("/ready", h.ReadinessCheck)
	r.Handle("/metrics", promhttp.Handler())

	// Task endpoints.
	r.Post("/tasks", h.CreateTask)
	r.Post("/tasks/batch", h.CreateTaskBatch)
	r.Get("/tasks/{id}", h.GetTask)
	r.Get("/tasks/{id}/history", h.GetTaskHistory)

	// Dead-letter queue endpoints.
	r.Get("/dlq/tasks", h.ListDLQTasks)
	r.Get("/dlq/tasks/{id}", h.GetDLQTask)
	r.Post("/dlq/tasks/{id}/retry", h.RetryDLQTask)

	return r
}
