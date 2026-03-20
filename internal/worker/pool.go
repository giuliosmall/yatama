// Package worker provides a concurrent worker pool that claims tasks from a
// repository, executes them via a registry of task functions, and handles
// retries, timeouts, and graceful shutdown.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/giulio/task-manager/internal/history"
	"github.com/giulio/task-manager/internal/queue"
	"github.com/giulio/task-manager/internal/task"
)

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

var (
	tasksInProgress = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "worker_tasks_in_progress",
			Help: "Number of tasks currently being executed by workers.",
		},
	)

	taskDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "worker_task_duration_seconds",
			Help:    "Duration of task execution in seconds, labeled by type and status.",
			Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"type", "status"},
	)

	tasksProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_tasks_processed_total",
			Help: "Total number of tasks processed, labeled by type and outcome status.",
		},
		[]string{"type", "status"},
	)

	dlqTasksDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "worker_dlq_tasks_depth",
			Help: "Current number of tasks in the dead-letter queue.",
		},
	)

	reaperTasksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "reaper_tasks_total",
			Help: "Total number of tasks processed by the stuck-task reaper.",
		},
		[]string{"outcome"},
	)

	tasksSkippedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "worker_tasks_skipped_total",
			Help: "Total number of tasks skipped due to terminal status (idempotency check).",
		},
	)
)

func init() {
	prometheus.MustRegister(tasksInProgress, taskDurationSeconds, tasksProcessedTotal, dlqTasksDepth, reaperTasksTotal, tasksSkippedTotal)
}

// DLQDepthDec decrements the DLQ depth gauge. Call this when a task is
// retried out of the dead-letter queue.
func DLQDepthDec() { dlqTasksDepth.Dec() }

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

// Pool manages a set of concurrent worker goroutines that poll for queued
// tasks, execute them with per-task timeouts, and update their status in the
// repository. Use NewPool to create a Pool and Start to launch workers.
type Pool struct {
	repo          task.Repository
	consumer      queue.Consumer
	registry      *task.Registry
	historyWriter history.Writer
	concurrency   int
	reapInterval  time.Duration
	wg            sync.WaitGroup
}

// NewPool creates a Pool with the given queue consumer, metadata repository,
// task registry, history writer, concurrency level, and reap interval.
func NewPool(consumer queue.Consumer, repo task.Repository, registry *task.Registry, hw history.Writer, concurrency int, reapInterval time.Duration) *Pool {
	return &Pool{
		repo:          repo,
		consumer:      consumer,
		registry:      registry,
		historyWriter: hw,
		concurrency:   concurrency,
		reapInterval:  reapInterval,
	}
}

// isTerminal reports whether a task status is a final state.
func isTerminal(status string) bool {
	return status == "succeeded" || status == "failed" || status == "dead_lettered"
}

// Start launches p.concurrency worker goroutines that consume from the queue,
// plus a single reaper goroutine. When the provided context is cancelled,
// every goroutine finishes its current in-flight work and exits.
// Use Wait to block until all goroutines have stopped.
func (p *Pool) Start(ctx context.Context) {
	deliveries, err := p.consumer.Consume(ctx)
	if err != nil {
		slog.Error("failed to start queue consumer", "error", err)
		return
	}

	for i := 0; i < p.concurrency; i++ {
		p.wg.Add(1)
		go func(workerID int) {
			defer p.wg.Done()
			p.run(ctx, workerID, deliveries)
		}(i)
	}

	if p.reapInterval > 0 {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.reap(ctx)
		}()
	}
}

// Wait blocks until every worker goroutine launched by Start has returned.
// Callers should cancel the context passed to Start before calling Wait.
func (p *Pool) Wait() {
	p.wg.Wait()
}

// WaitWithTimeout blocks until all workers finish or the timeout expires.
// It returns true if all workers completed, false if the timeout was reached.
func (p *Pool) WaitWithTimeout(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		slog.Warn("worker pool wait timed out", "timeout", timeout.String())
		return false
	}
}

// ---------------------------------------------------------------------------
// Worker loop
// ---------------------------------------------------------------------------

// run is the main loop for a single worker goroutine. It reads from the
// shared deliveries channel and executes each task until the channel is
// closed or ctx is cancelled.
func (p *Pool) run(ctx context.Context, workerID int, deliveries <-chan queue.Delivery) {
	slog.Info("worker started", "worker_id", workerID)
	defer slog.Info("worker stopped", "worker_id", workerID)

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			slog.Info("task claimed", "task_id", d.Message.TaskID, "type", d.Message.Type, "worker_id", workerID)
			p.executeDelivery(ctx, d, workerID)
		}
	}
}

// executeDelivery checks for duplicate delivery, converts a queue.Delivery
// into a task.Task, and executes it.
func (p *Pool) executeDelivery(ctx context.Context, d queue.Delivery, workerID int) {
	// Idempotency check: skip if already in a terminal state.
	existing, err := p.repo.GetTask(ctx, d.Message.TaskID)
	if err == nil && isTerminal(existing.Status) {
		slog.Debug("skipping terminal task",
			"task_id", d.Message.TaskID,
			"status", existing.Status,
			"worker_id", workerID,
		)
		tasksSkippedTotal.Inc()
		_ = d.Ack()
		return
	}

	t := &task.Task{
		ID:             d.Message.TaskID,
		Name:           d.Message.Name,
		Type:           d.Message.Type,
		Payload:        d.Message.Payload,
		Priority:       d.Message.Priority,
		RetryCount:     d.Message.RetryCount,
		MaxRetries:     d.Message.MaxRetries,
		TimeoutSeconds: d.Message.TimeoutSeconds,
	}
	p.execute(ctx, t, workerID)
}

// ---------------------------------------------------------------------------
// Reaper loop
// ---------------------------------------------------------------------------

// sleep pauses for d but returns false immediately if ctx is cancelled.
func (p *Pool) sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// reap periodically scans for tasks stuck in "running" beyond their timeout
// and reclaims them by requeuing or moving to the DLQ.
func (p *Pool) reap(ctx context.Context) {
	slog.Info("reaper started", "interval", p.reapInterval.String())
	defer slog.Info("reaper stopped")

	for {
		if !p.sleep(ctx, p.reapInterval) {
			return
		}

		requeued, deadLettered, err := p.repo.ReapStaleTasks(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("reaper cycle failed", "error", err)
			continue
		}

		if requeued > 0 || deadLettered > 0 {
			slog.Info("reaper cycle completed",
				"requeued", requeued,
				"dead_lettered", deadLettered,
			)
		}

		reaperTasksTotal.WithLabelValues("requeued").Add(float64(requeued))
		reaperTasksTotal.WithLabelValues("dead_lettered").Add(float64(deadLettered))
		dlqTasksDepth.Add(float64(deadLettered))
	}
}

// ---------------------------------------------------------------------------
// Task execution
// ---------------------------------------------------------------------------

// execute runs a single claimed task: looks up the task function, enforces
// the per-task timeout, records metrics, and updates the repository status.
func (p *Pool) execute(ctx context.Context, t *task.Task, workerID int) {
	fn, ok := p.registry.Get(t.Type)
	if !ok {
		slog.Error("unknown task type", "task_id", t.ID, "type", t.Type, "worker_id", workerID)
		if err := p.repo.UpdateTaskStatusOnly(ctx, t.ID, "failed"); err != nil {
			slog.Error("failed to update status for unknown type", "task_id", t.ID, "error", err, "worker_id", workerID)
		}
		_ = p.historyWriter.Write(ctx, history.Event{TaskID: t.ID, Status: "failed", At: time.Now()})
		return
	}

	// Per-task timeout context.
	timeout := time.Duration(t.TimeoutSeconds) * time.Second
	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tasksInProgress.Inc()
	start := time.Now()

	slog.Info("starting task", "task_id", t.ID, "type", t.Type, "worker_id", workerID)
	err := fn(taskCtx, t.Payload)
	duration := time.Since(start)

	tasksInProgress.Dec()

	if err != nil {
		p.handleFailure(ctx, t, workerID, err, duration)
		return
	}

	p.handleSuccess(ctx, t, workerID, duration)
}

// handleSuccess updates the task to "succeeded" and records metrics.
func (p *Pool) handleSuccess(ctx context.Context, t *task.Task, workerID int, duration time.Duration) {
	status := "succeeded"

	taskDurationSeconds.WithLabelValues(t.Type, status).Observe(duration.Seconds())
	tasksProcessedTotal.WithLabelValues(t.Type, status).Inc()

	if err := p.repo.UpdateTaskStatusOnly(ctx, t.ID, status); err != nil {
		slog.Error("failed to update task status to completed", "task_id", t.ID, "error", err, "worker_id", workerID)
		return
	}

	if err := p.historyWriter.Write(ctx, history.Event{TaskID: t.ID, Status: status, At: time.Now()}); err != nil {
		slog.Error("failed to write history event", "task_id", t.ID, "error", err, "worker_id", workerID)
	}

	slog.Info("task succeeded", "task_id", t.ID, "type", t.Type, "duration", duration.String(), "worker_id", workerID)
}

// handleFailure either requeues the task (if retries remain) or marks it as
// permanently failed, recording metrics in both cases.
func (p *Pool) handleFailure(ctx context.Context, t *task.Task, workerID int, taskErr error, duration time.Duration) {
	if t.RetryCount < t.MaxRetries {
		taskDurationSeconds.WithLabelValues(t.Type, "retry").Observe(duration.Seconds())
		tasksProcessedTotal.WithLabelValues(t.Type, "retry").Inc()

		if err := p.repo.RequeueTaskOnly(ctx, t.ID); err != nil {
			slog.Error("failed to requeue task", "task_id", t.ID, "error", err, "worker_id", workerID)
			return
		}

		if err := p.historyWriter.Write(ctx, history.Event{TaskID: t.ID, Status: "queued", At: time.Now()}); err != nil {
			slog.Error("failed to write history event", "task_id", t.ID, "error", err, "worker_id", workerID)
		}

		slog.Info("task requeued",
			"task_id", t.ID,
			"type", t.Type,
			"retry_count", t.RetryCount,
			"max_retries", t.MaxRetries,
			"error", taskErr,
			"worker_id", workerID,
		)
		return
	}

	status := "dead_lettered"

	taskDurationSeconds.WithLabelValues(t.Type, status).Observe(duration.Seconds())
	tasksProcessedTotal.WithLabelValues(t.Type, status).Inc()
	dlqTasksDepth.Inc()

	if err := p.repo.MoveTaskToDLQOnly(ctx, t.ID, taskErr.Error()); err != nil {
		slog.Error("failed to move task to DLQ", "task_id", t.ID, "error", err, "worker_id", workerID)
		return
	}

	if err := p.historyWriter.Write(ctx, history.Event{TaskID: t.ID, Status: status, At: time.Now()}); err != nil {
		slog.Error("failed to write history event", "task_id", t.ID, "error", err, "worker_id", workerID)
	}

	slog.Error("task dead-lettered",
		"task_id", t.ID,
		"type", t.Type,
		"retry_count", t.RetryCount,
		"max_retries", t.MaxRetries,
		"error", taskErr,
		"worker_id", workerID,
	)
}
