// Package history provides asynchronous, buffered writing of task history
// events. Instead of inserting one row per status change synchronously,
// events are accumulated in a bounded buffer and flushed in batches using
// pgx.CopyFrom for maximum throughput.
package history

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

var (
	historyBufferDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "history_buffer_depth",
		Help: "Current number of events in the history write buffer.",
	})
	historyFlushTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "history_flush_total",
		Help: "Total number of history flush operations by status.",
	}, []string{"status"})
	historyFlushLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "history_flush_latency_seconds",
		Help:    "Time to complete a history flush to the database.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	})
	historyFlushBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "history_flush_batch_size",
		Help:    "Number of events per history flush.",
		Buckets: []float64{1, 10, 50, 100, 250, 500, 1000, 2500, 5000},
	})
	historyEventsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "history_events_total",
		Help: "Total number of history events written.",
	})
)

func init() {
	prometheus.MustRegister(
		historyBufferDepth,
		historyFlushTotal,
		historyFlushLatency,
		historyFlushBatchSize,
		historyEventsTotal,
	)
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// Event represents a single task status transition.
type Event struct {
	TaskID uuid.UUID
	Status string
	At     time.Time
}

// Writer is the interface for persisting history events.
type Writer interface {
	// Write enqueues a history event for eventual persistence.
	// It is non-blocking under normal load.
	Write(ctx context.Context, event Event) error

	// Flush forces all buffered events to be written immediately.
	Flush(ctx context.Context) error

	// Close flushes remaining events and shuts down the writer.
	Close() error
}

// ---------------------------------------------------------------------------
// SyncWriter — synchronous, single-row INSERT (backward compatible)
// ---------------------------------------------------------------------------

// SyncWriter writes each event synchronously via a single INSERT.
// Use this when you want the pre-scale behavior (every status change in the
// same transaction). It implements Writer.
type SyncWriter struct {
	pool *pgxpool.Pool
}

// NewSyncWriter returns a Writer that inserts one row per call.
func NewSyncWriter(pool *pgxpool.Pool) *SyncWriter {
	return &SyncWriter{pool: pool}
}

func (w *SyncWriter) Write(ctx context.Context, event Event) error {
	_, err := w.pool.Exec(ctx,
		`INSERT INTO task_history (task_id, status, occurred_at) VALUES ($1, $2, $3)`,
		event.TaskID, event.Status, event.At,
	)
	if err != nil {
		return fmt.Errorf("sync history write: %w", err)
	}
	historyEventsTotal.Inc()
	return nil
}

func (w *SyncWriter) Flush(_ context.Context) error { return nil }
func (w *SyncWriter) Close() error                  { return nil }

// ---------------------------------------------------------------------------
// BufferedWriter — batched CopyFrom
// ---------------------------------------------------------------------------

// BufferedWriter accumulates events in a channel and periodically flushes
// them to PostgreSQL using pgx.CopyFrom for high throughput.
type BufferedWriter struct {
	pool      *pgxpool.Pool
	buffer    chan Event
	batchSize int
	flushEvery time.Duration
	wg        sync.WaitGroup
	done      chan struct{}
	closeOnce sync.Once
}

// BufferedWriterConfig configures the BufferedWriter.
type BufferedWriterConfig struct {
	// BatchSize is the max events per flush (default 1000).
	BatchSize int
	// FlushInterval is the max time between flushes (default 100ms).
	FlushInterval time.Duration
	// BufferSize is the channel capacity (default 10000).
	BufferSize int
}

// NewBufferedWriter creates a BufferedWriter and starts its background flush
// goroutine. Call Close to drain and shut down.
func NewBufferedWriter(pool *pgxpool.Pool, cfg BufferedWriterConfig) *BufferedWriter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 10_000
	}

	w := &BufferedWriter{
		pool:       pool,
		buffer:     make(chan Event, cfg.BufferSize),
		batchSize:  cfg.BatchSize,
		flushEvery: cfg.FlushInterval,
		done:       make(chan struct{}),
	}

	w.wg.Add(1)
	go w.flushLoop()

	return w
}

// Write enqueues an event. If the buffer is full it blocks until space is
// available or the context is cancelled.
func (w *BufferedWriter) Write(ctx context.Context, event Event) error {
	select {
	case w.buffer <- event:
		historyBufferDepth.Inc()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush drains the buffer and writes all pending events immediately.
func (w *BufferedWriter) Flush(ctx context.Context) error {
	batch := w.drain()
	if len(batch) == 0 {
		return nil
	}
	return w.writeBatch(ctx, batch)
}

// Close signals the flush loop to stop, drains remaining events, and blocks
// until the final flush completes.
func (w *BufferedWriter) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.done)
		w.wg.Wait()

		// Final drain of anything left in the buffer.
		remaining := w.drain()
		if len(remaining) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = w.writeBatch(ctx, remaining)
		}
	})
	return err
}

// flushLoop runs in a background goroutine. It flushes on timer or when
// enough events have accumulated.
func (w *BufferedWriter) flushLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()

	batch := make([]Event, 0, w.batchSize)

	for {
		select {
		case evt, ok := <-w.buffer:
			if !ok {
				// Channel closed — shouldn't happen, but handle gracefully.
				if len(batch) > 0 {
					w.flushBatch(batch)
				}
				return
			}
			historyBufferDepth.Dec()
			batch = append(batch, evt)

			if len(batch) >= w.batchSize {
				w.flushBatch(batch)
				batch = make([]Event, 0, w.batchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				w.flushBatch(batch)
				batch = make([]Event, 0, w.batchSize)
			}

		case <-w.done:
			// Drain any remaining from the channel.
		drainLoop:
			for {
				select {
				case evt := <-w.buffer:
					historyBufferDepth.Dec()
					batch = append(batch, evt)
				default:
					break drainLoop
				}
			}
			if len(batch) > 0 {
				w.flushBatch(batch)
			}
			return
		}
	}
}

// drain pulls all available events from the buffer without blocking.
func (w *BufferedWriter) drain() []Event {
	var batch []Event
	for {
		select {
		case evt := <-w.buffer:
			historyBufferDepth.Dec()
			batch = append(batch, evt)
		default:
			return batch
		}
	}
}

// flushBatch writes a batch and records metrics.
func (w *BufferedWriter) flushBatch(batch []Event) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.writeBatch(ctx, batch); err != nil {
		slog.Error("history flush failed", "error", err, "count", len(batch))
		historyFlushTotal.WithLabelValues("error").Inc()
	} else {
		historyFlushTotal.WithLabelValues("success").Inc()
	}
}

// writeBatch performs the actual COPY INTO task_history.
func (w *BufferedWriter) writeBatch(ctx context.Context, batch []Event) error {
	if w.pool == nil {
		return fmt.Errorf("history copy: pool is nil")
	}

	start := time.Now()

	n, err := w.pool.CopyFrom(ctx,
		pgx.Identifier{"task_history"},
		[]string{"task_id", "status", "occurred_at"},
		pgx.CopyFromSlice(len(batch), func(i int) ([]any, error) {
			return []any{batch[i].TaskID, batch[i].Status, batch[i].At}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("history copy: %w", err)
	}

	duration := time.Since(start)
	historyFlushLatency.Observe(duration.Seconds())
	historyFlushBatchSize.Observe(float64(n))
	historyEventsTotal.Add(float64(n))

	slog.Debug("history batch flushed", "count", n, "duration", duration.String())
	return nil
}
