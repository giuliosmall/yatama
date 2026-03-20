// Package outbox implements the transactional outbox pattern for reliable
// message dispatch. Tasks and their outbox entries are written in the same
// PostgreSQL transaction. The Publisher polls for unpublished entries and
// produces them to Kafka, guaranteeing at-least-once delivery.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/giulio/task-manager/internal/queue"
)

var (
	outboxPublishedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "outbox_published_total",
		Help: "Total outbox entries published to Kafka.",
	})
	outboxPollLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "outbox_poll_latency_seconds",
		Help:    "Time to complete an outbox poll+publish cycle.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	})
	outboxDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_depth",
		Help: "Current number of unpublished outbox entries.",
	})
)

func init() {
	prometheus.MustRegister(outboxPublishedTotal, outboxPollLatency, outboxDepth)
}

// Config configures the outbox Publisher.
type Config struct {
	PollInterval  time.Duration // how often to poll (default 100ms)
	BatchSize     int           // max rows per poll (default 500)
	CleanupAge    time.Duration // delete published rows older than this (default 1h)
}

// Publisher polls the outbox table and produces messages to Kafka.
type Publisher struct {
	pool         *pgxpool.Pool
	producer     queue.Producer
	pollInterval time.Duration
	batchSize    int
	cleanupAge   time.Duration
	wg           sync.WaitGroup
	done         chan struct{}
}

// NewPublisher creates an outbox Publisher.
func NewPublisher(pool *pgxpool.Pool, producer queue.Producer, cfg Config) *Publisher {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.CleanupAge <= 0 {
		cfg.CleanupAge = 1 * time.Hour
	}
	return &Publisher{
		pool:         pool,
		producer:     producer,
		pollInterval: cfg.PollInterval,
		batchSize:    cfg.BatchSize,
		cleanupAge:   cfg.CleanupAge,
		done:         make(chan struct{}),
	}
}

// Start begins the poll loop in a background goroutine.
func (p *Publisher) Start(ctx context.Context) {
	p.wg.Add(1)
	go p.run(ctx)
	slog.Info("outbox publisher started", "poll_interval", p.pollInterval, "batch_size", p.batchSize)
}

// Close signals the publisher to stop and waits for it to finish.
func (p *Publisher) Close() error {
	close(p.done)
	p.wg.Wait()
	return nil
}

type outboxRow struct {
	id      uuid.UUID
	taskID  uuid.UUID
	payload json.RawMessage
}

func (p *Publisher) run(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	cleanupTicker := time.NewTicker(1 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case <-ticker.C:
			p.poll(ctx)
		case <-cleanupTicker.C:
			p.cleanup(ctx)
		}
	}
}

func (p *Publisher) poll(ctx context.Context) {
	start := time.Now()

	rows, err := p.pool.Query(ctx,
		`SELECT id, task_id, payload FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY created_at ASC
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		p.batchSize,
	)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("outbox poll: query failed", "error", err)
		}
		return
	}

	var batch []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.id, &r.taskID, &r.payload); err != nil {
			slog.Error("outbox poll: scan failed", "error", err)
			rows.Close()
			return
		}
		batch = append(batch, r)
	}
	rows.Close()
	if rows.Err() != nil {
		slog.Error("outbox poll: rows error", "error", rows.Err())
		return
	}

	if len(batch) == 0 {
		return
	}

	// Produce each message to Kafka.
	var publishedIDs []uuid.UUID
	for _, r := range batch {
		var msg queue.Message
		if err := json.Unmarshal(r.payload, &msg); err != nil {
			slog.Error("outbox poll: unmarshal failed", "error", err, "outbox_id", r.id)
			// Mark as published to skip permanently bad rows.
			publishedIDs = append(publishedIDs, r.id)
			continue
		}

		if err := p.producer.Enqueue(ctx, msg); err != nil {
			slog.Error("outbox poll: enqueue failed", "error", err, "task_id", r.taskID)
			// Stop processing this batch — retry on next poll.
			break
		}
		publishedIDs = append(publishedIDs, r.id)
	}

	if len(publishedIDs) > 0 {
		_, err := p.pool.Exec(ctx,
			`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`,
			publishedIDs,
		)
		if err != nil {
			slog.Error("outbox poll: mark published failed", "error", err)
		}

		outboxPublishedTotal.Add(float64(len(publishedIDs)))
	}

	outboxPollLatency.Observe(time.Since(start).Seconds())

	// Update depth gauge.
	var depth int64
	err = p.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM outbox WHERE published_at IS NULL`,
	).Scan(&depth)
	if err == nil {
		outboxDepth.Set(float64(depth))
	}
}

func (p *Publisher) cleanup(ctx context.Context) {
	tag, err := p.pool.Exec(ctx,
		fmt.Sprintf(`DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < now() - interval '%d seconds'`,
			int(p.cleanupAge.Seconds())),
	)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("outbox cleanup failed", "error", err)
		}
		return
	}
	if tag.RowsAffected() > 0 {
		slog.Debug("outbox cleanup", "deleted", tag.RowsAffected())
	}
}
