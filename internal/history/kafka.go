package history

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ---------------------------------------------------------------------------
// KafkaProducer — publishes history events to a Kafka topic
// ---------------------------------------------------------------------------

// KafkaProducer implements Writer by publishing events to a Kafka topic.
// A separate KafkaConsumer batch-inserts them into PostgreSQL.
type KafkaProducer struct {
	client *kgo.Client
	topic  string
}

// NewKafkaProducer creates a Writer that publishes history events to the given
// Kafka topic. Events are batched by franz-go's internal producer.
func NewKafkaProducer(brokers []string, topic string) (*KafkaProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.ProducerLinger(10*time.Millisecond),
		kgo.RequiredAcks(kgo.LeaderAck()), // history is best-effort; leader ack is sufficient
		kgo.MaxBufferedRecords(50_000),
	)
	if err != nil {
		return nil, fmt.Errorf("history kafka producer: %w", err)
	}
	return &KafkaProducer{client: client, topic: topic}, nil
}

// wireEvent is the JSON encoding of a history Event on Kafka.
type wireEvent struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
	At     string `json:"at"` // RFC3339Nano
}

func (p *KafkaProducer) Write(ctx context.Context, event Event) error {
	data, err := json.Marshal(wireEvent{
		TaskID: event.TaskID.String(),
		Status: event.Status,
		At:     event.At.Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("history kafka marshal: %w", err)
	}

	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(event.TaskID.String()),
		Value: data,
	}

	// Async produce — fire and forget for throughput. Errors are logged by
	// the callback but don't block the caller.
	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			slog.Error("history kafka produce failed",
				"task_id", event.TaskID,
				"error", err,
			)
			return
		}
		historyEventsTotal.Inc()
	})

	return nil
}

func (p *KafkaProducer) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("history kafka flush: %w", err)
	}
	return nil
}

func (p *KafkaProducer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = p.Flush(ctx)
	p.client.Close()
	return nil
}

// ---------------------------------------------------------------------------
// KafkaConsumer — consumes history events and batch-inserts into PG
// ---------------------------------------------------------------------------

// KafkaConsumer reads from a history Kafka topic and batch-inserts events
// into the task_history table using pgx.CopyFrom.
type KafkaConsumer struct {
	client    *kgo.Client
	pool      *pgxpool.Pool
	batchSize int
	flushEvery time.Duration
	wg        sync.WaitGroup
	done      chan struct{}
}

// KafkaConsumerConfig configures the history KafkaConsumer.
type KafkaConsumerConfig struct {
	Brokers       []string
	Topic         string
	GroupID       string
	BatchSize     int           // default 1000
	FlushInterval time.Duration // default 100ms
}

// NewKafkaConsumer creates a consumer that reads history events from Kafka
// and batch-writes them to PostgreSQL.
func NewKafkaConsumer(cfg KafkaConsumerConfig, pool *pgxpool.Pool) (*KafkaConsumer, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "history-writers"
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchMaxBytes(10<<20),
		kgo.SessionTimeout(30*time.Second),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("history kafka consumer: %w", err)
	}

	return &KafkaConsumer{
		client:     client,
		pool:       pool,
		batchSize:  cfg.BatchSize,
		flushEvery: cfg.FlushInterval,
		done:       make(chan struct{}),
	}, nil
}

// Start begins consuming and batch-inserting. It blocks until ctx is
// cancelled or Close is called.
func (c *KafkaConsumer) Start(ctx context.Context) {
	c.wg.Add(1)
	go c.run(ctx)
}

// Close stops the consumer and waits for it to finish.
func (c *KafkaConsumer) Close() error {
	close(c.done)
	c.wg.Wait()
	c.client.Close()
	return nil
}

func (c *KafkaConsumer) run(ctx context.Context) {
	defer c.wg.Done()

	batch := make([]Event, 0, c.batchSize)
	ticker := time.NewTicker(c.flushEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.commitBatch(ctx, batch)
			return
		case <-c.done:
			c.commitBatch(ctx, batch)
			return
		case <-ticker.C:
			if len(batch) > 0 {
				c.commitBatch(ctx, batch)
				batch = make([]Event, 0, c.batchSize)
			}
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			c.commitBatch(ctx, batch)
			return
		}

		fetches.EachRecord(func(r *kgo.Record) {
			var w wireEvent
			if err := json.Unmarshal(r.Value, &w); err != nil {
				slog.Error("history consumer: unmarshal failed", "error", err, "offset", r.Offset)
				c.client.MarkCommitRecords(r)
				return
			}

			id, err := uuid.Parse(w.TaskID)
			if err != nil {
				slog.Error("history consumer: parse task_id failed", "error", err, "raw", w.TaskID)
				c.client.MarkCommitRecords(r)
				return
			}

			at, err := time.Parse(time.RFC3339Nano, w.At)
			if err != nil {
				at = time.Now()
			}

			batch = append(batch, Event{TaskID: id, Status: w.Status, At: at})
			c.client.MarkCommitRecords(r)
		})

		if len(batch) >= c.batchSize {
			c.commitBatch(ctx, batch)
			batch = make([]Event, 0, c.batchSize)
		}
	}
}

// commitBatch writes the batch to PG and commits offsets.
func (c *KafkaConsumer) commitBatch(ctx context.Context, batch []Event) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()
	n, err := c.pool.CopyFrom(ctx,
		pgx.Identifier{"task_history"},
		[]string{"task_id", "status", "occurred_at"},
		pgx.CopyFromSlice(len(batch), func(i int) ([]any, error) {
			return []any{batch[i].TaskID, batch[i].Status, batch[i].At}, nil
		}),
	)
	if err != nil {
		slog.Error("history consumer: batch insert failed", "error", err, "count", len(batch))
		historyFlushTotal.WithLabelValues("error").Inc()
		return
	}

	if err := c.client.CommitMarkedOffsets(ctx); err != nil {
		slog.Error("history consumer: commit offsets failed", "error", err)
	}

	duration := time.Since(start)
	historyFlushTotal.WithLabelValues("success").Inc()
	historyFlushLatency.Observe(duration.Seconds())
	historyFlushBatchSize.Observe(float64(n))
	historyEventsTotal.Add(float64(n))

	slog.Debug("history consumer: batch committed", "count", n, "duration", duration.String())
}
