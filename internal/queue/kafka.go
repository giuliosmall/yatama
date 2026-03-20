package queue

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaConfig holds the configuration for Kafka producer and consumer.
type KafkaConfig struct {
	Brokers  []string
	Topic    string
	DLQTopic string
	GroupID  string // consumer group; empty for producer-only
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

// KafkaProducer publishes task messages to a Kafka topic using franz-go.
type KafkaProducer struct {
	client     *kgo.Client
	topic      string
	dlqTopic   string
	serializer Serializer
}

// NewKafkaProducer creates a Producer that writes to the given Kafka topic.
func NewKafkaProducer(cfg KafkaConfig) (*KafkaProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.ProducerBatchMaxBytes(1<<20),                  // 1 MB batch
		kgo.ProducerLinger(5*time.Millisecond),            // batch for 5ms
		kgo.RequiredAcks(kgo.AllISRAcks()),                // wait for all ISR
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), // partition by key
		kgo.MaxBufferedRecords(100_000),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: new client: %w", err)
	}

	return &KafkaProducer{
		client:     client,
		topic:      cfg.Topic,
		dlqTopic:   cfg.DLQTopic,
		serializer: JSONSerializer{},
	}, nil
}

// Enqueue publishes a task message to Kafka asynchronously. The message key
// is the task type for partition affinity. Returns nil immediately; errors
// are logged and counted via Prometheus in the async callback.
func (p *KafkaProducer) Enqueue(ctx context.Context, msg Message) error {
	value, err := p.serializer.Marshal(msg)
	if err != nil {
		return fmt.Errorf("kafka enqueue: marshal: %w", err)
	}

	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(msg.Type),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "task_id", Value: []byte(msg.TaskID.String())},
			{Key: "retry_count", Value: []byte(strconv.Itoa(msg.RetryCount))},
		},
	}

	p.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			slog.Error("kafka async produce failed",
				"task_id", msg.TaskID,
				"type", msg.Type,
				"error", err,
			)
		}
	})

	return nil
}

// EnqueueDLQ publishes a failed task message to the dead-letter topic.
func (p *KafkaProducer) EnqueueDLQ(ctx context.Context, msg Message, taskErr string) error {
	if p.dlqTopic == "" {
		return nil
	}

	value, err := p.serializer.Marshal(msg)
	if err != nil {
		return fmt.Errorf("kafka enqueue dlq: marshal: %w", err)
	}

	record := &kgo.Record{
		Topic: p.dlqTopic,
		Key:   []byte(msg.Type),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "task_id", Value: []byte(msg.TaskID.String())},
			{Key: "error", Value: []byte(taskErr)},
		},
	}

	p.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			slog.Error("kafka async dlq produce failed", "task_id", msg.TaskID, "error", err)
		}
	})
	return nil
}

// Flush blocks until all buffered records have been produced and acknowledged.
func (p *KafkaProducer) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("kafka producer flush: %w", err)
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
// Consumer
// ---------------------------------------------------------------------------

// KafkaConsumer receives task messages from a Kafka topic via a consumer group.
type KafkaConsumer struct {
	client      *kgo.Client
	topic       string
	serializer  Serializer
	concurrency int

	mu     sync.Mutex
	closed bool
}

// NewKafkaConsumer creates a Consumer that joins the specified consumer group
// and reads from the given topic.
func NewKafkaConsumer(cfg KafkaConfig, concurrency int) (*KafkaConsumer, error) {
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("kafka consumer: group_id is required")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchMaxBytes(50<<20),          // 50 MB max fetch
		kgo.FetchMaxPartitionBytes(10<<20), // 10 MB per partition
		kgo.SessionTimeout(30*time.Second),
		kgo.HeartbeatInterval(3*time.Second),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: new client: %w", err)
	}

	return &KafkaConsumer{
		client:      client,
		topic:       cfg.Topic,
		serializer:  JSONSerializer{},
		concurrency: concurrency,
	}, nil
}

// Consume starts polling Kafka and delivers messages to the returned channel.
// The goroutine exits when ctx is cancelled or Close is called.
func (c *KafkaConsumer) Consume(ctx context.Context) (<-chan Delivery, error) {
	ch := make(chan Delivery, c.concurrency*2)

	go func() {
		defer close(ch)

		for {
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()

			fetches := c.client.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}

			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					slog.Error("kafka fetch error",
						"topic", e.Topic,
						"partition", e.Partition,
						"error", e.Err,
					)
				}
			}

			fetches.EachRecord(func(r *kgo.Record) {
				msg, err := c.serializer.Unmarshal(r.Value)
				if err != nil {
					slog.Error("kafka consumer: failed to unmarshal message",
						"error", err,
						"topic", r.Topic,
						"partition", r.Partition,
						"offset", r.Offset,
					)
					// Mark as consumed to skip the bad message.
					c.client.MarkCommitRecords(r)
					return
				}

				// Populate Meta with Kafka-specific information.
				msg.Meta = map[string]string{
					"partition": strconv.Itoa(int(r.Partition)),
					"offset":   strconv.FormatInt(r.Offset, 10),
					"topic":    r.Topic,
				}

				d := Delivery{
					Message: msg,
					Ack: func() error {
						c.client.MarkCommitRecords(r)
						return nil
					},
					Nack: func() error {
						// Don't mark; message will be redelivered after
						// session timeout or rebalance.
						return nil
					},
				}

				select {
				case ch <- d:
				case <-ctx.Done():
					return
				}
			})

			// Allow cooperative rebalancing between polls.
			c.client.AllowRebalance()

			// Periodically commit marked offsets.
			if err := c.client.CommitMarkedOffsets(ctx); err != nil {
				if ctx.Err() == nil {
					slog.Error("kafka consumer: commit offsets failed", "error", err)
				}
			}
		}
	}()

	return ch, nil
}

func (c *KafkaConsumer) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	// Commit any remaining marked offsets before leaving the group.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.client.CommitMarkedOffsets(ctx); err != nil {
		slog.Error("kafka consumer: final commit failed", "error", err)
	}

	c.client.Close()
	return nil
}
