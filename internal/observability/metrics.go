package observability

import "github.com/prometheus/client_golang/prometheus"

// Extended Prometheus metrics for the scaled architecture.
// These complement the per-package metrics already registered by the worker,
// api, and history packages.

var (
	// Kafka producer metrics.
	KafkaProduceTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_produce_total",
		Help: "Total Kafka produce operations by topic and status.",
	}, []string{"topic", "status"})

	KafkaProduceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_produce_latency_seconds",
		Help:    "Latency of Kafka produce operations by topic.",
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"topic"})

	// Kafka consumer metrics.
	KafkaConsumeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consume_total",
		Help: "Total Kafka records consumed by topic.",
	}, []string{"topic"})

	KafkaConsumeLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consume_lag",
		Help: "Estimated consumer lag by topic and partition.",
	}, []string{"topic", "partition"})

	KafkaCommitTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_commit_total",
		Help: "Total Kafka offset commits by status.",
	}, []string{"status"})

	// End-to-end task latency.
	TaskEndToEndLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "task_end_to_end_latency_seconds",
		Help:    "End-to-end latency from task creation to completion, by type and status.",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"type", "status"})

	// Worker batch metrics (when consuming from Kafka).
	WorkerBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_batch_size",
		Help:    "Number of messages delivered per Kafka poll batch.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
	})

	// Connection pool saturation.
	PGPoolActiveConns = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pg_pool_active_connections",
		Help: "Current number of active PostgreSQL connections from the pool.",
	})

	PGPoolIdleConns = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pg_pool_idle_connections",
		Help: "Current number of idle PostgreSQL connections in the pool.",
	})

	PGPoolMaxConns = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pg_pool_max_connections",
		Help: "Maximum number of connections configured for the pool.",
	})
)

func init() {
	prometheus.MustRegister(
		KafkaProduceTotal,
		KafkaProduceLatency,
		KafkaConsumeTotal,
		KafkaConsumeLag,
		KafkaCommitTotal,
		TaskEndToEndLatency,
		WorkerBatchSize,
		PGPoolActiveConns,
		PGPoolIdleConns,
		PGPoolMaxConns,
	)
}
