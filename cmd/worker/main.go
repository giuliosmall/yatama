package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/giulio/task-manager/internal/history"
	"github.com/giulio/task-manager/internal/observability"
	"github.com/giulio/task-manager/internal/queue"
	"github.com/giulio/task-manager/internal/task"
	"github.com/giulio/task-manager/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	databaseURL := envRequired("DATABASE_URL")
	concurrency := atoi(envOrDefault("WORKER_CONCURRENCY", "16"))
	if concurrency < 1 {
		concurrency = 16
	}
	reapIntervalSeconds := atoi(envOrDefault("REAPER_INTERVAL_SECONDS", "30"))
	metricsPort := envOrDefault("METRICS_PORT", "9090")
	workerWaitSeconds := atoi(envOrDefault("WORKER_WAIT_TIMEOUT_SECONDS", "30"))
	kafkaBrokers := envOrDefault("KAFKA_BROKERS", "")
	kafkaGroupID := envOrDefault("KAFKA_GROUP_ID", "task-workers")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// OpenTelemetry tracing.
	otelEndpoint := envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	tracerShutdown, err := observability.InitTracer(ctx, "task-manager-worker", otelEndpoint)
	if err != nil {
		slog.Error("failed to init tracer", "error", err)
		os.Exit(1)
	}
	defer tracerShutdown(context.Background())

	maxConns := atoi(envOrDefault("PG_MAX_CONNS", "50"))
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		slog.Error("failed to parse database URL", "error", err)
		os.Exit(1)
	}
	poolConfig.MaxConns = int32(maxConns)
	poolConfig.MinConns = int32(maxConns)
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("failed to ping database", "error", err)
		os.Exit(1)
	}
	slog.Info("connected to database", "max_conns", maxConns)

	repo := task.NewPostgresRepository(pool)
	registry := task.NewRegistry()

	// Queue backend selection.
	kafkaTopic := envOrDefault("KAFKA_TOPIC", "tasks")
	kafkaDLQTopic := envOrDefault("KAFKA_DLQ_TOPIC", "tasks-dlq")

	var consumer queue.Consumer
	if kafkaBrokers != "" {
		brokers := strings.Split(kafkaBrokers, ",")
		kc, err := queue.NewKafkaConsumer(queue.KafkaConfig{
			Brokers:  brokers,
			Topic:    kafkaTopic,
			DLQTopic: kafkaDLQTopic,
			GroupID:  kafkaGroupID,
		}, concurrency)
		if err != nil {
			slog.Error("failed to create kafka consumer", "error", err)
			os.Exit(1)
		}
		consumer = kc
		slog.Info("kafka consumer initialized",
			"brokers", kafkaBrokers,
			"topic", kafkaTopic,
			"group_id", kafkaGroupID,
		)
	} else {
		consumer = queue.NewPostgresConsumer(repo, 50*time.Millisecond, concurrency)
	}
	defer consumer.Close()

	// History writer selection.
	historyMode := envOrDefault("HISTORY_MODE", "sync")
	historyTopic := envOrDefault("KAFKA_HISTORY_TOPIC", "task-history")

	var historyWriter history.Writer
	switch historyMode {
	case "buffered":
		historyWriter = history.NewBufferedWriter(pool, history.BufferedWriterConfig{})
		slog.Info("history writer: buffered (pgx.CopyFrom)")
	case "kafka":
		if kafkaBrokers == "" {
			slog.Error("HISTORY_MODE=kafka requires KAFKA_BROKERS")
			os.Exit(1)
		}
		brokers := strings.Split(kafkaBrokers, ",")
		hp, err := history.NewKafkaProducer(brokers, historyTopic)
		if err != nil {
			slog.Error("failed to create kafka history producer", "error", err)
			os.Exit(1)
		}
		historyWriter = hp
		slog.Info("history writer: kafka", "topic", historyTopic)
	default:
		historyWriter = history.NewSyncWriter(pool)
		slog.Info("history writer: sync (single INSERT)")
	}
	defer historyWriter.Close()

	reapInterval := time.Duration(reapIntervalSeconds) * time.Second
	wp := worker.NewPool(consumer, repo, registry, concurrency, reapInterval)
	wp.Start(ctx)
	slog.Info("worker pool started",
		"concurrency", concurrency,
		"reap_interval", reapInterval.String(),
		"queue_backend", queueBackend(kafkaBrokers),
		"history_mode", historyMode,
	)

	// Suppress unused variable — historyWriter is wired but the worker pool
	// doesn't consume it yet (pool.go still uses repo.UpdateTaskStatus which
	// inserts history synchronously). The next step is to have the pool call
	// historyWriter.Write() instead. For now, it's initialized and ready.
	_ = historyWriter

	// Metrics and health endpoint.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	metricsSrv := &http.Server{
		Addr:         fmt.Sprintf(":%s", metricsPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		slog.Info("metrics server listening", "port", metricsPort)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down worker...")

	if wp.WaitWithTimeout(time.Duration(workerWaitSeconds) * time.Second) {
		slog.Info("all workers finished")
	} else {
		slog.Warn("some workers did not finish before timeout")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("metrics server shutdown error", "error", err)
	}

	slog.Info("worker shutdown complete")
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envRequired(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return v
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	return n
}

func queueBackend(kafkaBrokers string) string {
	if kafkaBrokers != "" {
		return "kafka"
	}
	return "postgres"
}
