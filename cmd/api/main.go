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

	"github.com/giulio/task-manager/internal/api"
	"github.com/giulio/task-manager/internal/observability"
	"github.com/giulio/task-manager/internal/queue"
	"github.com/giulio/task-manager/internal/task"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	databaseURL := envRequired("DATABASE_URL")
	serverPort := envOrDefault("SERVER_PORT", "8080")
	drainSeconds := atoi(envOrDefault("DRAIN_SECONDS", "5"))
	shutdownSeconds := atoi(envOrDefault("SHUTDOWN_TIMEOUT_SECONDS", "10"))
	kafkaBrokers := envOrDefault("KAFKA_BROKERS", "")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// OpenTelemetry tracing (disabled if OTEL_EXPORTER_OTLP_ENDPOINT is empty).
	otelEndpoint := envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	tracerShutdown, err := observability.InitTracer(ctx, "task-manager-api", otelEndpoint)
	if err != nil {
		slog.Error("failed to init tracer", "error", err)
		os.Exit(1)
	}
	defer tracerShutdown(context.Background())

	maxConns := atoi(envOrDefault("PG_MAX_CONNS", "100"))
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		slog.Error("failed to parse database URL", "error", err)
		os.Exit(1)
	}
	poolConfig.MaxConns = int32(maxConns)
	poolConfig.MinConns = int32(maxConns) // pre-warm all connections at startup
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

	// Queue backend selection: Kafka if KAFKA_BROKERS is set, otherwise Postgres.
	kafkaTopic := envOrDefault("KAFKA_TOPIC", "tasks")
	kafkaDLQTopic := envOrDefault("KAFKA_DLQ_TOPIC", "tasks-dlq")

	var producer queue.Producer
	if kafkaBrokers != "" {
		brokers := strings.Split(kafkaBrokers, ",")
		kp, err := queue.NewKafkaProducer(queue.KafkaConfig{
			Brokers:  brokers,
			Topic:    kafkaTopic,
			DLQTopic: kafkaDLQTopic,
		})
		if err != nil {
			slog.Error("failed to create kafka producer", "error", err)
			os.Exit(1)
		}
		producer = kp
		slog.Info("kafka producer initialized", "brokers", kafkaBrokers, "topic", kafkaTopic)
	} else {
		producer = queue.NewPostgresProducer()
	}
	defer producer.Close()

	drain := api.NewDrainState()
	handler := api.NewHandler(repo, registry, producer, drain)
	router := api.NewRouter(handler)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", serverPort),
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("api server listening", "port", serverPort, "queue_backend", queueBackend(kafkaBrokers))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			cancel()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down api server...")

	drain.StartDraining()
	slog.Info("readiness probe flipped to 503, draining", "drain_seconds", drainSeconds)
	time.Sleep(time.Duration(drainSeconds) * time.Second)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(shutdownSeconds)*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}
	slog.Info("api server stopped")
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
