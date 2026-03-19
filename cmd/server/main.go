package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/giulio/task-manager/internal/api"
	"github.com/giulio/task-manager/internal/queue"
	"github.com/giulio/task-manager/internal/task"
	"github.com/giulio/task-manager/internal/worker"
)

func main() {
	// Structured JSON logging.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	// Read configuration from environment variables.
	databaseURL := envOrDefault("DATABASE_URL", "postgres://postgres:taskmanager@localhost:5432/taskmanager?sslmode=disable")
	serverPort := envOrDefault("SERVER_PORT", "8080")
	concurrency, _ := strconv.Atoi(envOrDefault("WORKER_CONCURRENCY", "16"))
	if concurrency < 1 {
		concurrency = 16
	}
	reapIntervalSeconds, _ := strconv.Atoi(envOrDefault("REAPER_INTERVAL_SECONDS", "30"))
	drainSeconds, _ := strconv.Atoi(envOrDefault("DRAIN_SECONDS", "5"))
	shutdownSeconds, _ := strconv.Atoi(envOrDefault("SHUTDOWN_TIMEOUT_SECONDS", "10"))
	workerWaitSeconds, _ := strconv.Atoi(envOrDefault("WORKER_WAIT_TIMEOUT_SECONDS", "30"))

	// Root context with signal-based cancellation.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Connect to PostgreSQL.
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("failed to ping database", "error", err)
		os.Exit(1)
	}
	slog.Info("connected to database")

	// Build dependencies.
	repo := task.NewPostgresRepository(pool)
	registry := task.NewRegistry()
	producer := queue.NewPostgresProducer()
	consumer := queue.NewPostgresConsumer(repo, 1*time.Second)
	drain := api.NewDrainState()
	handler := api.NewHandler(repo, registry, producer, drain)
	router := api.NewRouter(handler)

	// Start worker pool.
	reapInterval := time.Duration(reapIntervalSeconds) * time.Second
	wp := worker.NewPool(consumer, repo, registry, concurrency, reapInterval)
	wp.Start(ctx)
	slog.Info("worker pool started", "concurrency", concurrency, "reap_interval", reapInterval.String())

	// Start HTTP server.
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", serverPort),
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("server listening", "port", serverPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			cancel()
		}
	}()

	// Wait for shutdown signal.
	<-ctx.Done()
	slog.Info("shutting down...")

	// Phase 1: Flip readiness probe to 503 so Kubernetes stops routing
	// traffic to this pod, then keep the listener open for the drain window
	// so in-flight requests complete and kube-proxy rules propagate.
	drain.StartDraining()
	slog.Info("readiness probe flipped to 503, draining", "drain_seconds", drainSeconds)
	time.Sleep(time.Duration(drainSeconds) * time.Second)

	// Phase 2: Stop accepting new connections and wait for in-flight
	// HTTP requests to finish.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(shutdownSeconds)*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}
	slog.Info("HTTP server stopped")

	// Phase 3: Wait for workers to finish in-flight tasks with a bounded
	// timeout so the process doesn't hang indefinitely.
	if wp.WaitWithTimeout(time.Duration(workerWaitSeconds) * time.Second) {
		slog.Info("all workers finished")
	} else {
		slog.Warn("some workers did not finish before timeout")
	}
	slog.Info("shutdown complete")
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
