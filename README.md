# Task Manager

A background task management system built in Go with PostgreSQL as a durable queue backend. Tasks are enqueued via a REST API, claimed by a built-in worker pool using `SELECT FOR UPDATE SKIP LOCKED`, executed with per-task timeouts, and retried on failure. Supports an optional scaled deployment mode with Kafka as the task transport and independently scalable API and worker pods.

## Architecture

### Single-binary mode (default)

```mermaid
graph TD
    Client([Client])

    subgraph Server["Server Process"]
        Router["chi Router<br/>/tasks · /tasks/{id} · /dlq/tasks · /health · /metrics"]

        Handler["Handler (api pkg)<br/>CreateTask · GetTask · GetHistory<br/>ListDLQ · GetDLQ · RetryDLQ"]
        Worker["Worker Pool (N goroutines)<br/>poll → claim → execute → update/DLQ"]
        Reaper["Stuck Task Reaper (1 goroutine)<br/>periodic scan → requeue or DLQ"]
        RepoIface{{"task.Repository (interface)"}}

        Repo["PostgresRepository<br/>CreateTask · ClaimTask · GetTask<br/>UpdateTaskStatus · RequeueTask<br/>MoveTaskToDLQ · RetryDLQTask<br/>ReapStaleTasks"]
        Registry["Registry<br/>send_email() · run_query()"]
    end

    subgraph DB["PostgreSQL"]
        tasks["tasks<br/>id, name, type, payload, status, priority,<br/>run_after, idempotency_key<br/>SELECT FOR UPDATE SKIP LOCKED"]
        history["task_history<br/>status transitions log"]
        dlq["dead_letter_queue<br/>tasks that exhausted all retries"]
    end

    Client --> Router
    Router --> Handler
    Handler --> RepoIface
    Worker --> RepoIface
    Worker --> Registry
    Reaper --> RepoIface
    RepoIface --> Repo
    Repo -- "pgx/v5 pool" --> DB
```

**Task lifecycle:**

```mermaid
stateDiagram-v2
    [*] --> queued
    queued --> running : ClaimTask
    running --> succeeded : success
    running --> queued : retry with backoff (retry_count < max_retries)
    running --> dead_lettered : retries exhausted → DLQ
    dead_lettered --> queued : POST /dlq/tasks/{id}/retry
```

The server runs as a single binary. On startup it launches an HTTP server, a configurable number of worker goroutines, and a stuck-task reaper goroutine in the same process. Workers poll the `tasks` table for queued work, claim rows atomically with row-level locking, execute the associated task function, and record every status transition in `task_history`. The reaper periodically scans for tasks stuck in `"running"` beyond their `timeout_seconds` and reclaims them by requeuing (if retries remain) or moving to the DLQ.

### Scaled mode (Kafka)

When `KAFKA_BROKERS` is set, the system splits into independently deployable API and worker binaries connected via Kafka:

```
Clients → API pods → Kafka → Worker pods → PostgreSQL
                                          → Kafka → History consumer → PostgreSQL
```

- **API pods** (`cmd/api`) accept HTTP requests and produce task messages to Kafka.
- **Worker pods** (`cmd/worker`) consume from Kafka, execute tasks, and write results to PostgreSQL.
- **History** is written asynchronously via a buffered writer (`internal/history`) that batches inserts using `pgx.CopyFrom`.
- **PgBouncer** pools database connections between workers and PostgreSQL.
- **KEDA** auto-scales worker pods based on Kafka consumer group lag.

## Project Structure

```
cmd/server/main.go              Single-binary entrypoint (API + workers in one process)
cmd/api/main.go                 Scaled mode: API-only binary (HTTP + Kafka producer)
cmd/worker/main.go              Scaled mode: Worker-only binary (Kafka consumer + executor)
cmd/stress/main.go              Stress test tool -- blast + monitor + report
internal/api/handler.go         HTTP handlers (CreateTask, GetTask, GetTaskHistory, DLQ, HealthCheck)
internal/api/routes.go          chi router setup and middleware registration
internal/queue/queue.go         Queue interface (Producer, Consumer, Message, Delivery)
internal/queue/postgres.go      PostgreSQL adapter (polling via ClaimTask)
internal/queue/kafka.go         Kafka adapter (franz-go producer/consumer)
internal/queue/serialization.go JSON serializer for queue messages
internal/history/writer.go      Async buffered history writer (SyncWriter, BufferedWriter)
internal/history/kafka.go       Kafka-backed history writer
internal/observability/         Tracing (OpenTelemetry) and Prometheus metrics helpers
internal/task/models.go         Domain types: Task, TaskHistory, DeadLetterTask, CreateTaskRequest
internal/task/repository.go     Repository interface and PostgreSQL implementation
internal/task/registry.go       Thread-safe registry mapping task type names to functions
internal/worker/pool.go         Concurrent worker pool with polling, timeouts, and retries
pkg/email/email.go              send_email task implementation (simulated, 3s sleep)
pkg/query/query.go              run_query task implementation (simulated, 2s sleep, 20% failure rate)
pkg/noop/noop.go                noop task implementation (instant return, for throughput testing)
migrations/                     SQL migration files (001-005: tables, DLQ, run_after, reaper+idempotency, scale optimizations)
deploy/                         Kubernetes manifests, kind config, Helm values
deploy/kafka/                   Strimzi Kafka manifests and topic definitions
Makefile                        Build, deploy, and teardown targets
Dockerfile                      Multi-stage build for single-binary mode
Dockerfile.api                  Multi-stage build for API binary
Dockerfile.worker               Multi-stage build for worker binary
```

## Prerequisites

| Tool      | Minimum Version | Purpose                          |
|-----------|-----------------|----------------------------------|
| Go        | 1.25+           | Build and test the binary        |
| Docker    | 20.10+          | Container builds, testcontainers |
| kind      | 0.20+           | Local Kubernetes cluster         |
| kubectl   | 1.27+           | Cluster management               |
| Helm      | 3.12+           | PostgreSQL chart deployment      |

## Quick Start

Deploy everything (kind cluster + PostgreSQL + migrations + application) with a single command:

```bash
make all
```

This runs the following steps:

1. **kind-up** -- Creates a local Kubernetes cluster named `task-manager` (skips if it already exists).
2. **build** -- Builds the Docker image and loads it into the kind cluster.
3. **deploy-pg** -- Installs PostgreSQL via the Bitnami Helm chart.
4. **wait-pg** -- Waits for the PostgreSQL pod to become ready.
5. **migrate** -- Applies all SQL migrations (`001_create_tables`, `002_create_dead_letter_queue`, `003_add_run_after`, `004_add_reaper_and_idempotency`, `005_optimize_for_scale`) inside the PostgreSQL pod.
6. **deploy-app** -- Applies the ConfigMap, Deployment, and Service manifests.
7. **deploy-prometheus** -- Deploys Prometheus with a scrape config targeting the app.
8. **deploy-grafana** -- Deploys Grafana with a pre-built dashboard for all app metrics.

When finished the API is accessible at `http://localhost:8080` and the Grafana dashboard at `http://localhost:3000`.

To tear everything down:

```bash
make clean
```

## Manual Build

Single-binary mode:

```bash
go build -o task-manager ./cmd/server/
```

Scaled mode (separate binaries):

```bash
go build -o task-manager-api    ./cmd/api
go build -o task-manager-worker ./cmd/worker
```

Run locally (requires a running PostgreSQL instance):

```bash
export DATABASE_URL="postgres://postgres:taskmanager@localhost:5432/taskmanager?sslmode=disable"
./task-manager
```

## Environment Variables

| Variable             | Default                                                                      | Description                          |
|----------------------|------------------------------------------------------------------------------|--------------------------------------|
| `DATABASE_URL`                 | `postgres://postgres:taskmanager@localhost:5432/taskmanager?sslmode=disable` | PostgreSQL connection string                 |
| `SERVER_PORT`                  | `8080`                                                                       | HTTP listen port                             |
| `WORKER_CONCURRENCY`          | `16`                                                                         | Number of parallel worker goroutines         |
| `REAPER_INTERVAL_SECONDS`     | `30`                                                                         | How often the stuck-task reaper scans (0 to disable) |
| `DRAIN_SECONDS`               | `5`                                                                          | Seconds to keep listener open after SIGTERM  |
| `SHUTDOWN_TIMEOUT_SECONDS`    | `10`                                                                         | HTTP server shutdown timeout                 |
| `WORKER_WAIT_TIMEOUT_SECONDS` | `30`                                                                         | Max seconds to wait for in-flight workers    |
| `KAFKA_BROKERS`               | _(empty)_                                                                    | Comma-separated Kafka bootstrap servers (enables scaled mode) |
| `KAFKA_TOPIC`                 | `tasks`                                                                      | Kafka topic for task messages                |
| `KAFKA_DLQ_TOPIC`             | `tasks-dlq`                                                                  | Kafka topic for dead-letter messages         |
| `KAFKA_GROUP_ID`              | `task-workers`                                                               | Kafka consumer group ID                      |
| `PG_MAX_CONNS`                | `100`                                                                        | Max pgxpool connections (MinConns set equal for pre-warming) |
| `HISTORY_MODE`                | `sync`                                                                       | History writer mode: `sync`, `buffered`, or `kafka` |
| `METRICS_PORT`                | `9090`                                                                       | Port for Prometheus `/metrics` endpoint      |

## API Endpoints

| Method | Path                      | Description                                  |
|--------|---------------------------|----------------------------------------------|
| POST   | `/tasks`                  | Create a new task                            |
| POST   | `/tasks/batch`            | Create up to 1000 tasks in a single transaction |
| GET    | `/tasks/{id}`             | Retrieve a task by ID                        |
| GET    | `/tasks/{id}/history`     | List status transitions for a task           |
| GET    | `/dlq/tasks`              | List all dead-letter queue entries           |
| GET    | `/dlq/tasks/{id}`         | Retrieve a single DLQ entry by ID            |
| POST   | `/dlq/tasks/{id}/retry`   | Retry a dead-lettered task (re-queue it)     |
| GET    | `/health`                 | Liveness probe (always 200)                  |
| GET    | `/ready`                  | Readiness probe (503 when draining)          |
| GET    | `/metrics`                | Prometheus metrics (Go + app)                |

### Create a task

```bash
curl -s -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "welcome-email",
    "type": "send_email",
    "payload": {"to": "alice@example.com", "subject": "Welcome!"},
    "priority": 10,
    "max_retries": 3,
    "timeout_seconds": 60
  }'
```

Response (201 Created):

```json
{"id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}
```

#### Idempotency keys

To safely retry requests without creating duplicates, include an `idempotency_key`:

```bash
curl -s -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "welcome-email",
    "type": "send_email",
    "payload": {"to": "alice@example.com", "subject": "Welcome!"},
    "priority": 10,
    "idempotency_key": "welcome-alice-2026-02-28"
  }'
```

First call returns **201 Created**. Subsequent calls with the same key return **200 OK** with the existing task's ID -- no duplicate task is created. The key is enforced by a PostgreSQL partial unique index on non-NULL values.

### Create tasks in batch

For high throughput, create up to 1000 tasks in a single HTTP call and database transaction:

```bash
curl -s -X POST http://localhost:8080/tasks/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "email-1", "type": "send_email", "payload": {"to": "a@b.com", "subject": "Hi"}},
    {"name": "email-2", "type": "send_email", "payload": {"to": "c@d.com", "subject": "Hey"}},
    {"name": "fast-task", "type": "noop", "payload": {}}
  ]'
```

Response (201 Created):

```json
{"ids": ["uuid-1", "uuid-2", "uuid-3"], "count": 3}
```

### Task types

| Type | Package | Behavior |
|------|---------|----------|
| `send_email` | `pkg/email` | Simulates email sending (3s sleep) |
| `run_query` | `pkg/query` | Simulates query execution (2s sleep, 20% failure rate) |
| `noop` | `pkg/noop` | Returns immediately -- use for throughput testing |

### Get a task

```bash
curl -s http://localhost:8080/tasks/a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

Response (200 OK):

```json
{
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "name": "welcome-email",
  "type": "send_email",
  "payload": {"to": "alice@example.com", "subject": "Welcome!"},
  "status": "succeeded",
  "priority": 10,
  "retry_count": 0,
  "max_retries": 3,
  "timeout_seconds": 60,
  "idempotency_key": "welcome-alice-2026-02-28",
  "created_at": "2025-01-15T10:30:00Z",
  "updated_at": "2025-01-15T10:30:05Z",
  "run_after": "2025-01-15T10:30:00Z"
}
```

The `idempotency_key` field is omitted from the JSON response when not set (null).

### Get task history

```bash
curl -s http://localhost:8080/tasks/a1b2c3d4-e5f6-7890-abcd-ef1234567890/history
```

Response (200 OK):

```json
[
  {"id": "...", "task_id": "a1b2c3d4-...", "status": "queued",    "occurred_at": "2025-01-15T10:30:00Z"},
  {"id": "...", "task_id": "a1b2c3d4-...", "status": "running",   "occurred_at": "2025-01-15T10:30:01Z"},
  {"id": "...", "task_id": "a1b2c3d4-...", "status": "succeeded", "occurred_at": "2025-01-15T10:30:05Z"}
]
```

### Health check

```bash
curl -s http://localhost:8080/health
```

Response (200 OK):

```json
{"status": "ok"}
```

### Error responses

All errors return a structured JSON body with a machine-readable code and a human-readable message:

```json
{"code": "TASK_NOT_FOUND", "message": "task not found"}
```

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `INVALID_JSON_BODY` | 400 | Request body is not valid JSON |
| `NAME_REQUIRED` | 400 | Missing `name` field |
| `TYPE_REQUIRED` | 400 | Missing `type` field |
| `PAYLOAD_NULL` | 400 | `payload` field is null |
| `UNKNOWN_TASK_TYPE` | 400 | Task type not registered |
| `INVALID_TASK_ID` | 400 | Task ID is not a valid UUID |
| `INVALID_DLQ_TASK_ID` | 400 | DLQ task ID is not a valid UUID |
| `TASK_NOT_FOUND` | 404 | No task with this ID |
| `DLQ_TASK_NOT_FOUND` | 404 | No DLQ entry with this ID |
| `INTERNAL_ERROR` | 500 | Unexpected server error |

## Dead-Letter Queue (DLQ)

Tasks that exhaust all retries are automatically moved to the dead-letter queue. The original task is preserved in the `tasks` table with status `"dead_lettered"` to maintain the full audit trail. A corresponding entry is created in the `dead_letter_queue` table with the task snapshot and the error message from the final failure.

### List DLQ entries

```bash
curl -s http://localhost:8080/dlq/tasks
```

Response (200 OK):

```json
[
  {
    "id": "f1e2d3c4-b5a6-7890-abcd-ef1234567890",
    "original_task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "name": "welcome-email",
    "type": "send_email",
    "payload": {"to": "alice@example.com", "subject": "Welcome!"},
    "priority": 10,
    "retry_count": 3,
    "max_retries": 3,
    "timeout_seconds": 60,
    "error_message": "connection refused",
    "original_created_at": "2025-01-15T10:30:00Z",
    "dead_lettered_at": "2025-01-15T10:35:00Z"
  }
]
```

Returns an empty JSON array `[]` when no tasks are in the DLQ.

### Get a single DLQ entry

```bash
curl -s http://localhost:8080/dlq/tasks/f1e2d3c4-b5a6-7890-abcd-ef1234567890
```

Response (200 OK):

```json
{
  "id": "f1e2d3c4-b5a6-7890-abcd-ef1234567890",
  "original_task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "name": "welcome-email",
  "type": "send_email",
  "payload": {"to": "alice@example.com", "subject": "Welcome!"},
  "priority": 10,
  "retry_count": 3,
  "max_retries": 3,
  "timeout_seconds": 60,
  "error_message": "connection refused",
  "original_created_at": "2025-01-15T10:30:00Z",
  "dead_lettered_at": "2025-01-15T10:35:00Z"
}
```

Returns 404 if the DLQ entry does not exist:

```json
{"code": "DLQ_TASK_NOT_FOUND", "message": "dlq task not found"}
```

### Retry a dead-lettered task

Moves the task back to the main queue with `status="queued"` and `retry_count=0`. The DLQ entry is deleted and the original task becomes eligible for processing again.

```bash
curl -s -X POST http://localhost:8080/dlq/tasks/f1e2d3c4-b5a6-7890-abcd-ef1234567890/retry
```

Response (200 OK):

```json
{"id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "status": "queued"}
```

Returns 404 if the DLQ entry does not exist:

```json
{"code": "DLQ_TASK_NOT_FOUND", "message": "dlq task not found"}
```

**Note:** Changing the Kind port mapping (e.g., after a fresh clone) requires `make clean && make all` to recreate the cluster.

## Grafana Dashboard

A pre-built Grafana dashboard is deployed automatically and accessible at `http://localhost:3000` (anonymous auth, no login required). It provides a "Task Manager" dashboard with the following panels:

| Panel | Description |
|-------|-------------|
| HTTP Request Rate | `rate(http_requests_total[1m])` by method and path |
| HTTP Latency p95 | 95th percentile request duration by path |
| Tasks Created | Task creation rate by type |
| Worker Tasks Processed | Processing rate by type and status |
| Tasks In Progress | Current gauge of executing tasks |
| Task Duration p95 | 95th percentile task execution time by type |
| DLQ Tasks | Current number of tasks in the dead-letter queue |

The dashboard auto-refreshes every 10 seconds and uses Prometheus as its datasource.

## Running Tests

Unit tests (no external dependencies):

```bash
go test ./...
```

Integration tests (requires Docker for testcontainers-go):

```bash
go test -tags integration ./internal/task/ -v
```

Run everything:

```bash
go test -tags integration ./... -v
```

## Local Kubernetes Deployment

```bash
make all       # create cluster, build, deploy PostgreSQL, migrate, deploy app
make clean     # delete the kind cluster entirely
```

Individual targets are available for partial operations:

```bash
make kind-up      # create the kind cluster only
make build        # docker build + load into kind
make deploy-pg    # install PostgreSQL via Helm
make migrate      # apply SQL migrations
make deploy-app        # apply k8s manifests
make deploy-prometheus # deploy Prometheus server
make deploy-grafana    # deploy Grafana dashboard
make kind-down         # delete the kind cluster
```

### Scaled deployment (Kafka)

```bash
make deploy-all-scaled   # full scaled k8s deploy (kind + pg + kafka + pgbouncer + api + worker)
```

This deploys the complete scaled architecture:

| Component | Details |
|-----------|---------|
| **kind cluster** | 1 control-plane + 3 worker nodes |
| **PostgreSQL** | Tuned: 300 max_connections, async commit, 512MB shared_buffers |
| **Strimzi Kafka** | 3 brokers (KRaft mode), 128 partitions on `tasks` topic |
| **PgBouncer** | Transaction pooling, 200 pool size, 10K max clients |
| **API pods** | 2 replicas (HPA up to 10), Kafka producer |
| **Worker pods** | 2 replicas (KEDA up to 20), 256 goroutines each, buffered history |
| **Prometheus + Grafana** | Metrics and dashboards |

After deployment, port-forward and test:

```bash
kubectl port-forward svc/task-manager-api 8080:80
go run ./cmd/stress -tasks 500000 -concurrency 200 -type noop -skip-monitor -base-url http://localhost:8080
```

## Scaling

To move from single-binary mode to the scaled architecture:

1. **Set `KAFKA_BROKERS`** to your Kafka bootstrap servers. This switches the queue backend from PostgreSQL polling to Kafka produce/consume.
2. **Deploy separate API and worker pods** using `Dockerfile.api` and `Dockerfile.worker`. The API pods handle HTTP traffic and produce to Kafka; worker pods consume from Kafka and execute tasks.
3. **KEDA auto-scales workers** based on Kafka consumer group lag. When the lag on `KAFKA_TOPIC` exceeds the threshold, KEDA adds worker pods. When lag drops to zero, it scales back down.
4. **PgBouncer pools database connections** between the worker fleet and PostgreSQL, preventing connection exhaustion as workers scale out.
5. **Set `HISTORY_MODE=buffered`** (or `kafka`) for high-throughput history writes. The buffered writer batches inserts using `pgx.CopyFrom` instead of one INSERT per status change.

## Graceful Shutdown

When the process receives SIGTERM (e.g., during a Kubernetes rolling update), it runs a three-phase drain sequence to avoid dropping in-flight requests:

```
SIGTERM → /ready returns 503 → sleep(DRAIN_SECONDS) → srv.Shutdown → wp.WaitWithTimeout → exit
```

| Phase | What happens | Duration |
|-------|-------------|----------|
| **Drain window** | `/ready` flips to 503, Kubernetes stops routing new traffic. The HTTP listener remains open so in-flight requests complete. | `DRAIN_SECONDS` (default 5s) |
| **HTTP shutdown** | `srv.Shutdown` stops accepting new connections and waits for in-flight requests to finish. | `SHUTDOWN_TIMEOUT_SECONDS` (default 10s) |
| **Worker wait** | `WaitWithTimeout` waits for in-flight task executions to complete, with a bounded timeout to avoid hanging. | `WORKER_WAIT_TIMEOUT_SECONDS` (default 30s) |

The Kubernetes deployment sets `terminationGracePeriodSeconds: 50` (5 + 10 + 30 + 5s buffer) to accommodate the full sequence. The liveness probe (`/health`) continues returning 200 throughout shutdown so Kubernetes doesn't SIGKILL the pod prematurely.

## Stress Test / Benchmarks

The included stress tool (`cmd/stress`) supports high-throughput testing with rate limiting, live reporting, and configurable concurrency.

### Results on local kind cluster (single laptop, 4 Docker nodes)

| Metric | Single binary (before) | Scaled mode (after) |
|--------|----------------------|---------------------|
| **Throughput** | 238 tasks/sec | **2,180 tasks/sec** |
| **Error rate** | 89.5% | **0.01%** |
| **p50 creation latency** | 2,324ms | **83ms** |
| **p99 creation latency** | 21,689ms | **224ms** |
| **Tasks created (500K run)** | N/A | **499,932 / 500,000** |

### Running the stress test

```bash
# Basic (against local server)
go run ./cmd/stress -tasks 50000 -concurrency 200 -type noop -skip-monitor

# Against kind cluster (requires port-forward)
kubectl port-forward svc/task-manager-api 8080:80
go run ./cmd/stress -tasks 500000 -concurrency 200 -type noop -skip-monitor -base-url http://localhost:8080

# Rate-limited with monitoring
go run ./cmd/stress -tasks 10000 -concurrency 100 -target-rate 500 -type send_email

# Duration-based (unlimited tasks)
go run ./cmd/stress -tasks 0 -duration 1m -concurrency 500 -type noop -skip-monitor
```

### Stress tool flags

| Flag | Default | Description |
|------|---------|-------------|
| `-tasks` | 500 | Total tasks to create (0 = unlimited, use with `-duration`) |
| `-concurrency` | 50 | Number of goroutines creating tasks |
| `-type` | `send_email` | Task type: `send_email`, `run_query`, `noop`, or `mixed` |
| `-target-rate` | 0 | Target tasks/sec (0 = unlimited) |
| `-duration` | 0 | Run for this long then stop |
| `-skip-monitor` | false | Skip completion monitoring (pure throughput test) |
| `-connections` | concurrency+10 | Max HTTP connections |
| `-base-url` | `http://localhost:8080` | API base URL |
| `-live-interval` | 2s | Live throughput reporting interval |

## Design Decisions

### Why PostgreSQL as a queue

The most common approach for background task processing is a dedicated message broker -- Redis with Celery, RabbitMQ with its DLX exchanges, or SQS in AWS. Each of those adds an operational dependency: another process to monitor, another failure mode to handle, another data store whose state can diverge from your database.

This project uses PostgreSQL itself as the queue. The tradeoff is lower theoretical throughput (a SQL database isn't optimized for message dispatch), but in exchange you get:

- **Transactional consistency.** When a task is created, its initial `"queued"` history row is inserted in the same transaction. When a worker claims a task, the status update and the history row are atomic. There's no window where the task table says one thing and the history table says another. With a separate broker, you'd need to coordinate writes across two systems (the database and the broker), which introduces failure modes like "task exists in the database but the broker message was lost."

- **One operational dependency.** The application needs PostgreSQL and nothing else. No Redis cluster to size, no RabbitMQ federation to configure, no broker failover to test. For a team that already runs PostgreSQL, this is zero additional infrastructure.

- **`SELECT FOR UPDATE SKIP LOCKED`.** This PostgreSQL feature is what makes the pattern viable. `FOR UPDATE` row-locks the selected task so no other transaction can claim it. `SKIP LOCKED` tells concurrent workers to skip already-locked rows instead of blocking. Together they provide non-blocking, contention-free task dispatch without any application-level locking. Each worker gets a different task, guaranteed by the database engine.

- **Partial indexes keep the hot path fast.** The `tasks` table will accumulate millions of succeeded rows over time, but the claim query only looks at `status = 'queued'` rows. The partial index `idx_tasks_claim` (`WHERE status = 'queued'`) means the index stays small regardless of total table size. The same pattern applies to the reaper's `idx_tasks_stuck` (`WHERE status = 'running'`). This is a PostgreSQL-specific feature that wouldn't be available with most brokers.

The ceiling for this approach is roughly 1,000-5,000 tasks/second depending on hardware and row size. Beyond that, a dedicated broker becomes necessary -- which is why the system also supports **Kafka as the queue backend** (set `KAFKA_BROKERS`). The PostgreSQL mode remains the default for simplicity; Kafka mode is for deployments targeting tens of thousands to millions of tasks per second.

### Why a single binary

The API server and the worker pool run in the same Go process, sharing a `pgxpool.Pool` connection pool. This is a deliberate choice:

- **Deployment simplicity.** One container image, one Kubernetes Deployment, one set of health probes. No need to coordinate separate API and worker deployments, no version skew between them, no separate scaling policies to manage.

- **Horizontal scaling still works.** Running multiple replicas of this binary is safe because `SKIP LOCKED` prevents double-processing at the database level. Each replica runs its own pool of workers, and they all contend fairly for queued tasks. You scale by increasing replica count, not by splitting into microservices.

- **Shared connection pool.** The API and workers share the same `pgxpool.Pool`, which means fewer total database connections and better connection utilization. A separate worker process would need its own pool, doubling the connection count for no benefit at this scale.

The tradeoff: you can't scale the API and workers independently. For workloads where this matters, the system provides separate `cmd/api` and `cmd/worker` binaries that can be deployed and scaled independently via Kubernetes HPA (API) and KEDA (workers, based on Kafka consumer lag).

### Why the Repository interface

All database access goes through the `task.Repository` interface -- 11 methods covering CRUD, claiming, requeueing, DLQ operations, and the reaper. This is not abstraction for its own sake:

- **Testability.** The HTTP handler tests (`handler_test.go`) use a `mockRepository` that returns canned data. No database needed, tests run in milliseconds. The worker pool could be tested the same way. Without the interface, every test would need a real PostgreSQL instance.

- **Single implementation is fine.** There's only one implementation (`PostgresRepository`), and that's okay. The interface exists for the test seam, not because we expect to swap databases. YAGNI applies to the "what if we switch to MySQL" scenario, but it doesn't apply to "I need to test my handlers without a database."

### Why no ORM

The project uses raw SQL via pgx/v5 instead of GORM, sqlc, or any other abstraction layer. The reasons:

- **PostgreSQL-specific features.** `SELECT FOR UPDATE SKIP LOCKED`, `make_interval(secs => ...)`, partial indexes, and `INSERT ... RETURNING` are all PostgreSQL-specific. An ORM would either not support them or require dropping to raw SQL anyway, giving you the worst of both worlds.

- **Explicit queries.** Every SQL statement is visible in the code. When debugging a performance issue, you don't need to reverse-engineer what the ORM generated. When reviewing a PR, the reviewer sees the exact query that will hit the database.

- **pgx/v5 is already ergonomic.** `QueryRow`, `Scan`, `Exec`, and `RowsAffected` cover everything this project needs. The `scanTask` helper and `taskColumns` constant keep the boilerplate manageable. Adding an ORM would not meaningfully reduce code volume, but it would add a dependency and an abstraction layer to learn.

### Why a separate dead-letter queue table

When a task exhausts all retries, it could simply be marked `status = 'failed'` in the `tasks` table. Instead, this project copies the task snapshot into a separate `dead_letter_queue` table. The reasoning:

- **Separation of concerns.** The `tasks` table is the hot path -- workers poll it constantly. Keeping permanently failed tasks out of it means fewer rows for the claim query to skip, even with partial indexes.

- **Error snapshot.** The DLQ entry captures the task's state at the moment of failure, including the final error message. If the original task is later retried and succeeds, the DLQ history is preserved separately. The original task also retains `status = "dead_lettered"` for audit trail continuity.

- **Independent retention.** You might want to keep DLQ entries for 90 days (for debugging) but purge succeeded tasks after 7 days. Separate tables make this trivial with different cleanup policies.

- **Explicit retry semantics.** `POST /dlq/tasks/{id}/retry` resets the original task to `"queued"` with `retry_count = 0` and deletes the DLQ row, all in one transaction. This is a conscious operator action, not an automatic retry -- the system distinguishes between "the task failed transiently" (automatic retry with backoff) and "an operator reviewed the failure and wants to try again" (DLQ retry with no backoff).

### Other design choices

- **History as audit trail.** Every status transition inserts a row in `task_history` within the same transaction that updates the task. This guarantees a complete, ordered audit trail with no gaps. If the application crashes between the UPDATE and the INSERT, both are rolled back.

- **Exponential backoff for retries.** Requeued tasks use `run_after = now() + 5s * 2^retry_count` (capped at 5 minutes), preventing thundering-herd retries when a downstream service is temporarily down. DLQ retries and reaper requeues set `run_after = now()` because they represent different failure modes (operator action and dead workers, respectively) where a delay would be counterproductive.

- **Stuck task reaper.** A single background goroutine scans for tasks stuck in `"running"` beyond their `timeout_seconds`. Each stuck task is processed in its own transaction so one failure doesn't block the rest. The reaper doesn't use backoff for requeues because the failure was a dead worker, not a task-level error. A race condition guard (`WHERE status = 'running'` in the UPDATE) ensures that if a worker legitimately completes a task between the reaper's SELECT and UPDATE, the requeue is silently skipped.

- **Opt-in idempotency keys.** The `POST /tasks` endpoint accepts an optional `idempotency_key` for callers that need exactly-once semantics (e.g., webhook handlers that might be retried). Callers that genuinely want multiple tasks (e.g., cron jobs, batch operations) simply omit the field. Enforced by a PostgreSQL partial unique index (`WHERE idempotency_key IS NOT NULL`) so tasks without keys incur zero indexing overhead. The key is nullable -- not mandatory -- because forcing every caller to generate a unique key they don't care about would be pointless ceremony.

- **No globals -- dependency injection via structs.** Every component receives its dependencies through constructor arguments (`NewHandler(repo, registry, drain)`, `NewPool(repo, registry, concurrency, reapInterval)`). There's no global state, no service locator, no init-time side effects beyond Prometheus metric registration. This makes the dependency graph explicit and every component independently testable.

## What I Would Test Further

- **Concurrent claim contention.** Spin up multiple goroutines calling `ClaimTask` simultaneously against the same queued tasks to verify that `SKIP LOCKED` prevents double-processing.
- **Context cancellation in workers.** Verify that a cancelled context during task execution propagates correctly and that the task is marked as failed (not left in "running").
- **Retry accounting.** End-to-end test that a failing task is requeued up to `max_retries` times, then moved to the dead-letter queue, with the correct `retry_count` at each step.
- **Timeout enforcement.** Test that a task exceeding `timeout_seconds` is cancelled and its status transitions correctly.
- **Malicious payloads.** Fuzz the API with oversized JSON, deeply nested objects, and non-UTF8 strings.
- **Metrics correctness.** Assert that Prometheus counters and histograms are incremented with the correct labels after task creation and execution.
- **DLQ retry-then-fail cycle.** Retry a DLQ task, let it fail again through all retries, and verify it re-enters the DLQ with updated error information and a fresh history trail.
- **Concurrent DLQ retry.** Two callers simultaneously POST `/dlq/tasks/{id}/retry` for the same entry. Only one should succeed; the other should get a 404.
- **Reaper correctness.** Create a task with a short timeout, simulate a dead worker (leave the task in `"running"` past its timeout), and verify the reaper requeues it with `run_after = now()` and increments `retry_count`.
- **Reaper race condition.** Verify that a task completed by a worker between the reaper's SELECT and UPDATE is handled gracefully (the reaper's `WHERE status = 'running'` guard should make the UPDATE a no-op).
- **Idempotency key deduplication.** Send two concurrent `POST /tasks` requests with the same idempotency key. The first should return 201, the second should return 200 with the same task ID.
- **Idempotency key null handling.** Verify that tasks without idempotency keys are never subject to uniqueness constraints (multiple tasks with NULL keys should coexist).

## What's Left to Do

Many items from the original roadmap have been implemented (distributed tracing, HPA, table partitioning, Kafka integration, batch endpoint, connection pooling, multi-node deployment). The remaining gaps:

### API features

- **Task cancellation.** `POST /tasks/{id}/cancel` with context propagation to running workers via a per-task cancel map.
- **Delayed scheduling.** Expose `run_after` in `CreateTaskRequest` for future-scheduled tasks.
- **Task results.** A `result JSONB` column so callers can retrieve output from completed tasks.
- **Pagination.** Cursor-based pagination on history and DLQ list endpoints.
- **Authentication and rate limiting.** JWT/API key validation and per-client rate limiting.

### Operations

- **TTL-based cleanup.** Background job to purge old succeeded tasks and DLQ entries.
- **DLQ alerting.** Webhook/email when DLQ depth exceeds a threshold.
- **Alerting rules.** Prometheus alerts for DLQ depth, worker saturation, reaper activity, HTTP error rate.
- **Network policies.** Restrict pod ingress/egress to only required services.
- **CI pipeline.** GitHub Actions for `go vet`, `golangci-lint`, unit tests, integration tests, and Docker builds.

### Code

- **Migration runner.** Embed `golang-migrate/migrate` in startup instead of manual `kubectl exec psql`.
- **Configuration validation.** Fail fast on malformed env vars instead of silently falling back to defaults.
- **Protobuf serialization.** Replace JSON queue serialization with protobuf for lower latency at 1M/s.
- **Chaos testing.** Kill worker pods mid-task and verify reaper recovery with `chaos-mesh`.
