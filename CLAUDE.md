# Background Task Management System

## Project
Go background task management system with PostgreSQL as queue backend, deployed on local Kubernetes via kind.

## Architecture
- Single binary: API server + worker pool in same process via goroutines
- PostgreSQL task queue using SELECT FOR UPDATE SKIP LOCKED
- Module: github.com/giulio/task-manager

## Conventions
- Go 1.23+
- Logging: slog (structured JSON)
- HTTP router: chi
- Database: pgx/v5 (no ORM)
- Config: environment variables only
- Errors: return JSON {"code": "SCREAMING_SNAKE", "message": "human-readable"} with appropriate HTTP status
- No globals — dependency injection via structs
- Context propagation everywhere

## Project Structure
```
cmd/server/main.go          # entrypoint, wires everything
internal/api/               # HTTP handlers and routing
internal/task/              # models, repository, registry
internal/worker/            # worker pool
pkg/email/email.go          # send_email task
pkg/query/query.go          # run_query task
migrations/                 # SQL migrations
deploy/                     # k8s manifests, kind config, helm values
```

## Commands
```bash
go build ./...              # compile
go test ./...               # run tests
make all                    # full local k8s deploy (kind + pg + app)
make clean                  # tear down
```

## Rules
- Do not modify files outside your assigned scope when working as a sub-agent
- Every task status change must insert a history row in the same transaction
- All task functions must implement TaskFunc: func(ctx context.Context, params map[string]string) error
- Respect context cancellation in all task functions

## Recurring Agent
After each phase, run the Go Guide agent to update docs/go-guide-for-pythonistas.md
and README.md with patterns from the latest code changes.
