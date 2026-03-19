package query

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"
)

// RunQuery simulates executing a database query. It logs the received
// parameters, waits for 3 seconds (or until the context is cancelled),
// and then fails roughly 20% of the time to simulate transient errors.
// The function signature matches task.TaskFunc.
func RunQuery(ctx context.Context, params map[string]string) error {
	slog.InfoContext(ctx, "executing query", "query", params["query"])

	select {
	case <-ctx.Done():
		slog.WarnContext(ctx, "run query cancelled")
		return ctx.Err()
	case <-time.After(3 * time.Second):
	}

	if rand.Float64() < 0.2 {
		slog.ErrorContext(ctx, "query execution failed: simulated random failure")
		return errors.New("query execution failed: simulated random failure")
	}

	slog.InfoContext(ctx, "query executed successfully")
	return nil
}
