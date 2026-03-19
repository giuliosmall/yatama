// Package noop provides a no-op task function for throughput testing.
// Unlike send_email (3s sleep) or run_query (2s sleep), this returns
// immediately so stress tests measure the queue infrastructure, not
// simulated work.
package noop

import "context"

// Noop is a task function that returns immediately with no error.
func Noop(_ context.Context, _ map[string]string) error {
	return nil
}
