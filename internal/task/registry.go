package task

import (
	"context"
	"fmt"
	"sync"

	"github.com/giulio/task-manager/pkg/email"
	"github.com/giulio/task-manager/pkg/query"
)

// TaskFunc is the signature that all executable task functions must implement.
// It receives a context (which may carry a deadline or cancellation signal)
// and a string-keyed parameter map supplied by the task's payload.
type TaskFunc func(ctx context.Context, params map[string]string) error

// Registry holds a named set of task functions and provides thread-safe
// lookup and registration. Use NewRegistry to obtain a registry that comes
// pre-loaded with the built-in task types (send_email, run_query).
type Registry struct {
	mu    sync.RWMutex
	funcs map[string]TaskFunc
}

// NewRegistry creates a Registry with the default task functions registered.
func NewRegistry() *Registry {
	r := &Registry{
		funcs: make(map[string]TaskFunc),
	}
	r.funcs["send_email"] = email.SendEmail
	r.funcs["run_query"] = query.RunQuery
	return r
}

// Register adds a task function to the registry under the given name.
// If a function is already registered with that name it will be replaced.
func (r *Registry) Register(name string, fn TaskFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.funcs[name] = fn
}

// Get retrieves a task function by name. The boolean return value reports
// whether the name was found; callers should check it before invoking the
// returned function.
func (r *Registry) Get(name string) (TaskFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.funcs[name]
	if !ok {
		return nil, false
	}
	return fn, true
}

// MustGet retrieves a task function by name and panics if it is not found.
// This is a convenience for use during application startup where a missing
// task type indicates a programming error.
func (r *Registry) MustGet(name string) TaskFunc {
	fn, ok := r.Get(name)
	if !ok {
		panic(fmt.Sprintf("task %q not found in registry", name))
	}
	return fn
}
