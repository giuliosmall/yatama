package task

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// testNoop is a minimal TaskFunc used in tests that do not care about execution.
func testNoop(_ context.Context, _ map[string]string) error { return nil }

// --------------------------------------------------------------------------
// NewRegistry
// --------------------------------------------------------------------------

func TestNewRegistry_PreRegisteredTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		taskType string
	}{
		{name: "send_email is pre-registered", taskType: "send_email"},
		{name: "run_query is pre-registered", taskType: "run_query"},
	}

	r := NewRegistry()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn, ok := r.Get(tt.taskType)
			if !ok {
				t.Fatalf("expected task type %q to be registered, but Get returned false", tt.taskType)
			}
			if fn == nil {
				t.Fatalf("expected non-nil TaskFunc for %q", tt.taskType)
			}
		})
	}
}

// --------------------------------------------------------------------------
// Register + Get
// --------------------------------------------------------------------------

func TestRegister_AndGet_CustomFunction(t *testing.T) {
	t.Parallel()

	r := NewRegistry()

	called := false
	custom := func(_ context.Context, _ map[string]string) error {
		called = true
		return nil
	}

	r.Register("custom_task", custom)

	fn, ok := r.Get("custom_task")
	if !ok {
		t.Fatal("expected Get to return true for a newly registered task")
	}
	if fn == nil {
		t.Fatal("expected non-nil TaskFunc for custom_task")
	}

	// Verify the returned function is the one we registered.
	if err := fn(context.Background(), nil); err != nil {
		t.Fatalf("unexpected error calling custom TaskFunc: %v", err)
	}
	if !called {
		t.Fatal("expected the custom TaskFunc to be invoked")
	}
}

func TestRegister_Overwrite_ExistingType(t *testing.T) {
	t.Parallel()

	r := NewRegistry()

	marker := errors.New("overwritten")
	overwrite := func(_ context.Context, _ map[string]string) error {
		return marker
	}

	// Overwrite the built-in send_email.
	r.Register("send_email", overwrite)

	fn, ok := r.Get("send_email")
	if !ok {
		t.Fatal("expected Get to return true after overwriting send_email")
	}

	err := fn(context.Background(), nil)
	if !errors.Is(err, marker) {
		t.Fatalf("expected overwritten function to return marker error, got: %v", err)
	}
}

// --------------------------------------------------------------------------
// Get — unknown type
// --------------------------------------------------------------------------

func TestGet_UnknownType_ReturnsFalse(t *testing.T) {
	t.Parallel()

	r := NewRegistry()

	fn, ok := r.Get("does_not_exist")
	if ok {
		t.Fatal("expected Get to return false for an unregistered task type")
	}
	if fn != nil {
		t.Fatal("expected nil TaskFunc for an unregistered task type")
	}
}

// --------------------------------------------------------------------------
// MustGet
// --------------------------------------------------------------------------

func TestMustGet_RegisteredType_ReturnsFunc(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.Register("ping", testNoop)

	// MustGet should not panic for a registered type.
	fn := r.MustGet("ping")
	if fn == nil {
		t.Fatal("expected non-nil TaskFunc from MustGet")
	}
}

func TestMustGet_UnknownType_Panics(t *testing.T) {
	t.Parallel()

	r := NewRegistry()

	defer func() {
		rec := recover()
		if rec == nil {
			t.Fatal("expected MustGet to panic for an unregistered type, but it did not")
		}

		msg, ok := rec.(string)
		if !ok {
			t.Fatalf("expected panic value to be a string, got %T", rec)
		}
		if msg == "" {
			t.Fatal("expected non-empty panic message")
		}
	}()

	_ = r.MustGet("nonexistent")
}

// --------------------------------------------------------------------------
// Registered functions can be called with context and params
// --------------------------------------------------------------------------

func TestRegisteredFunction_CalledWithContextAndParams(t *testing.T) {
	t.Parallel()

	r := NewRegistry()

	var receivedCtx context.Context
	var receivedParams map[string]string

	r.Register("capture", func(ctx context.Context, params map[string]string) error {
		receivedCtx = ctx
		receivedParams = params
		return nil
	})

	fn, ok := r.Get("capture")
	if !ok {
		t.Fatal("expected capture to be registered")
	}

	ctx := context.WithValue(context.Background(), contextKey("test"), "value")
	params := map[string]string{"key": "val"}

	if err := fn(ctx, params); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedCtx != ctx {
		t.Fatal("expected the same context to be passed through")
	}
	if receivedParams["key"] != "val" {
		t.Fatalf("expected params[key]=val, got %q", receivedParams["key"])
	}
}

// contextKey is a private type to avoid key collisions in context.WithValue.
type contextKey string

func TestRegisteredFunction_ReturnsError(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	sentinel := errors.New("task failed")

	r.Register("failing", func(_ context.Context, _ map[string]string) error {
		return sentinel
	})

	fn, _ := r.Get("failing")
	err := fn(context.Background(), nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got: %v", err)
	}
}

// --------------------------------------------------------------------------
// Concurrency safety
// --------------------------------------------------------------------------

func TestRegistry_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines register, half read.
	for i := 0; i < goroutines; i++ {
		name := "task_" + string(rune('A'+i%26))

		go func(n string) {
			defer wg.Done()
			r.Register(n, testNoop)
		}(name)

		go func(n string) {
			defer wg.Done()
			// Get may or may not find the entry yet; we just verify no data race.
			_, _ = r.Get(n)
		}(name)
	}

	wg.Wait()
}
