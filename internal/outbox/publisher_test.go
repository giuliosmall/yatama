package outbox

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/giulio/task-manager/internal/queue"
)

// mockProducer implements queue.Producer for testing without Kafka.
type mockProducer struct {
	enqueued []queue.Message
	mu       sync.Mutex
}

func (m *mockProducer) Enqueue(_ context.Context, msg queue.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enqueued = append(m.enqueued, msg)
	return nil
}

func (m *mockProducer) Flush(_ context.Context) error { return nil }
func (m *mockProducer) Close() error                  { return nil }

// ---------------------------------------------------------------------------
// NewPublisher defaults
// ---------------------------------------------------------------------------

func TestNewPublisher_DefaultConfig(t *testing.T) {
	t.Parallel()

	mp := &mockProducer{}
	p := NewPublisher(nil, mp, Config{}) // zero-value Config

	if p.pollInterval != 100*time.Millisecond {
		t.Errorf("pollInterval: got %v, want 100ms", p.pollInterval)
	}
	if p.batchSize != 500 {
		t.Errorf("batchSize: got %d, want 500", p.batchSize)
	}
	if p.cleanupAge != 1*time.Hour {
		t.Errorf("cleanupAge: got %v, want 1h", p.cleanupAge)
	}
	if p.done == nil {
		t.Error("done channel should be non-nil")
	}
	if p.producer != mp {
		t.Error("producer should be the mock passed in")
	}
}

// ---------------------------------------------------------------------------
// NewPublisher custom config
// ---------------------------------------------------------------------------

func TestNewPublisher_CustomConfig(t *testing.T) {
	t.Parallel()

	cfg := Config{
		PollInterval: 250 * time.Millisecond,
		BatchSize:    42,
		CleanupAge:   30 * time.Minute,
	}
	p := NewPublisher(nil, &mockProducer{}, cfg)

	if p.pollInterval != 250*time.Millisecond {
		t.Errorf("pollInterval: got %v, want 250ms", p.pollInterval)
	}
	if p.batchSize != 42 {
		t.Errorf("batchSize: got %d, want 42", p.batchSize)
	}
	if p.cleanupAge != 30*time.Minute {
		t.Errorf("cleanupAge: got %v, want 30m", p.cleanupAge)
	}
}

// ---------------------------------------------------------------------------
// Start and Close lifecycle
// ---------------------------------------------------------------------------

func TestPublisher_StartAndClose_ViaDone(t *testing.T) {
	t.Parallel()

	mp := &mockProducer{}
	// Use a large poll interval so the ticker never fires before Close
	// sends on the done channel. This avoids a nil-pool panic in poll().
	p := NewPublisher(nil, mp, Config{
		PollInterval: 1 * time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should not panic.
	p.Start(ctx)

	// Close signals the goroutine via the done channel and waits for it.
	if err := p.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	// After Close returns the WaitGroup is at zero. Verify by checking
	// that another Wait completes immediately (no goroutine leak).
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK — no goroutine leak.
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine leak: wg.Wait() did not return after Close()")
	}
}

func TestPublisher_StartAndClose_ViaContext(t *testing.T) {
	t.Parallel()

	mp := &mockProducer{}
	// Use a large poll interval so the ticker never fires before context
	// cancellation. This avoids a nil-pool panic in poll().
	p := NewPublisher(nil, mp, Config{
		PollInterval: 1 * time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Start should not panic.
	p.Start(ctx)

	// Cancel the context — the goroutine should exit via ctx.Done().
	cancel()

	// Wait for the goroutine to finish, with a safety timeout.
	waitDone := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		// OK — goroutine exited cleanly.
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine leak: wg.Wait() did not return after context cancellation")
	}
}
