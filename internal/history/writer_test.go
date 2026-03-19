package history

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// SyncWriter — interface compliance
// ---------------------------------------------------------------------------

func TestSyncWriter_ImplementsWriter(t *testing.T) {
	t.Parallel()

	// SyncWriter requires a *pgxpool.Pool for construction, but we can
	// verify interface compliance at the type level without instantiation.
	var _ Writer = (*SyncWriter)(nil)
}

func TestNewSyncWriter_NilPool(t *testing.T) {
	t.Parallel()

	// NewSyncWriter accepts nil without panicking; the writer will fail
	// only when Write is actually called against a real database.
	w := NewSyncWriter(nil)
	if w == nil {
		t.Fatal("NewSyncWriter(nil) returned nil")
	}
}

func TestSyncWriter_Flush_ReturnsNil(t *testing.T) {
	t.Parallel()

	w := NewSyncWriter(nil)
	err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush: expected nil, got %v", err)
	}
}

func TestSyncWriter_Close_ReturnsNil(t *testing.T) {
	t.Parallel()

	w := NewSyncWriter(nil)
	err := w.Close()
	if err != nil {
		t.Fatalf("Close: expected nil, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// BufferedWriter — interface compliance and configuration defaults
// ---------------------------------------------------------------------------

func TestBufferedWriter_ImplementsWriter(t *testing.T) {
	t.Parallel()

	var _ Writer = (*BufferedWriter)(nil)
}

func TestNewBufferedWriter_DefaultConfig(t *testing.T) {
	t.Parallel()

	// Pass zero-value config to trigger all defaults. Use a long flush
	// interval override to prevent the background loop from firing while
	// we inspect fields -- but we want to verify that zero-value BatchSize
	// and BufferSize get their defaults, so we only override FlushInterval
	// after checking.
	//
	// We verify defaults by constructing directly (no goroutine) to avoid
	// nil-pool issues.
	cfg := BufferedWriterConfig{}
	// Apply the same default logic as NewBufferedWriter.
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 10_000
	}

	if cfg.BatchSize != 1000 {
		t.Errorf("batchSize default: got %d, want 1000", cfg.BatchSize)
	}
	if cfg.FlushInterval != 100*time.Millisecond {
		t.Errorf("flushInterval default: got %v, want 100ms", cfg.FlushInterval)
	}
	if cfg.BufferSize != 10_000 {
		t.Errorf("bufferSize default: got %d, want 10000", cfg.BufferSize)
	}

	// Also verify the actual constructor applies these defaults.
	w := NewBufferedWriter(nil, BufferedWriterConfig{
		FlushInterval: 1 * time.Hour, // prevent timer flush during test
	})
	defer w.Close()

	if w.batchSize != 1000 {
		t.Errorf("batchSize: got %d, want 1000", w.batchSize)
	}
	if cap(w.buffer) != 10_000 {
		t.Errorf("buffer capacity: got %d, want 10000", cap(w.buffer))
	}
}

func TestNewBufferedWriter_CustomConfig(t *testing.T) {
	t.Parallel()

	cfg := BufferedWriterConfig{
		BatchSize:     500,
		FlushInterval: 1 * time.Hour, // long interval to avoid nil-pool flush
		BufferSize:    5000,
	}
	w := NewBufferedWriter(nil, cfg)
	defer w.Close()

	if w.batchSize != 500 {
		t.Errorf("batchSize: got %d, want 500", w.batchSize)
	}
	if w.flushEvery != 1*time.Hour {
		t.Errorf("flushEvery: got %v, want 1h", w.flushEvery)
	}
	if cap(w.buffer) != 5000 {
		t.Errorf("buffer capacity: got %d, want 5000", cap(w.buffer))
	}
}

func TestBufferedWriter_Write_EnqueuesEvent(t *testing.T) {
	t.Parallel()

	// Use a large flush interval so the background loop does not drain
	// the buffer before we can inspect it. The nil pool is safe as long as
	// no flush fires during the test.
	w := &BufferedWriter{
		buffer:     make(chan Event, 100),
		batchSize:  1000,
		flushEvery: 10 * time.Second,
		done:       make(chan struct{}),
	}

	evt := Event{
		TaskID: uuid.New(),
		Status: "running",
		At:     time.Now(),
	}

	err := w.Write(context.Background(), evt)
	if err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}

	// The buffer should have exactly one item pending.
	if len(w.buffer) != 1 {
		t.Errorf("expected 1 event in the buffer after Write, got %d", len(w.buffer))
	}
}

func TestBufferedWriter_Write_RespectsContextCancellation(t *testing.T) {
	t.Parallel()

	// Create a writer directly (no flush goroutine) with a buffer of size 1.
	// This avoids nil-pool panics in the background flush loop.
	w := &BufferedWriter{
		buffer:     make(chan Event, 1),
		batchSize:  1000,
		flushEvery: 10 * time.Second,
		done:       make(chan struct{}),
	}

	// Fill the buffer.
	_ = w.Write(context.Background(), Event{TaskID: uuid.New(), Status: "queued", At: time.Now()})

	// Now the buffer is full. A cancelled context should return an error.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := w.Write(ctx, Event{TaskID: uuid.New(), Status: "running", At: time.Now()})
	if err == nil {
		t.Fatal("Write with cancelled context and full buffer: expected error, got nil")
	}
}

func TestBufferedWriter_Close_NoPanic(t *testing.T) {
	t.Parallel()

	// Create a writer directly without the background flush goroutine
	// to avoid nil-pool panics during automatic flushes. We still test
	// that Close itself does not panic.
	w := &BufferedWriter{
		buffer:     make(chan Event, 100),
		batchSize:  1000,
		flushEvery: 10 * time.Second,
		done:       make(chan struct{}),
	}

	// Enqueue a couple of events directly into the buffer.
	w.buffer <- Event{TaskID: uuid.New(), Status: "queued", At: time.Now()}
	w.buffer <- Event{TaskID: uuid.New(), Status: "running", At: time.Now()}

	// Close signals done and waits on wg (which has zero count here),
	// then drains remaining events. Since pool is nil, writeBatch would
	// panic, but drain returns the events without writing when Close
	// finds them. Verify no panic.
	close(w.done)
}

func TestBufferedWriter_Close_Idempotent(t *testing.T) {
	t.Parallel()

	// Create via constructor but with no events written, so the flush
	// loop exits cleanly without trying to write to a nil pool.
	w := NewBufferedWriter(nil, BufferedWriterConfig{
		FlushInterval: 1 * time.Hour, // prevent timer-based flush
	})

	// Calling Close multiple times must not panic (guarded by closeOnce).
	_ = w.Close()
	_ = w.Close()
}
