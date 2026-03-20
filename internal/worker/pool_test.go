package worker

import (
	"sync"
	"testing"
	"time"
)

func TestWaitWithTimeout_CompletesBeforeTimeout(t *testing.T) {
	t.Parallel()

	p := &Pool{}
	// No workers started, so wg is zero — WaitWithTimeout should return immediately.
	if !p.WaitWithTimeout(1 * time.Second) {
		t.Fatal("expected WaitWithTimeout to return true (all done), got false (timeout)")
	}
}

func TestWaitWithTimeout_TimesOut(t *testing.T) {
	t.Parallel()

	p := &Pool{wg: sync.WaitGroup{}}
	p.wg.Add(1) // simulate a stuck worker that never calls Done

	start := time.Now()
	result := p.WaitWithTimeout(100 * time.Millisecond)
	elapsed := time.Since(start)

	if result {
		t.Fatal("expected WaitWithTimeout to return false (timeout), got true")
	}
	if elapsed < 90*time.Millisecond {
		t.Fatalf("expected at least ~100ms wait, got %s", elapsed)
	}

	// Clean up so the test doesn't leak.
	p.wg.Done()
}

// ---------------------------------------------------------------------------
// isTerminal
// ---------------------------------------------------------------------------

func TestIsTerminal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status string
		want   bool
	}{
		{"succeeded", true},
		{"failed", true},
		{"dead_lettered", true},
		{"queued", false},
		{"running", false},
		{"", false},
	}

	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			t.Parallel()
			got := isTerminal(tc.status)
			if got != tc.want {
				t.Errorf("isTerminal(%q) = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}
