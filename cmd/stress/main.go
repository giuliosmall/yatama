package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// taskResult holds the outcome of a single POST /tasks call.
type taskResult struct {
	index   int
	id      string
	err     error
	latency time.Duration
}

// taskStatus holds the final observed status and end-to-end latency for a created task.
type taskStatus struct {
	id      string
	status  string
	e2e     time.Duration
	created time.Time
}

// liveStats tracks real-time throughput counters.
type liveStats struct {
	sent    atomic.Int64
	success atomic.Int64
	errors  atomic.Int64
	start   time.Time
}

func main() {
	// ── Flags ─────────────────────────────────────────────────────────────
	concurrency := flag.Int("concurrency", 50, "number of goroutines creating tasks")
	totalTasks := flag.Int("tasks", 500, "total number of tasks to create (0 = unlimited, use with -duration)")
	baseURL := flag.String("base-url", "http://localhost:8080", "base URL of the task manager API")
	taskType := flag.String("type", "send_email", "task type: send_email, run_query, or mixed")
	pollInterval := flag.Duration("poll-interval", 1*time.Second, "how often to poll for task completion")
	timeout := flag.Duration("timeout", 10*time.Minute, "max time to wait for all tasks to complete")

	// High-throughput flags.
	targetRate := flag.Int("target-rate", 0, "target tasks/sec (0 = unlimited, fire as fast as possible)")
	batchSize := flag.Int("batch-size", 1, "number of tasks per HTTP request (>1 uses batched creation)")
	duration := flag.Duration("duration", 0, "run for this long then stop (0 = run until -tasks is reached)")
	skipMonitor := flag.Bool("skip-monitor", false, "skip the completion monitoring phase (for pure throughput testing)")
	liveInterval := flag.Duration("live-interval", 2*time.Second, "interval for live throughput reporting during blast phase")
	connections := flag.Int("connections", 0, "max HTTP connections (0 = concurrency + 10)")

	flag.Parse()

	validTypes := map[string]bool{"send_email": true, "run_query": true, "mixed": true, "noop": true}
	if !validTypes[*taskType] {
		fmt.Fprintf(os.Stderr, "invalid -type %q: must be send_email, run_query, mixed, or noop\n", *taskType)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	maxConns := *concurrency + 10
	if *connections > 0 {
		maxConns = *connections
	}
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxConns,
			MaxIdleConns:        maxConns,
			MaxConnsPerHost:     maxConns,
		},
	}

	fmt.Printf("=== Stress Test Config ===\n")
	fmt.Printf("Base URL:      %s\n", *baseURL)
	fmt.Printf("Tasks:         %d", *totalTasks)
	if *totalTasks == 0 {
		fmt.Print(" (unlimited)")
	}
	fmt.Println()
	fmt.Printf("Concurrency:   %d\n", *concurrency)
	fmt.Printf("Connections:   %d\n", maxConns)
	fmt.Printf("Type:          %s\n", *taskType)
	fmt.Printf("Batch size:    %d\n", *batchSize)
	if *targetRate > 0 {
		fmt.Printf("Target rate:   %d tasks/sec\n", *targetRate)
	} else {
		fmt.Printf("Target rate:   unlimited\n")
	}
	if *duration > 0 {
		fmt.Printf("Duration:      %s\n", *duration)
	}
	fmt.Printf("Skip monitor:  %v\n", *skipMonitor)
	fmt.Println()

	// ── Phase 1: Blast ──────────────────────────────────────────────────

	fmt.Println("Phase 1: Creating tasks...")
	blastStart := time.Now()

	var limiter *rate.Limiter
	if *targetRate > 0 {
		limiter = rate.NewLimiter(rate.Limit(*targetRate), *targetRate/10+1)
	}

	results := blast(ctx, client, *baseURL, *totalTasks, *concurrency, *taskType, *batchSize, limiter, *duration, *liveInterval)
	blastDuration := time.Since(blastStart)

	var created []taskResult
	var errCount int
	for i := range results {
		if results[i].err != nil {
			errCount++
		} else {
			created = append(created, results[i])
		}
	}

	creationRate := float64(len(created)) / blastDuration.Seconds()
	fmt.Printf("\nPhase 1 done: %d created, %d errors in %s (%.0f tasks/sec)\n\n",
		len(created), errCount, blastDuration.Truncate(time.Millisecond), creationRate)

	if len(created) == 0 {
		fmt.Fprintln(os.Stderr, "no tasks created — aborting")
		os.Exit(1)
	}

	// ── Phase 2: Monitor ────────────────────────────────────────────────

	if *skipMonitor {
		fmt.Println("Phase 2: Skipped (--skip-monitor)")
		totalDuration := time.Since(blastStart)
		reportBlastOnly(results, created, blastDuration, totalDuration)
		scrapeMetrics(client, *baseURL)
		return
	}

	// For very large runs, only monitor a sample.
	monitorSet := created
	if len(monitorSet) > 10000 {
		fmt.Printf("Phase 2: Monitoring sample of 10000/%d tasks...\n", len(created))
		monitorSet = created[:10000]
	} else {
		fmt.Println("Phase 2: Monitoring completion...")
	}

	statuses := monitor(ctx, client, *baseURL, monitorSet, *pollInterval, *timeout)

	// ── Phase 3: Report ─────────────────────────────────────────────────

	totalDuration := time.Since(blastStart)
	report(results, created, statuses, blastDuration, totalDuration)
	scrapeMetrics(client, *baseURL)
}

// ─── Phase 1 ────────────────────────────────────────────────────────────────

func blast(ctx context.Context, client *http.Client, baseURL string, total, concurrency int, taskType string, batchSize int, limiter *rate.Limiter, maxDuration time.Duration, liveInterval time.Duration) []taskResult {
	stats := &liveStats{start: time.Now()}

	// Live reporting goroutine.
	reportCtx, reportCancel := context.WithCancel(ctx)
	defer reportCancel()
	go liveReport(reportCtx, stats, liveInterval)

	// Duration-based mode: total=0, run until duration expires.
	if total == 0 && maxDuration > 0 {
		var dCtx context.Context
		var dCancel context.CancelFunc
		dCtx, dCancel = context.WithTimeout(ctx, maxDuration)
		defer dCancel()
		return blastUnlimited(dCtx, client, baseURL, concurrency, taskType, batchSize, limiter, stats)
	}

	// Fixed-count mode.
	if maxDuration > 0 {
		var dCtx context.Context
		var dCancel context.CancelFunc
		dCtx, dCancel = context.WithTimeout(ctx, maxDuration)
		defer dCancel()
		ctx = dCtx
	}

	results := make([]taskResult, total)
	ch := make(chan int, total)
	var wg sync.WaitGroup

	for i := 0; i < total; i++ {
		ch <- i
	}
	close(ch)

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range ch {
				if ctx.Err() != nil {
					results[idx] = taskResult{index: idx, err: ctx.Err()}
					continue
				}
				if limiter != nil {
					if err := limiter.Wait(ctx); err != nil {
						results[idx] = taskResult{index: idx, err: err}
						continue
					}
				}
				stats.sent.Add(1)
				r := createTask(ctx, client, baseURL, idx, taskType)
				results[idx] = r
				if r.err != nil {
					stats.errors.Add(1)
				} else {
					stats.success.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	reportCancel()
	return results
}

// blastUnlimited creates tasks until the context expires, returning a dynamically sized slice.
func blastUnlimited(ctx context.Context, client *http.Client, baseURL string, concurrency int, taskType string, batchSize int, limiter *rate.Limiter, stats *liveStats) []taskResult {
	var mu sync.Mutex
	var results []taskResult
	var idx atomic.Int64
	var wg sync.WaitGroup

	_ = batchSize // reserved for future batch API

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				if limiter != nil {
					if err := limiter.Wait(ctx); err != nil {
						return
					}
				}
				i := int(idx.Add(1) - 1)
				stats.sent.Add(1)
				r := createTask(ctx, client, baseURL, i, taskType)
				if r.err != nil {
					if ctx.Err() != nil {
						return
					}
					stats.errors.Add(1)
				} else {
					stats.success.Add(1)
				}
				mu.Lock()
				results = append(results, r)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return results
}

func liveReport(ctx context.Context, stats *liveStats, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastSuccess int64
	var lastTime time.Time = stats.start

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			success := stats.success.Load()
			errors := stats.errors.Load()
			elapsed := now.Sub(lastTime).Seconds()
			recentRate := float64(success-lastSuccess) / elapsed
			overallRate := float64(success) / now.Sub(stats.start).Seconds()

			fmt.Printf("  [live] sent=%d ok=%d err=%d | current=%.0f/s overall=%.0f/s\n",
				stats.sent.Load(), success, errors, recentRate, overallRate)

			lastSuccess = success
			lastTime = now
		}
	}
}

func createTask(ctx context.Context, client *http.Client, baseURL string, idx int, taskType string) taskResult {
	typ := taskType
	if taskType == "mixed" {
		if idx%2 == 0 {
			typ = "send_email"
		} else {
			typ = "run_query"
		}
	}

	payload := map[string]any{
		"name":     fmt.Sprintf("stress-%d", idx),
		"type":     typ,
		"priority": idx % 10,
	}
	switch typ {
	case "send_email":
		payload["payload"] = map[string]string{
			"to":      fmt.Sprintf("%d@test.com", idx),
			"subject": "stress",
		}
	case "run_query":
		payload["payload"] = map[string]string{
			"query": fmt.Sprintf("SELECT %d", idx),
		}
	case "noop":
		payload["payload"] = map[string]string{}
	}

	body, _ := json.Marshal(payload)

	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/tasks", bytes.NewReader(body))
	if err != nil {
		return taskResult{index: idx, err: err, latency: time.Since(start)}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	latency := time.Since(start)
	if err != nil {
		return taskResult{index: idx, err: err, latency: latency}
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return taskResult{index: idx, err: fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody)), latency: latency}
	}

	var result struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return taskResult{index: idx, err: fmt.Errorf("bad response JSON: %w", err), latency: latency}
	}

	return taskResult{index: idx, id: result.ID, latency: latency}
}

// ─── Phase 2 ────────────────────────────────────────────────────────────────

func monitor(ctx context.Context, client *http.Client, baseURL string, created []taskResult, pollInterval, timeout time.Duration) []taskStatus {
	statuses := make([]taskStatus, len(created))
	createdAt := time.Now()

	pending := make(map[int]string, len(created))
	for i, r := range created {
		statuses[i] = taskStatus{id: r.id, status: "stuck", created: createdAt}
		pending[i] = r.id
	}

	deadline := time.After(timeout)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	sem := make(chan struct{}, 200)

	for len(pending) > 0 {
		select {
		case <-ctx.Done():
			fmt.Println("  interrupted — marking remaining as stuck")
			return statuses
		case <-deadline:
			fmt.Printf("  timeout — %d tasks still pending, marking as stuck\n", len(pending))
			return statuses
		case <-ticker.C:
		}

		var mu sync.Mutex
		var wg sync.WaitGroup
		var done []int
		statusCounts := make(map[string]int)

		for i, id := range pending {
			i, id := i, id
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()

				status := pollTask(ctx, client, baseURL, id)
				mu.Lock()
				statusCounts[status]++
				if status == "succeeded" || status == "failed" || status == "dead_lettered" {
					statuses[i] = taskStatus{
						id:      id,
						status:  status,
						e2e:     time.Since(createdAt),
						created: createdAt,
					}
					done = append(done, i)
				}
				mu.Unlock()
			}()
		}
		wg.Wait()

		for _, i := range done {
			delete(pending, i)
		}

		var parts []string
		for s, n := range statusCounts {
			parts = append(parts, fmt.Sprintf("%s=%d", s, n))
		}
		sort.Strings(parts)
		fmt.Printf("  pending: %d / %d  [%s]\n", len(pending), len(created), strings.Join(parts, " "))
	}

	return statuses
}

func pollTask(ctx context.Context, client *http.Client, baseURL, id string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/tasks/"+id, nil)
	if err != nil {
		return "unknown"
	}
	resp, err := client.Do(req)
	if err != nil {
		return "unknown"
	}
	defer resp.Body.Close()

	var t struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		return "unknown"
	}
	return t.Status
}

// ─── Phase 3 ────────────────────────────────────────────────────────────────

func reportBlastOnly(allResults []taskResult, created []taskResult, blastDur, totalDur time.Duration) {
	fmt.Println()
	fmt.Println("=== Stress Test Results (blast only) ===")

	errCount := len(allResults) - len(created)
	fmt.Printf("Tasks created:     %d\n", len(created))
	fmt.Printf("Creation errors:   %d\n", errCount)
	fmt.Printf("Error rate:        %.2f%%\n", 100*float64(errCount)/float64(len(allResults)))

	creationLatencies := make([]time.Duration, len(created))
	for i, r := range created {
		creationLatencies[i] = r.latency
	}
	if len(creationLatencies) > 0 {
		sort.Slice(creationLatencies, func(i, j int) bool { return creationLatencies[i] < creationLatencies[j] })
		fmt.Printf("Creation latency:  p50=%s  p95=%s  p99=%s  p999=%s\n",
			percentile(creationLatencies, 50),
			percentile(creationLatencies, 95),
			percentile(creationLatencies, 99),
			percentile(creationLatencies, 99.9),
		)
	}

	tps := float64(len(created)) / blastDur.Seconds()
	fmt.Printf("\nThroughput:        %.0f tasks/sec created\n", tps)
	fmt.Printf("Duration:          %s\n", totalDur.Truncate(time.Millisecond))
}

func report(allResults []taskResult, created []taskResult, statuses []taskStatus, blastDur, totalDur time.Duration) {
	fmt.Println()
	fmt.Println("=== Stress Test Results ===")

	errCount := len(allResults) - len(created)
	fmt.Printf("Tasks created:     %d\n", len(created))
	fmt.Printf("Creation errors:   %d\n", errCount)
	fmt.Printf("Error rate:        %.2f%%\n", 100*float64(errCount)/float64(len(allResults)))

	creationLatencies := make([]time.Duration, len(created))
	for i, r := range created {
		creationLatencies[i] = r.latency
	}
	if len(creationLatencies) > 0 {
		sort.Slice(creationLatencies, func(i, j int) bool { return creationLatencies[i] < creationLatencies[j] })
		fmt.Printf("Creation latency:  p50=%s  p95=%s  p99=%s  p999=%s\n",
			percentile(creationLatencies, 50),
			percentile(creationLatencies, 95),
			percentile(creationLatencies, 99),
			percentile(creationLatencies, 99.9),
		)
	}

	var completed, failed, stuck int
	var e2eLatencies []time.Duration
	for _, s := range statuses {
		switch s.status {
		case "succeeded":
			completed++
			e2eLatencies = append(e2eLatencies, s.e2e)
		case "failed", "dead_lettered":
			failed++
			e2eLatencies = append(e2eLatencies, s.e2e)
		default:
			stuck++
		}
	}

	fmt.Println()
	fmt.Printf("Tasks completed:   %d\n", completed)
	fmt.Printf("Tasks failed:      %d\n", failed)
	fmt.Printf("Tasks stuck:       %d\n", stuck)

	if len(e2eLatencies) > 0 {
		sort.Slice(e2eLatencies, func(i, j int) bool { return e2eLatencies[i] < e2eLatencies[j] })
		fmt.Printf("E2E latency:       p50=%s  p95=%s  p99=%s  p999=%s\n",
			percentile(e2eLatencies, 50),
			percentile(e2eLatencies, 95),
			percentile(e2eLatencies, 99),
			percentile(e2eLatencies, 99.9),
		)
	}

	if totalDur > 0 && (completed+failed) > 0 {
		tps := float64(completed+failed) / totalDur.Seconds()
		fmt.Printf("\nCreation rate:     %.0f tasks/sec\n", float64(len(created))/blastDur.Seconds())
		fmt.Printf("Processing rate:   %.0f tasks/sec\n", tps)
	}
	fmt.Printf("Duration:          %s total\n", totalDur.Truncate(time.Second))
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*p/100)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx].Truncate(time.Millisecond)
}

func scrapeMetrics(client *http.Client, baseURL string) {
	resp, err := client.Get(baseURL + "/metrics")
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nCould not fetch /metrics: %v\n", err)
		return
	}
	defer resp.Body.Close()

	fmt.Println()
	fmt.Println("=== Worker Metrics (from /metrics) ===")

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "worker_tasks_processed_total") ||
			strings.HasPrefix(line, "worker_tasks_in_progress") ||
			strings.HasPrefix(line, "worker_task_duration_seconds") ||
			strings.HasPrefix(line, "tasks_created_total") ||
			strings.HasPrefix(line, "kafka_produce_total") ||
			strings.HasPrefix(line, "kafka_consume_total") ||
			strings.HasPrefix(line, "kafka_consume_lag") ||
			strings.HasPrefix(line, "history_flush_total") ||
			strings.HasPrefix(line, "history_buffer_depth") {
			fmt.Println(line)
		}
	}
}
