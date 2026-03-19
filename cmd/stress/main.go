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

func main() {
	concurrency := flag.Int("concurrency", 50, "number of goroutines creating tasks")
	totalTasks := flag.Int("tasks", 500, "total number of tasks to create")
	baseURL := flag.String("base-url", "http://localhost:8080", "base URL of the task manager API")
	taskType := flag.String("type", "send_email", "task type: send_email, run_query, or mixed")
	pollInterval := flag.Duration("poll-interval", 1*time.Second, "how often to poll for task completion")
	timeout := flag.Duration("timeout", 10*time.Minute, "max time to wait for all tasks to complete")
	flag.Parse()

	if *taskType != "send_email" && *taskType != "run_query" && *taskType != "mixed" {
		fmt.Fprintf(os.Stderr, "invalid -type %q: must be send_email, run_query, or mixed\n", *taskType)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: *concurrency + 10,
			MaxIdleConns:        *concurrency + 10,
		},
	}

	fmt.Printf("=== Stress Test Config ===\n")
	fmt.Printf("Base URL:     %s\n", *baseURL)
	fmt.Printf("Tasks:        %d\n", *totalTasks)
	fmt.Printf("Concurrency:  %d\n", *concurrency)
	fmt.Printf("Type:         %s\n", *taskType)
	fmt.Printf("Poll interval:%s\n", *pollInterval)
	fmt.Printf("Timeout:      %s\n\n", *timeout)

	// ── Phase 1: Blast ──────────────────────────────────────────────────

	fmt.Println("Phase 1: Creating tasks...")
	blastStart := time.Now()
	results := blast(ctx, client, *baseURL, *totalTasks, *concurrency, *taskType)
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
	fmt.Printf("Phase 1 done: %d created, %d errors in %s\n\n", len(created), errCount, blastDuration.Truncate(time.Millisecond))

	if len(created) == 0 {
		fmt.Fprintln(os.Stderr, "no tasks created — aborting")
		os.Exit(1)
	}

	// ── Phase 2: Monitor ────────────────────────────────────────────────

	fmt.Println("Phase 2: Monitoring completion...")
	statuses := monitor(ctx, client, *baseURL, created, *pollInterval, *timeout)

	// ── Phase 3: Report ─────────────────────────────────────────────────

	totalDuration := time.Since(blastStart)
	report(results, created, statuses, blastDuration, totalDuration)
	scrapeMetrics(client, *baseURL)
}

// ─── Phase 1 ────────────────────────────────────────────────────────────────

func blast(ctx context.Context, client *http.Client, baseURL string, total, concurrency int, taskType string) []taskResult {
	results := make([]taskResult, total)
	ch := make(chan int, total)
	var progress atomic.Int64
	var wg sync.WaitGroup

	// Fill work channel.
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
				results[idx] = createTask(ctx, client, baseURL, idx, taskType)
				n := progress.Add(1)
				if n%100 == 0 {
					fmt.Printf("  created %d/%d\n", n, total)
				}
			}
		}()
	}
	wg.Wait()
	return results
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
		"priority": time.Now().Unix(),
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

	// Build pending set: index in statuses slice → task ID.
	pending := make(map[int]string, len(created))
	for i, r := range created {
		statuses[i] = taskStatus{id: r.id, status: "stuck", created: createdAt}
		pending[i] = r.id
	}

	deadline := time.After(timeout)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	sem := make(chan struct{}, 50)

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

		// Build status breakdown string.
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

func report(allResults []taskResult, created []taskResult, statuses []taskStatus, blastDur, totalDur time.Duration) {
	fmt.Println()
	fmt.Println("=== Stress Test Results ===")

	// Creation stats.
	errCount := len(allResults) - len(created)
	fmt.Printf("Tasks created:     %d\n", len(created))
	fmt.Printf("Creation errors:   %d\n", errCount)

	creationLatencies := make([]time.Duration, len(created))
	for i, r := range created {
		creationLatencies[i] = r.latency
	}
	if len(creationLatencies) > 0 {
		sort.Slice(creationLatencies, func(i, j int) bool { return creationLatencies[i] < creationLatencies[j] })
		fmt.Printf("Creation latency:  p50=%s  p95=%s  p99=%s\n",
			percentile(creationLatencies, 50),
			percentile(creationLatencies, 95),
			percentile(creationLatencies, 99),
		)
	}

	// Completion stats.
	var completed, failed, stuck int
	var e2eLatencies []time.Duration
	for _, s := range statuses {
		switch s.status {
		case "succeeded":
			completed++
			e2eLatencies = append(e2eLatencies, s.e2e)
		case "failed":
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
		fmt.Printf("E2E latency:       p50=%s  p95=%s  p99=%s\n",
			percentile(e2eLatencies, 50),
			percentile(e2eLatencies, 95),
			percentile(e2eLatencies, 99),
		)
	}

	// Throughput.
	if totalDur > 0 && (completed+failed) > 0 {
		tps := float64(completed+failed) / totalDur.Seconds()
		fmt.Printf("\nThroughput:        ~%.1f tasks/sec processed\n", tps)
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
			strings.HasPrefix(line, "tasks_created_total") {
			fmt.Println(line)
		}
	}
}
