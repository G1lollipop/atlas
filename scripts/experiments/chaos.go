package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type chaosRun struct {
	ID           string
	ExecutionKey string
	Status       string
	Attempt      int16
	Result       map[string]any
}

type chaosCase struct {
	runner *runner
	result *chaosResult
}

func (c *chaosCase) check(name string, passed bool, observed, expected string) error {
	c.result.Assertions = append(c.result.Assertions, assertion{
		Name: name, Passed: passed, Observed: observed, Expected: expected,
	})
	if passed {
		return nil
	}
	return fmt.Errorf("%s: observed %s, expected %s", name, observed, expected)
}

func (r *runner) runChaos(ctx context.Context) error {
	scenarios := []string{
		"side-effect-crash",
		"worker-loss",
		"scheduler-failover",
		"timeout",
		"postgres-outage",
		"redis-outage",
		"graceful-shutdown",
	}
	if r.opts.scenario != "all" {
		scenarios = []string{r.opts.scenario}
	}

	var failures []string
	for _, scenario := range scenarios {
		if err := ctx.Err(); err != nil {
			failures = append(failures, err.Error())
			break
		}

		caseCtx, cancel := context.WithTimeout(ctx, r.opts.caseDeadline)
		r.setLabel("chaos-" + scenario)
		if scenario == "graceful-shutdown" {
			result, err := r.runGracefulShutdownScenario(caseCtx)
			cancel()
			r.report.Chaos = append(r.report.Chaos, result)
			if !result.Passed || err != nil {
				message := result.Error
				if message == "" && err != nil {
					message = err.Error()
				}
				if message == "" {
					message = "scenario reported failure"
				}
				failure := scenario + ": " + message
				failures = append(failures, failure)
				r.report.Errors = append(r.report.Errors, failure)
			}
			continue
		}
		result := chaosResult{ScenarioID: scenario, StartedAt: time.Now().UTC()}
		c := &chaosCase{runner: r, result: &result}
		err := c.run(caseCtx, scenario)
		cancel()
		result.FinishedAt = time.Now().UTC()
		result.Passed = err == nil
		if err != nil {
			result.Error = err.Error()
			failure := scenario + ": " + err.Error()
			failures = append(failures, failure)
			r.report.Errors = append(r.report.Errors, failure)
		}
		r.report.Chaos = append(r.report.Chaos, result)
	}

	if len(failures) > 0 {
		return fmt.Errorf("chaos scenarios failed: %s", strings.Join(failures, "; "))
	}
	return nil
}

func (c *chaosCase) run(ctx context.Context, scenario string) error {
	switch scenario {
	case "side-effect-crash":
		return c.sideEffectCrash(ctx)
	case "worker-loss":
		return c.workerLoss(ctx)
	case "scheduler-failover":
		return c.schedulerFailover(ctx)
	case "timeout":
		return c.timeoutRetry(ctx)
	case "postgres-outage":
		return c.postgresOutage(ctx)
	case "redis-outage":
		return c.redisOutage(ctx)
	default:
		return fmt.Errorf("unknown chaos scenario %q", scenario)
	}
}

func (c *chaosCase) prepare(ctx context.Context, scenario string, schedulerCount int) (workerSpec, error) {
	r := c.runner
	var cleanupErr error
	if err := r.stopWorkers(); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("stop previous experiment workers: %w", err))
	}
	if err := r.stopSchedulers(); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("stop previous experiment schedulers: %w", err))
	}
	if cleanupErr != nil {
		return workerSpec{}, cleanupErr
	}

	worker := workerSpec{id: "chaos-" + safeAppName(scenario) + "-worker", cpu: 4000, mem: 8192}
	if err := r.startWorker(worker, r.opts.leaseDuration); err != nil {
		return worker, fmt.Errorf("start chaos worker: %w", err)
	}
	if err := r.waitWorkerHeartbeat(ctx, worker.id, time.Now().Add(-time.Second)); err != nil {
		return worker, err
	}
	for i := 0; i < schedulerCount; i++ {
		if err := r.startScheduler("first-fit"); err != nil {
			return worker, fmt.Errorf("start chaos scheduler %d: %w", i+1, err)
		}
	}
	if schedulerCount > 0 {
		if err := r.waitLeader(ctx, 5*time.Second); err != nil {
			return worker, err
		}
	}
	return worker, nil
}

func (c *chaosCase) sideEffectCrash(ctx context.Context) (scenarioErr error) {
	r := c.runner
	worker, err := c.prepare(ctx, "side-effect-crash", 1)
	if err != nil {
		return err
	}

	dir, err := os.MkdirTemp("", "atlas-chaos-side-effect-")
	if err != nil {
		return fmt.Errorf("create side-effect barrier directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			scenarioErr = errors.Join(scenarioErr, fmt.Errorf("remove side-effect barrier directory: %w", err))
		}
	}()
	barrierPath := filepath.Join(dir, "effect-committed")
	releasePath := filepath.Join(dir, "release")
	name := chaosJobName("side-effect-crash")
	status, jobID, err := r.submitChaosJob(ctx, name, "sideeffect_barrier", map[string]any{
		"barrier_path": barrierPath,
		"release_path": releasePath,
	}, 60, 4)
	if err != nil {
		return fmt.Errorf("submit side-effect job: %w", err)
	}
	if err := c.check("job accepted", status == http.StatusCreated, fmt.Sprintf("HTTP %d", status), "HTTP 201"); err != nil {
		return err
	}
	if err := waitForFile(ctx, barrierPath); err != nil {
		return fmt.Errorf("wait for committed side effect barrier: %w", err)
	}

	firstRun, err := r.waitChaosRun(ctx, jobID, func(run *chaosRun) bool { return run.Status == "running" })
	if err != nil {
		return fmt.Errorf("wait for side-effect run to be running: %w", err)
	}
	if count, err := r.effectCount(ctx, jobID); err != nil {
		return fmt.Errorf("count persisted effects: %w", err)
	} else if err := c.check("effect committed before crash", count == 1, fmt.Sprintf("%d rows", count), "1 row"); err != nil {
		return err
	}

	oldWorker := findChild(r.workers, "worker-"+worker.id)
	if oldWorker == nil {
		return fmt.Errorf("side-effect worker process %q not found", worker.id)
	}
	if err := oldWorker.kill(); err != nil {
		return fmt.Errorf("kill side-effect worker: %w", err)
	}
	before, err := r.workerHeartbeatTime(ctx, worker.id)
	if err != nil {
		return fmt.Errorf("read worker heartbeat after kill: %w", err)
	}
	if err := r.startWorker(worker, r.opts.leaseDuration); err != nil {
		return fmt.Errorf("restart side-effect worker: %w", err)
	}
	if err := r.waitWorkerHeartbeat(ctx, worker.id, before); err != nil {
		return fmt.Errorf("wait for restarted side-effect worker: %w", err)
	}
	if err := c.check("worker heartbeat resumed after crash", true, "fresh alive heartbeat", "fresh alive heartbeat"); err != nil {
		return err
	}
	terminal, err := r.waitChaosTerminal(ctx, jobID)
	if err != nil {
		return fmt.Errorf("wait for idempotent side-effect replay: %w", err)
	}
	if err := c.check("side-effect run recovered", terminal.Status == "succeeded" && terminal.Attempt == 2,
		fmt.Sprintf("status=%s attempt=%d", terminal.Status, terminal.Attempt), "status=succeeded attempt=2"); err != nil {
		return err
	}
	count, err := r.effectCount(ctx, jobID)
	if err != nil {
		return fmt.Errorf("count effects after replay: %w", err)
	}
	if err := c.check("effect was not repeated", count == 1, fmt.Sprintf("%d rows", count), "1 row"); err != nil {
		return err
	}
	if err := c.check("run keeps stable execution key", terminal.ExecutionKey != "" && terminal.ExecutionKey == firstRun.ExecutionKey,
		terminal.ExecutionKey, firstRun.ExecutionKey); err != nil {
		return err
	}
	return nil
}

func (c *chaosCase) workerLoss(ctx context.Context) error {
	r := c.runner
	worker, err := c.prepare(ctx, "worker-loss", 1)
	if err != nil {
		return err
	}
	name := chaosJobName("worker-loss")
	status, jobID, err := r.submitChaosJob(ctx, name, "simulate_cpu", map[string]any{"duration_ms": 12000}, 30, 4)
	if err != nil {
		return fmt.Errorf("submit worker-loss job: %w", err)
	}
	if err := c.check("job accepted", status == http.StatusCreated, fmt.Sprintf("HTTP %d", status), "HTTP 201"); err != nil {
		return err
	}
	run, err := r.waitChaosRun(ctx, jobID, func(run *chaosRun) bool { return run.Status == "running" })
	if err != nil {
		return fmt.Errorf("wait for worker-loss run to start: %w", err)
	}
	child := findChild(r.workers, "worker-"+worker.id)
	if child == nil {
		return fmt.Errorf("worker process %q not found", worker.id)
	}
	// Kill the process partway through its active lease. Its lease remains
	// recoverable, while a replacement process reuses the same worker identity.
	if err := waitContext(ctx, r.opts.leaseDuration/2); err != nil {
		return err
	}
	if err := child.kill(); err != nil {
		return fmt.Errorf("kill worker during run %s: %w", run.ID, err)
	}
	oldHeartbeat, err := r.workerHeartbeatTime(ctx, worker.id)
	if err != nil {
		return fmt.Errorf("read worker heartbeat after loss: %w", err)
	}
	if err := r.startWorker(worker, r.opts.leaseDuration); err != nil {
		return fmt.Errorf("restart worker after loss: %w", err)
	}
	if err := r.waitWorkerHeartbeat(ctx, worker.id, oldHeartbeat); err != nil {
		return fmt.Errorf("wait for replacement worker heartbeat: %w", err)
	}
	if err := c.check("worker heartbeat resumed after loss", true, "fresh alive heartbeat", "fresh alive heartbeat"); err != nil {
		return err
	}
	terminal, err := r.waitChaosTerminal(ctx, jobID)
	if err != nil {
		return fmt.Errorf("wait for worker-loss recovery: %w", err)
	}
	return c.check("run recovered after worker loss", terminal.Status == "succeeded" && terminal.Attempt >= 2,
		fmt.Sprintf("status=%s attempt=%d", terminal.Status, terminal.Attempt), "status=succeeded attempt>=2")
}

func (c *chaosCase) schedulerFailover(ctx context.Context) error {
	r := c.runner
	if _, err := c.prepare(ctx, "scheduler-failover", 2); err != nil {
		return err
	}
	leaders, err := r.leaderRows(ctx)
	if err != nil || len(leaders) != 1 {
		return fmt.Errorf("read initial leader: leaders=%v err=%v", leaders, err)
	}
	firstLeader := leaders[0]
	leaderProcess := findChildByApp(r.schedulers, firstLeader.app)
	if leaderProcess == nil {
		return fmt.Errorf("leader process %q not found", firstLeader.app)
	}
	if err := leaderProcess.kill(); err != nil {
		return fmt.Errorf("kill scheduler leader %q: %w", firstLeader.app, err)
	}
	newLeader, err := r.waitDifferentLeader(ctx, firstLeader.app)
	if err != nil {
		return err
	}
	if err := c.check("leadership transferred", newLeader.app != firstLeader.app,
		newLeader.app, "a different scheduler holds the advisory lock"); err != nil {
		return err
	}
	name := chaosJobName("scheduler-failover")
	status, jobID, err := r.submitChaosJob(ctx, name, "simulate_cpu", map[string]any{"duration_ms": 100}, 30, 3)
	if err != nil {
		return fmt.Errorf("submit failover job: %w", err)
	}
	if err := c.check("job accepted after failover", status == http.StatusCreated, fmt.Sprintf("HTTP %d", status), "HTTP 201"); err != nil {
		return err
	}
	terminal, err := r.waitChaosTerminal(ctx, jobID)
	if err != nil {
		return fmt.Errorf("wait for post-failover job: %w", err)
	}
	return c.check("new leader schedules work", terminal.Status == "succeeded",
		fmt.Sprintf("status=%s attempt=%d", terminal.Status, terminal.Attempt), "status=succeeded")
}

func (c *chaosCase) timeoutRetry(ctx context.Context) error {
	r := c.runner
	if _, err := c.prepare(ctx, "timeout", 1); err != nil {
		return err
	}
	name := chaosJobName("timeout")
	status, jobID, err := r.submitChaosJob(ctx, name, "stale_completion_probe", map[string]any{
		"first_attempt_duration_ms": 1600,
		"later_attempt_duration_ms": 50,
	}, 1, 3)
	if err != nil {
		return fmt.Errorf("submit timeout probe: %w", err)
	}
	if err := c.check("timeout probe accepted", status == http.StatusCreated, fmt.Sprintf("HTTP %d", status), "HTTP 201"); err != nil {
		return err
	}
	terminal, err := r.waitChaosTerminal(ctx, jobID)
	if err != nil {
		return fmt.Errorf("wait for timeout retry: %w", err)
	}
	if err := c.check("timed-out attempt retried", terminal.Attempt == 2,
		fmt.Sprintf("attempt=%d", terminal.Attempt), "attempt=2"); err != nil {
		return err
	}
	if err := c.check("later attempt completed", terminal.Status == "succeeded",
		fmt.Sprintf("status=%s", terminal.Status), "status=succeeded"); err != nil {
		return err
	}
	return c.check("completion belongs to latest attempt", terminal.Result["attempt"] == float64(terminal.Attempt),
		fmt.Sprintf("result attempt=%v, run attempt=%d", terminal.Result["attempt"], terminal.Attempt), "matching attempt number")
}

func (c *chaosCase) postgresOutage(ctx context.Context) error {
	r := c.runner
	if r.pgProxy == nil {
		return fmt.Errorf("postgresql proxy is unavailable in chaos mode")
	}
	if _, err := c.prepare(ctx, "postgres-outage", 1); err != nil {
		return err
	}
	name := chaosJobName("postgres-outage")
	r.pgProxy.SetAvailable(false)
	status, _, requestErr := r.submitChaosJob(ctx, name, "simulate_cpu", map[string]any{"duration_ms": 50}, 30, 2)
	r.pgProxy.SetAvailable(true)
	if requestErr != nil {
		return fmt.Errorf("submit while PostgreSQL proxy was unavailable: %w", requestErr)
	}
	if err := c.check("PostgreSQL outage rejects create", status != http.StatusCreated,
		fmt.Sprintf("HTTP %d", status), "a non-201 response"); err != nil {
		return err
	}
	count, err := r.jobCountByIdempotencyKey(ctx, name)
	if err != nil {
		return fmt.Errorf("check failed PostgreSQL create: %w", err)
	}
	if err := c.check("failed PostgreSQL create left no job", count == 0,
		fmt.Sprintf("%d rows", count), "0 rows"); err != nil {
		return err
	}
	if _, err := c.prepare(ctx, "postgres-outage-recovery", 1); err != nil {
		return fmt.Errorf("restart scheduler and worker after PostgreSQL recovery: %w", err)
	}
	status, jobID, err := r.submitChaosJob(ctx, name, "simulate_cpu", map[string]any{"duration_ms": 50}, 30, 2)
	if err != nil {
		return fmt.Errorf("submit after PostgreSQL recovery: %w", err)
	}
	if err := c.check("PostgreSQL recovered", status == http.StatusCreated, fmt.Sprintf("HTTP %d", status), "HTTP 201"); err != nil {
		return err
	}
	terminal, err := r.waitChaosTerminal(ctx, jobID)
	if err != nil {
		return fmt.Errorf("wait for job after PostgreSQL recovery: %w", err)
	}
	return c.check("scheduler and worker recovered", terminal.Status == "succeeded",
		fmt.Sprintf("status=%s attempt=%d", terminal.Status, terminal.Attempt), "status=succeeded")
}

func (c *chaosCase) redisOutage(ctx context.Context) error {
	r := c.runner
	if r.redisProxy == nil {
		return fmt.Errorf("redis proxy is unavailable; configure --redis-addr")
	}
	if _, err := c.prepare(ctx, "redis-outage", 1); err != nil {
		return err
	}
	name := chaosJobName("redis-outage")
	r.redisProxy.SetAvailable(false)
	status, _, requestErr := r.submitChaosJob(ctx, name, "simulate_cpu", map[string]any{"duration_ms": 50}, 30, 2)
	r.redisProxy.SetAvailable(true)
	if requestErr != nil {
		return fmt.Errorf("submit while Redis proxy was unavailable: %w", requestErr)
	}
	if err := c.check("Redis outage fails closed", status == http.StatusServiceUnavailable,
		fmt.Sprintf("HTTP %d", status), "HTTP 503"); err != nil {
		return err
	}
	count, err := r.jobCountByIdempotencyKey(ctx, name)
	if err != nil {
		return fmt.Errorf("check Redis outage did not create a job: %w", err)
	}
	if err := c.check("Redis rejection did not create a job", count == 0,
		fmt.Sprintf("%d rows", count), "0 rows"); err != nil {
		return err
	}
	status, jobID, err := r.submitChaosJob(ctx, name, "simulate_cpu", map[string]any{"duration_ms": 50}, 30, 2)
	if err != nil {
		return fmt.Errorf("submit after Redis recovery: %w", err)
	}
	if err := c.check("Redis recovered", status == http.StatusCreated, fmt.Sprintf("HTTP %d", status), "HTTP 201"); err != nil {
		return err
	}
	terminal, err := r.waitChaosTerminal(ctx, jobID)
	if err != nil {
		return fmt.Errorf("wait for job after Redis recovery: %w", err)
	}
	return c.check("API, scheduler, and worker recovered", terminal.Status == "succeeded",
		fmt.Sprintf("status=%s attempt=%d", terminal.Status, terminal.Attempt), "status=succeeded")
}

func (r *runner) submitChaosJob(ctx context.Context, name, handler string, payload map[string]any, timeoutSeconds int, maxAttempts int) (int, string, error) {
	body, err := json.Marshal(map[string]any{
		"name":                handler,
		"payload":             payload,
		"workload_type":       "chaos",
		"idempotency_key":     name,
		"required_cpu_millis": 1000,
		"required_memory_mb":  128,
		"max_attempts":        maxAttempts,
		"timeout_seconds":     timeoutSeconds,
	})
	if err != nil {
		return 0, "", fmt.Errorf("marshal chaos job request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+r.apiAddr+"/v1/jobs", strings.NewReader(string(body)))
	if err != nil {
		return 0, "", fmt.Errorf("build chaos job request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.token())
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 8192))
	closeErr := resp.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return resp.StatusCode, "", fmt.Errorf("read or close chaos job response: %w", err)
	}
	if resp.StatusCode != http.StatusCreated {
		return resp.StatusCode, "", nil
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(responseBody, &created); err != nil {
		return resp.StatusCode, "", fmt.Errorf("decode chaos job response: %w", err)
	}
	if created.ID == "" {
		return resp.StatusCode, "", fmt.Errorf("created chaos job response has no ID")
	}
	return resp.StatusCode, created.ID, nil
}

func (r *runner) waitChaosRun(ctx context.Context, jobID string, predicate func(*chaosRun) bool) (*chaosRun, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		run, err := r.latestChaosRun(ctx, jobID)
		if err != nil {
			return nil, err
		}
		if run != nil && predicate(run) {
			return run, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for run for job %s: %w", jobID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (r *runner) waitChaosTerminal(ctx context.Context, jobID string) (*chaosRun, error) {
	return r.waitChaosRun(ctx, jobID, func(run *chaosRun) bool {
		switch run.Status {
		case "succeeded", "dead", "failed", "canceled":
			return true
		default:
			return false
		}
	})
}

func (r *runner) latestChaosRun(ctx context.Context, jobID string) (*chaosRun, error) {
	var count int
	var runID, executionKey, status, resultRaw string
	var attempt int16
	err := r.dataPool.QueryRow(ctx, `
		SELECT count(*)::int,
		       COALESCE((array_agg(id::text ORDER BY created_at DESC))[1], ''),
		       COALESCE((array_agg(execution_key::text ORDER BY created_at DESC))[1], ''),
		       COALESCE((array_agg(status::text ORDER BY created_at DESC))[1], ''),
		       COALESCE((array_agg(attempt ORDER BY created_at DESC))[1], 0::smallint),
		       COALESCE((array_agg(result::text ORDER BY created_at DESC))[1], '{}')
		FROM job_runs WHERE job_id = $1::uuid
	`, jobID).Scan(&count, &runID, &executionKey, &status, &attempt, &resultRaw)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	run := &chaosRun{ID: runID, ExecutionKey: executionKey, Status: status, Attempt: attempt, Result: map[string]any{}}
	if err := json.Unmarshal([]byte(resultRaw), &run.Result); err != nil {
		return nil, fmt.Errorf("decode run result: %w", err)
	}
	return run, nil
}

func (r *runner) effectCount(ctx context.Context, jobID string) (int, error) {
	var count int
	err := r.dataPool.QueryRow(ctx, `
		SELECT count(*)::int
		FROM idempotent_handler_effects e
		JOIN job_runs r ON r.execution_key = e.execution_key
		WHERE r.job_id = $1::uuid
	`, jobID).Scan(&count)
	return count, err
}

func (r *runner) jobCountByIdempotencyKey(ctx context.Context, key string) (int, error) {
	var count int
	err := r.dataPool.QueryRow(ctx, `SELECT count(*)::int FROM jobs WHERE idempotency_key = $1`, key).Scan(&count)
	return count, err
}

func (r *runner) workerHeartbeatTime(ctx context.Context, workerID string) (time.Time, error) {
	var heartbeat time.Time
	err := r.dataPool.QueryRow(ctx, `SELECT last_heartbeat_at FROM workers WHERE id = $1`, workerID).Scan(&heartbeat)
	return heartbeat, err
}

func (r *runner) waitWorkerHeartbeat(ctx context.Context, workerID string, after time.Time) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var alive bool
		err := r.dataPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM workers
				WHERE id = $1 AND status = 'alive' AND last_heartbeat_at > $2
			)
		`, workerID, after).Scan(&alive)
		if err != nil {
			return fmt.Errorf("query heartbeat for %s: %w", workerID, err)
		}
		if alive {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("worker %s did not heartbeat after %s: %w", workerID, after.UTC().Format(time.RFC3339Nano), ctx.Err())
		case <-ticker.C:
		}
	}
}

func (r *runner) waitDifferentLeader(ctx context.Context, oldApp string) (leaderRow, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		leaders, err := r.leaderRows(ctx)
		if err == nil && len(leaders) == 1 && leaders[0].app != oldApp {
			return leaders[0], nil
		}
		select {
		case <-ctx.Done():
			return leaderRow{}, fmt.Errorf("new scheduler leader not observed after killing %s: %w", oldApp, ctx.Err())
		case <-ticker.C:
		}
	}
}

func findChild(children []*child, name string) *child {
	for _, process := range children {
		if process.name == name {
			return process
		}
	}
	return nil
}

func findChildByApp(children []*child, appName string) *child {
	for _, process := range children {
		if process.appName == appName {
			return process
		}
	}
	return nil
}

func chaosJobName(scenario string) string {
	return fmt.Sprintf("chaos-%s-%d", safeFileName(scenario), time.Now().UnixNano())
}

func waitForFile(ctx context.Context, path string) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
