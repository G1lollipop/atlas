package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	gracefulWorkerLease    = 3 * time.Second
	gracefulWorkerShutdown = 2 * time.Second
	gracefulProcessBound   = gracefulWorkerShutdown + 1500*time.Millisecond
)

type gracefulRunState struct {
	ID               string
	ExecutionKey     string
	Status           string
	Attempt          int16
	LeasedBy         string
	AssignedWorkerID string
	LeaseExpiresAt   time.Time
	StartedAt        time.Time
	Result           map[string]any
}

// runGracefulShutdownScenario exercises SIGTERM against real worker processes.
// It deliberately reports an unsupported-platform failure outside Linux rather
// than treating Windows interrupt delivery as equivalent to SIGTERM.
func (r *runner) runGracefulShutdownScenario(ctx context.Context) (result chaosResult, scenarioErr error) {
	result = chaosResult{ScenarioID: "graceful-shutdown", StartedAt: time.Now().UTC()}
	defer func() {
		result.FinishedAt = time.Now().UTC()
		result.Passed = result.Error == ""
	}()

	if runtime.GOOS != "linux" {
		result.Error = "graceful-shutdown requires Linux SIGTERM semantics; run the experiment harness under WSL or Linux"
		result.Assertions = append(result.Assertions, assertion{
			Name: "Linux SIGTERM process support", Passed: false,
			Observed: runtime.GOOS, Expected: "linux",
		})
		return result, fmt.Errorf("%s", result.Error)
	}

	caseRunner := &chaosCase{runner: r, result: &result}
	if err := caseRunner.gracefulShutdownCompletion(ctx); err != nil {
		result.Error = err.Error()
		return result, err
	}
	if err := caseRunner.gracefulShutdownStaleAttempt(ctx); err != nil {
		result.Error = err.Error()
		return result, err
	}
	return result, nil
}

func (c *chaosCase) gracefulShutdownCompletion(ctx context.Context) error {
	r := c.runner
	worker, process, err := c.startGracefulWorker(ctx, "drain-completion", gracefulWorkerLease)
	if err != nil {
		return err
	}

	name := chaosJobName("graceful-shutdown-completion")
	status, jobID, err := r.submitChaosJob(ctx, name, "simulate_cpu", map[string]any{"duration_ms": 1000}, 10, 1)
	if err != nil {
		return fmt.Errorf("submit graceful completion job: %w", err)
	}
	if err := c.check("graceful completion job accepted", status == 201, fmt.Sprintf("HTTP %d", status), "HTTP 201"); err != nil {
		return err
	}
	active, err := r.waitGracefulRun(ctx, jobID, func(run *gracefulRunState) bool { return run.Status == "running" })
	if err != nil {
		return fmt.Errorf("wait for graceful completion handler to run: %w", err)
	}
	queuedName := chaosJobName("graceful-shutdown-no-second-claim")
	queuedStatus, queuedJobID, err := r.submitChaosJob(ctx, queuedName, "simulate_cpu", map[string]any{"duration_ms": 50}, 10, 1)
	if err != nil {
		return fmt.Errorf("submit second-claim sentinel: %w", err)
	}
	if err := c.check("second-claim sentinel accepted", queuedStatus == 201, fmt.Sprintf("HTTP %d", queuedStatus), "HTTP 201"); err != nil {
		return err
	}
	queued, err := r.waitGracefulRun(ctx, queuedJobID, func(run *gracefulRunState) bool { return run.Status == "assigned" })
	if err != nil {
		return fmt.Errorf("wait for second-claim sentinel to be assigned but unclaimed: %w", err)
	}
	if active.LeaseExpiresAt.IsZero() || !active.LeaseExpiresAt.After(time.Now()) {
		return fmt.Errorf("active completion run has no live lease before SIGTERM: %+v", active)
	}
	if queued.Attempt != 0 || queued.AssignedWorkerID != worker.id {
		return fmt.Errorf("second-claim sentinel was already claimed or assigned elsewhere before SIGTERM: %+v", queued)
	}
	if err := sleepUntilOrCancel(ctx, 100*time.Millisecond); err != nil {
		return err
	}

	shutdownStarted := time.Now()
	if err := signalGracefulWorker(process); err != nil {
		return fmt.Errorf("send SIGTERM to worker %s: %w", worker.id, err)
	}
	if err := r.waitGracefulWorkerStatus(ctx, worker.id, "draining"); err != nil {
		return fmt.Errorf("worker did not persist draining status after SIGTERM: %w", err)
	}
	if err := c.check("worker enters draining state", true, "database worker status=draining", "database worker status=draining"); err != nil {
		return err
	}
	processErr, err := waitGracefulProcessExit(ctx, process, gracefulProcessBound)
	if err != nil {
		return fmt.Errorf("wait for graceful worker exit: %w", err)
	}
	shutdownElapsed := time.Since(shutdownStarted)
	if err := c.check("process exits within shutdown bound", shutdownElapsed <= gracefulWorkerShutdown+time.Second,
		shutdownElapsed.String(), fmt.Sprintf("<=%s (2s grace plus cleanup)", gracefulWorkerShutdown+time.Second)); err != nil {
		return err
	}
	if err := c.check("worker process exits cleanly", processErr == nil,
		fmt.Sprint(processErr), "exit code 0 after handled SIGTERM"); err != nil {
		return err
	}

	terminal, err := r.waitChaosTerminal(ctx, jobID)
	if err != nil {
		return fmt.Errorf("wait for in-flight completion during drain: %w", err)
	}
	if err := c.check("in-flight run succeeds during drain", terminal.Status == "succeeded" && terminal.Attempt == 1,
		fmt.Sprintf("status=%s attempt=%d", terminal.Status, terminal.Attempt), "status=succeeded attempt=1"); err != nil {
		return err
	}
	if err := c.check("drained completion result is from attempt one", terminal.Result["attempt"] == float64(1),
		fmt.Sprintf("result attempt=%v", terminal.Result["attempt"]), "result attempt=1"); err != nil {
		return err
	}
	queued, err = r.readGracefulRun(ctx, queuedJobID)
	if err != nil {
		return fmt.Errorf("read second-claim sentinel after worker exit: %w", err)
	}
	if err := c.check("no second claim during shutdown", queued != nil && queued.Status == "assigned" && queued.Attempt == 0 && queued.AssignedWorkerID == worker.id,
		formatGracefulRun(queued), "status=assigned attempt=0 still owned by draining worker"); err != nil {
		return err
	}
	if _, err := r.dataPool.Exec(ctx, `DELETE FROM jobs WHERE id = $1::uuid`, queuedJobID); err != nil {
		return fmt.Errorf("remove unclaimed sentinel job from isolated schema: %w", err)
	}
	return nil
}

func (c *chaosCase) gracefulShutdownStaleAttempt(ctx context.Context) error {
	r := c.runner
	oldWorker, oldProcess, err := c.startGracefulWorker(ctx, "stale-old", gracefulWorkerLease)
	if err != nil {
		return err
	}

	name := chaosJobName("graceful-shutdown-stale-completion")
	status, jobID, err := r.submitChaosJob(ctx, name, "stale_completion_probe", map[string]any{
		"first_attempt_duration_ms": 5000,
		"later_attempt_duration_ms": 50,
	}, 15, 2)
	if err != nil {
		return fmt.Errorf("submit stale-completion probe: %w", err)
	}
	if err := c.check("stale-completion probe accepted", status == 201, fmt.Sprintf("HTTP %d", status), "HTTP 201"); err != nil {
		return err
	}
	first, err := r.waitGracefulRun(ctx, jobID, func(run *gracefulRunState) bool { return run.Status == "running" && run.Attempt == 1 })
	if err != nil {
		return fmt.Errorf("wait for first stale-completion attempt to run: %w", err)
	}
	if first.ExecutionKey == "" || first.StartedAt.IsZero() {
		return fmt.Errorf("first attempt is missing its stable execution key or start time: %+v", first)
	}
	if err := sleepUntilOrCancel(ctx, 100*time.Millisecond); err != nil {
		return err
	}

	shutdownStarted := time.Now()
	if err := signalGracefulWorker(oldProcess); err != nil {
		return fmt.Errorf("send SIGTERM to old worker %s: %w", oldWorker.id, err)
	}
	if err := r.waitGracefulWorkerStatus(ctx, oldWorker.id, "draining"); err != nil {
		return fmt.Errorf("old worker did not persist draining status: %w", err)
	}
	processErr, err := waitGracefulProcessExit(ctx, oldProcess, gracefulProcessBound)
	if err != nil {
		return fmt.Errorf("wait for old worker exit: %w", err)
	}
	shutdownElapsed := time.Since(shutdownStarted)
	if err := c.check("ignored-handler worker exits within shutdown bound", shutdownElapsed <= gracefulWorkerShutdown+time.Second,
		shutdownElapsed.String(), fmt.Sprintf("<=%s (2s grace plus cleanup)", gracefulWorkerShutdown+time.Second)); err != nil {
		return err
	}
	if err := c.check("old worker process exits cleanly", processErr == nil,
		fmt.Sprint(processErr), "exit code 0 after handled SIGTERM"); err != nil {
		return err
	}

	stale, err := r.readGracefulRun(ctx, jobID)
	if err != nil {
		return fmt.Errorf("read first run after old process exit: %w", err)
	}
	if err := c.check("old attempt remains running under a live lease after exit",
		stale != nil && stale.ID == first.ID && stale.Status == "running" && stale.Attempt == 1 &&
			stale.LeasedBy == oldWorker.id && stale.LeaseExpiresAt.After(time.Now()),
		formatGracefulRun(stale), "attempt=1 status=running old worker lease still live"); err != nil {
		return err
	}
	if stale == nil {
		return fmt.Errorf("first stale-completion run disappeared after the process exited")
	}
	leaseExpiry := stale.LeaseExpiresAt

	_, _, err = c.startGracefulWorker(ctx, "stale-recovery", gracefulWorkerLease)
	if err != nil {
		return fmt.Errorf("start recovery worker: %w", err)
	}
	for time.Now().Before(leaseExpiry) {
		current, err := r.readGracefulRun(ctx, jobID)
		if err != nil {
			return fmt.Errorf("check lease before expiry: %w", err)
		}
		if current == nil || current.ID != first.ID || current.Status != "running" || current.Attempt != 1 || current.LeaseExpiresAt.After(leaseExpiry) {
			return fmt.Errorf("old run changed before its persisted lease expiry %s: %s", leaseExpiry.UTC().Format(time.RFC3339Nano), formatGracefulRun(current))
		}
		if err := sleepUntilOrCancel(ctx, min(100*time.Millisecond, time.Until(leaseExpiry))); err != nil {
			return err
		}
	}
	if err := c.check("run is not reclaimed before lease expiry", time.Now().After(leaseExpiry),
		"waited until "+leaseExpiry.UTC().Format(time.RFC3339Nano), "old lease expiry reached"); err != nil {
		return err
	}

	terminal, err := r.waitChaosTerminal(ctx, jobID)
	if err != nil {
		return fmt.Errorf("wait for recovery attempt to finish: %w", err)
	}
	if err := c.check("fresh worker recovers the run", terminal.Status == "succeeded" && terminal.Attempt == 2,
		fmt.Sprintf("status=%s attempt=%d", terminal.Status, terminal.Attempt), "status=succeeded attempt=2"); err != nil {
		return err
	}
	if err := c.check("recovery keeps the stable execution key", terminal.ExecutionKey == first.ExecutionKey,
		terminal.ExecutionKey, first.ExecutionKey); err != nil {
		return err
	}
	if err := c.check("recovered result belongs to attempt two", terminal.Result["attempt"] == float64(2),
		fmt.Sprintf("result attempt=%v", terminal.Result["attempt"]), "result attempt=2"); err != nil {
		return err
	}
	if err := sleepUntilOrCancel(ctx, time.Until(first.StartedAt.Add(5250*time.Millisecond))); err != nil {
		return err
	}
	final, err := r.waitGracefulRun(ctx, jobID, func(run *gracefulRunState) bool { return run.Status == "succeeded" })
	if err != nil {
		return fmt.Errorf("re-read run after the old handler's nominal completion time: %w", err)
	}
	return c.check("late stale completion cannot replace recovered result",
		final.ID == first.ID && final.Status == "succeeded" && final.Attempt == 2 && final.ExecutionKey == first.ExecutionKey && final.Result["attempt"] == float64(2),
		formatGracefulRun(final), "same run succeeded at attempt two with the stable execution key")
}

func (c *chaosCase) startGracefulWorker(ctx context.Context, role string, lease time.Duration) (workerSpec, *child, error) {
	r := c.runner
	if err := r.stopWorkers(); err != nil {
		return workerSpec{}, nil, fmt.Errorf("stop previous experiment workers: %w", err)
	}
	if err := r.stopSchedulers(); err != nil {
		return workerSpec{}, nil, fmt.Errorf("stop previous experiment schedulers: %w", err)
	}
	worker := workerSpec{id: "grace-" + safeAppName(role) + "-" + shortToken(), cpu: 2000, mem: 4096}
	metricsAddr, err := freeAddress()
	if err != nil {
		return worker, nil, fmt.Errorf("allocate worker metrics address: %w", err)
	}
	appName := "atlasexp-worker-" + safeAppName(worker.id) + "-" + shortToken()
	labels, _ := json.Marshal(map[string]string{"experiment": "true", "worker_class": "cpu"})
	env := map[string]string{
		"DATABASE_URL":                     r.processDBURL(appName),
		"WORKER_ID":                         worker.id,
		"WORKER_CONCURRENCY":                "1",
		"WORKER_CPU_CAPACITY_MILLIS":        fmt.Sprint(worker.cpu),
		"WORKER_MEMORY_CAPACITY_MB":         fmt.Sprint(worker.mem),
		"WORKER_GPU_COUNT":                  "0",
		"WORKER_GPU_MEMORY_MB":              "0",
		"WORKER_GPU_TYPE":                   "",
		"WORKER_LABELS":                     string(labels),
		"METRICS_ADDR":                      metricsAddr,
		"LEASE_DURATION":                    lease.String(),
		"POLL_INTERVAL":                     "50ms",
		"WORKER_RETRY_BASE_DELAY":           "10ms",
		"WORKER_RETRY_MAX_DELAY":            "100ms",
		"WORKER_RETRY_JITTER":               "0",
		"WORKER_SHUTDOWN_GRACE_PERIOD":      gracefulWorkerShutdown.String(),
		"EXPERIMENT_MIGRATIONS_DIR":         r.opts.migrationsDir,
	}
	if r.opts.otelEndpoint != "" {
		env["OTEL_EXPORTER_OTLP_ENDPOINT"] = r.opts.otelEndpoint
	}
	process, err := r.startProcess("worker-"+worker.id, r.opts.workerBin, appName, env)
	if err != nil {
		return worker, nil, fmt.Errorf("start worker %s: %w", worker.id, err)
	}
	r.workers = append(r.workers, process)
	if err := r.waitWorkerHeartbeat(ctx, worker.id, time.Now().Add(-time.Second)); err != nil {
		return worker, nil, fmt.Errorf("wait for worker %s registration: %w", worker.id, err)
	}
	if err := r.startScheduler("first-fit"); err != nil {
		return worker, nil, fmt.Errorf("start scheduler: %w", err)
	}
	if err := r.waitLeader(ctx, 5*time.Second); err != nil {
		return worker, nil, err
	}
	return worker, process, nil
}

func signalGracefulWorker(process *child) error {
	if process == nil || process.cmd == nil || process.cmd.Process == nil {
		return fmt.Errorf("worker process is unavailable")
	}
	return process.cmd.Process.Signal(syscall.SIGTERM)
}

func waitGracefulProcessExit(ctx context.Context, process *child, limit time.Duration) (error, error) {
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case err := <-process.done:
		return err, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("process %s did not exit within %s", process.name, limit)
	}
}

func (r *runner) waitGracefulWorkerStatus(ctx context.Context, workerID, expected string) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		var status string
		err := r.dataPool.QueryRow(ctx, `SELECT status FROM workers WHERE id = $1`, workerID).Scan(&status)
		if err == nil && status == expected {
			return nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("query worker %s status: %w", workerID, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *runner) waitGracefulRun(ctx context.Context, jobID string, predicate func(*gracefulRunState) bool) (*gracefulRunState, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		run, err := r.readGracefulRun(ctx, jobID)
		if err != nil {
			return nil, err
		}
		if run != nil && predicate(run) {
			return run, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for run %s: %w", jobID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (r *runner) readGracefulRun(ctx context.Context, jobID string) (*gracefulRunState, error) {
	var run gracefulRunState
	var resultJSON string
	err := r.dataPool.QueryRow(ctx, `
		SELECT r.id::text, r.execution_key::text, r.status, r.attempt,
		       COALESCE(r.leased_by, ''), COALESCE(r.assigned_worker_id, ''),
		       COALESCE(r.lease_expires_at, 'epoch'::timestamptz),
	       COALESCE(r.started_at, 'epoch'::timestamptz), COALESCE(r.result::text, '{}')
		FROM job_runs r
		WHERE r.job_id = $1::uuid
		ORDER BY r.created_at DESC
		LIMIT 1
	`, jobID).Scan(&run.ID, &run.ExecutionKey, &run.Status, &run.Attempt,
		&run.LeasedBy, &run.AssignedWorkerID, &run.LeaseExpiresAt, &run.StartedAt, &resultJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(resultJSON), &run.Result); err != nil {
		return nil, fmt.Errorf("decode run result: %w", err)
	}
	if run.Result == nil {
		run.Result = map[string]any{}
	}
	return &run, nil
}

func sleepUntilOrCancel(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func formatGracefulRun(run *gracefulRunState) string {
	if run == nil {
		return "run not found"
	}
	return fmt.Sprintf("id=%s status=%s attempt=%d leased_by=%q lease_expires_at=%s assigned_worker_id=%q execution_key=%q result_attempt=%v",
		run.ID, run.Status, run.Attempt, run.LeasedBy, run.LeaseExpiresAt.UTC().Format(time.RFC3339Nano),
		run.AssignedWorkerID, run.ExecutionKey, run.Result["attempt"])
}
