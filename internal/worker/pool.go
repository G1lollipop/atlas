// Package worker implements the lease/execute/complete loop that turns pending
// job_runs into finished (or retried, or dead-lettered) ones, plus the
// heartbeat and lease-reclaim background loops that keep the fleet healthy.
package worker

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/G1lollipop/atlas/internal/metrics"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
)

var tracer = otel.Tracer("atlas/worker")

const maxBackoff = 5 * time.Minute

type Pool struct {
	Store         store.Store
	WorkerID      string
	Concurrency   int
	LeaseDuration time.Duration
	PollInterval  time.Duration
	Logger        *slog.Logger

	capabilities model.Worker
	hostname     string
	startedAt    time.Time
	handlers     map[string]Handler
	mu           sync.RWMutex
}

func NewPool(st store.Store, workerID string, concurrency int, leaseDuration, pollInterval time.Duration, log *slog.Logger) *Pool {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	return &Pool{
		Store:         st,
		WorkerID:      workerID,
		Concurrency:   concurrency,
		LeaseDuration: leaseDuration,
		PollInterval:  pollInterval,
		Logger:        log,
		hostname:      hostname,
		startedAt:     time.Now().UTC(),
		handlers:      make(map[string]Handler),
	}
}

// SetCapabilities configures the resources this worker advertises. The Pool
// writes only capacity and labels; reservation and available-resource counters
// are derived by the store from active leases.
func (p *Pool) SetCapabilities(capabilities model.Worker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.capabilities.CPUCapacity = capabilities.CPUCapacity
	p.capabilities.MemoryCapacityMB = capabilities.MemoryCapacityMB
	p.capabilities.GPUCount = capabilities.GPUCount
	p.capabilities.GPUType = capabilities.GPUType
	p.capabilities.GPUMemoryMB = capabilities.GPUMemoryMB
	p.capabilities.Labels = make(map[string]string, len(capabilities.Labels))
	for key, value := range capabilities.Labels {
		p.capabilities.Labels[key] = value
	}
}

// RegisterHandler binds h to jobName; executeOne looks handlers up by model.Job.Name.
func (p *Pool) RegisterHandler(jobName string, h Handler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[jobName] = h
}

// Run blocks until ctx is cancelled, running Concurrency lease/execute loops
// plus one heartbeat loop and one janitor (lease-reclaim) loop.
func (p *Pool) Run(ctx context.Context) error {
	// A worker must be visible to resource matching before it can lease work.
	// Retry transient store failures here, before any leasing goroutine starts.
	for ctx.Err() == nil {
		if err := p.sendHeartbeat(ctx); err == nil {
			break
		} else {
			p.Logger.Error("initial worker registration failed", "error", err)
		}
		if !sleepCtx(ctx, p.PollInterval) {
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var wg sync.WaitGroup

	for i := 0; i < p.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.leaseLoop(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.heartbeatLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.janitorLoop(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

func (p *Pool) workerRecord() model.Worker {
	p.mu.RLock()
	capabilities := p.capabilities
	labels := make(map[string]string, len(capabilities.Labels))
	for key, value := range capabilities.Labels {
		labels[key] = value
	}
	p.mu.RUnlock()

	return model.Worker{
		ID:               p.WorkerID,
		Hostname:         p.hostname,
		Status:           model.WorkerStatusAlive,
		StartedAt:        p.startedAt,
		CPUCapacity:      capabilities.CPUCapacity,
		MemoryCapacityMB: capabilities.MemoryCapacityMB,
		GPUCount:         capabilities.GPUCount,
		GPUType:          capabilities.GPUType,
		GPUMemoryMB:      capabilities.GPUMemoryMB,
		Labels:           labels,
	}
}

func (p *Pool) sendHeartbeat(ctx context.Context) error {
	return p.Store.UpsertWorkerHeartbeat(ctx, p.workerRecord())
}

// leaseLoop is the body run by each of the Concurrency worker goroutines.
func (p *Pool) leaseLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		run, job, err := p.Store.LeaseNextRun(ctx, p.WorkerID, p.LeaseDuration)
		if err != nil {
			p.Logger.Error("lease next run failed", "error", err)
			if !sleepCtx(ctx, p.PollInterval) {
				return
			}
			continue
		}
		if run == nil {
			if !sleepCtx(ctx, p.PollInterval) {
				return
			}
			continue
		}

		p.executeOne(ctx, run, job)
	}
}

// sleepCtx sleeps for d or until ctx is done, whichever comes first, reporting
// which happened so callers can stop promptly on shutdown instead of finishing
// out a poll-interval sleep first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *Pool) executeOne(ctx context.Context, run *model.JobRun, job *model.Job) {
	ctx, span := tracer.Start(ctx, "worker.executeOne", trace.WithAttributes(
		attribute.String("job.id", job.ID),
		attribute.String("job.name", job.Name),
		attribute.String("run.id", run.ID),
		attribute.Int("run.attempt", int(run.Attempt)),
	))
	defer span.End()

	p.mu.RLock()
	h, ok := p.handlers[job.Name]
	p.mu.RUnlock()

	if !ok {
		msg := "no handler registered for job: " + job.Name
		if err := p.Store.FailRun(ctx, run.ID, p.WorkerID, run.Attempt, msg, false, 0); err != nil {
			p.Logger.Error("fail run failed", "run_id", run.ID, "error", err)
			return
		}
		if err := p.Store.MarkDead(ctx, run.ID, "no handler"); err != nil {
			p.Logger.Error("mark dead failed", "run_id", run.ID, "error", err)
			return
		}
		metrics.RunsCompleted.WithLabelValues("dead").Inc()
		span.SetStatus(codes.Error, msg)
		p.Logger.Error("job run dead: no handler registered",
			"run_id", run.ID, "job_id", job.ID, "job_name", job.Name)
		return
	}

	if err := p.Store.MarkRunning(ctx, run.ID, p.WorkerID, run.Attempt); err != nil {
		p.Logger.Error("mark running failed", "run_id", run.ID, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "mark running failed")
		return
	}
	stopLeaseRenewal := p.startLeaseRenewal(ctx, run.ID, run.Attempt)
	metrics.RunsLeased.WithLabelValues(p.WorkerID).Inc()

	runCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutSeconds)*time.Second)
	defer cancel()

	start := time.Now()
	result, err := func() (map[string]any, error) {
		defer stopLeaseRenewal()
		return h(runCtx, job, run)
	}()
	metrics.RunDuration.Observe(time.Since(start).Seconds())

	// Defensive: a handler that ignores ctx and returns nil error on a timed-out
	// run should still be treated as a failure, not a success.
	if err == nil && runCtx.Err() != nil {
		err = runCtx.Err()
	}

	if err == nil {
		if cerr := p.Store.CompleteRun(ctx, run.ID, p.WorkerID, run.Attempt, result); cerr != nil {
			p.Logger.Error("complete run failed", "run_id", run.ID, "error", cerr)
			span.RecordError(cerr)
			span.SetStatus(codes.Error, "complete run failed")
			return
		}
		metrics.RunsCompleted.WithLabelValues("succeeded").Inc()
		span.SetStatus(codes.Ok, "")
		p.Logger.Info("job run succeeded", "run_id", run.ID, "job_id", job.ID, "job_name", job.Name)
		return
	}

	span.RecordError(err)

	// run.Attempt already reflects this attempt (LeaseNextRun increments it),
	// so comparing it directly to MaxAttempts tells us if another try remains.
	if run.Attempt < job.MaxAttempts {
		backoff := time.Duration(1<<uint(run.Attempt)) * time.Second
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		if ferr := p.Store.FailRun(ctx, run.ID, p.WorkerID, run.Attempt, err.Error(), true, backoff); ferr != nil {
			p.Logger.Error("fail run failed", "run_id", run.ID, "error", ferr)
			return
		}
		metrics.RunsCompleted.WithLabelValues("failed").Inc()
		p.Logger.Warn("job run failed, will retry",
			"run_id", run.ID, "job_id", job.ID, "job_name", job.Name,
			"attempt", run.Attempt, "max_attempts", job.MaxAttempts,
			"backoff", backoff, "error", err)
		return
	}

	if ferr := p.Store.FailRun(ctx, run.ID, p.WorkerID, run.Attempt, err.Error(), false, 0); ferr != nil {
		p.Logger.Error("fail run failed", "run_id", run.ID, "error", ferr)
		return
	}
	if derr := p.Store.MarkDead(ctx, run.ID, "max attempts exceeded"); derr != nil {
		p.Logger.Error("mark dead failed", "run_id", run.ID, "error", derr)
		return
	}
	metrics.RunsCompleted.WithLabelValues("dead").Inc()
	span.SetStatus(codes.Error, err.Error())
	p.Logger.Error("job run dead: max attempts exceeded",
		"run_id", run.ID, "job_id", job.ID, "job_name", job.Name,
		"attempt", run.Attempt, "max_attempts", job.MaxAttempts, "error", err)
}

// startLeaseRenewal keeps an executing run leased while its handler is active.
// Renewal stops and joins before executeOne returns so no background extension
// can race the final completion or failure transition.
func (p *Pool) startLeaseRenewal(ctx context.Context, runID string, attempt int16) func() {
	if p.LeaseDuration <= 0 {
		return func() {}
	}
	interval := p.LeaseDuration / 3
	if interval <= 0 {
		interval = p.LeaseDuration
	}
	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				if err := p.Store.ExtendLease(renewCtx, runID, p.WorkerID, attempt, p.LeaseDuration); err != nil {
					p.Logger.Error("extend lease failed", "run_id", runID, "error", err)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
