// Package worker implements the lease/execute/complete loop for scheduler-assigned
// job_runs, plus heartbeat and lease-reclaim background loops that keep the fleet
// healthy.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/G1lollipop/atlas/internal/metrics"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/G1lollipop/atlas/internal/tracing"
)

var errCancellationRequested = errors.New("durable run cancellation requested")
var errShutdownExpired = errors.New("worker shutdown grace period expired")

type Pool struct {
	Store         store.Store
	WorkerID      string
	Concurrency   int
	LeaseDuration time.Duration
	PollInterval  time.Duration
	// ShutdownGracePeriod is the maximum time Run allows active handlers to
	// finish after its parent context is canceled. Zero preserves immediate
	// cancellation for callers that do not configure graceful shutdown.
	ShutdownGracePeriod time.Duration
	Logger        *slog.Logger

	retryPolicy   RetryPolicy
	retryRandom   func() float64
	retryPolicyMu sync.Mutex

	capabilities   model.Worker
	hostname       string
	startedAt      time.Time
	handlers       map[string]Handler
	executionSlots chan struct{}
	resources      *resourceAdmission
	heartbeatGate  chan struct{}
	mu             sync.RWMutex
	draining       bool
	shutdownExpired atomic.Bool
	shutdownDeadline atomic.Int64
}

func NewPool(st store.Store, workerID string, concurrency int, leaseDuration, pollInterval time.Duration, log *slog.Logger) *Pool {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	return &Pool{
		Store:          st,
		WorkerID:       workerID,
		Concurrency:    concurrency,
		LeaseDuration:  leaseDuration,
		PollInterval:   pollInterval,
		Logger:         log,
		retryPolicy:    DefaultRetryPolicy(),
		retryRandom:    rand.Float64,
		hostname:       hostname,
		startedAt:      time.Now().UTC(),
		handlers:       make(map[string]Handler),
		executionSlots: make(chan struct{}, max(concurrency, 0)),
		heartbeatGate:  make(chan struct{}, 1),
		resources:      newResourceAdmission(model.Worker{}),
	}
}

// SetRetryPolicy validates and installs the worker's retry policy. Configure it
// before Run; MaxAttempts caps each job's persisted retry limit when non-zero.
func (p *Pool) SetRetryPolicy(policy RetryPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	p.retryPolicyMu.Lock()
	p.retryPolicy = policy
	p.retryPolicyMu.Unlock()
	return nil
}

// SetShutdownGracePeriod configures bounded draining after Run's parent
// context is canceled. A zero duration cancels handlers immediately.
func (p *Pool) SetShutdownGracePeriod(grace time.Duration) error {
	if grace < 0 {
		return fmt.Errorf("worker shutdown grace period must not be negative")
	}
	p.ShutdownGracePeriod = grace
	return nil
}

// SetRetryRandomSource replaces the retry jitter source. It is primarily useful
// for deterministic tests; configure it before Run.
func (p *Pool) SetRetryRandomSource(source func() float64) {
	p.retryPolicyMu.Lock()
	defer p.retryPolicyMu.Unlock()
	if source == nil {
		p.retryRandom = rand.Float64
		return
	}
	p.retryRandom = source
}

func (p *Pool) retryDelay(attempt int16) (time.Duration, error) {
	p.retryPolicyMu.Lock()
	defer p.retryPolicyMu.Unlock()
	return p.retryPolicy.Delay(attempt, p.retryRandom)
}

func (p *Pool) maxAttempts(job *model.Job) int16 {
	p.retryPolicyMu.Lock()
	policyLimit := p.retryPolicy.MaxAttempts
	p.retryPolicyMu.Unlock()
	return effectiveMaxAttempts(job.MaxAttempts, policyLimit)
}

// SetCapabilities configures the resources this worker advertises. The store
// remains authoritative for fleet-wide reservations; the pool mirrors capacity
// locally to gate concurrent handler execution.
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
	p.resources.setCapacity(capabilities)
}

// RegisterHandler binds h to jobName; executeOne looks handlers up by model.Job.Name.
func (p *Pool) RegisterHandler(jobName string, h Handler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[jobName] = h
}

// Run blocks until ctx is cancelled. Positive Concurrency runs that many bounded
// lease loops alongside one heartbeat loop and one janitor. Zero is a janitor-only
// mode; the pool does not register as an executable worker in that mode.
func (p *Pool) Run(ctx context.Context) error {
	if p.Concurrency < 0 {
		return fmt.Errorf("worker concurrency must be zero or greater, got %d", p.Concurrency)
	}
	if p.Concurrency == 0 {
		p.Logger.Info("worker running in janitor-only mode; job leasing is disabled", "worker_id", p.WorkerID)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.janitorLoop(ctx)
		}()

		<-ctx.Done()
		wg.Wait()
		return ctx.Err()
	}

	// A worker must be visible to the scheduler before it can be assigned work.
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

	// The caller's cancellation stops new claims immediately. Handler execution
	// uses a separate context below so active work can drain independently.
	claimCtx, stopClaims := context.WithCancel(ctx)
	defer stopClaims()
	runtimeCtx, stopRuntime := context.WithCancel(context.WithoutCancel(ctx))
	defer stopRuntime()
	executionCtx, cancelExecutions := context.WithCancel(runtimeCtx)
	defer cancelExecutions()

	leaseDone := make(chan struct{}, p.Concurrency)

	for i := 0; i < p.Concurrency; i++ {
		go func() {
			defer func() { leaseDone <- struct{}{} }()
			p.leaseLoop(claimCtx, executionCtx)
		}()
	}

	var backgroundWG sync.WaitGroup
	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()
		p.heartbeatLoop(runtimeCtx)
	}()

	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()
		p.janitorLoop(claimCtx)
	}()

	<-ctx.Done()
	p.beginDraining()
	stopClaims()
	var graceDeadline time.Time
	if p.ShutdownGracePeriod <= 0 {
		p.shutdownExpired.Store(true)
		cancelExecutions()
	} else {
		graceDeadline = time.Now().Add(p.ShutdownGracePeriod)
		p.shutdownDeadline.Store(graceDeadline.UnixNano())
	}
	var graceTimer *time.Timer
	if p.ShutdownGracePeriod > 0 {
		graceTimer = time.NewTimer(time.Until(graceDeadline))
		defer graceTimer.Stop()
	}
	// Persist the draining state promptly so the scheduler stops assigning work.
	// Bound this write independently in case the database is already unhealthy.
	heartbeatDeadline := graceDeadline
	if p.ShutdownGracePeriod <= 0 {
		heartbeatDeadline = time.Now().Add(250 * time.Millisecond)
	} else if requestDeadline := time.Now().Add(2 * time.Second); heartbeatDeadline.IsZero() || requestDeadline.Before(heartbeatDeadline) {
		heartbeatDeadline = requestDeadline
	}
	heartbeatCtx, cancelHeartbeat := context.WithDeadline(context.Background(), heartbeatDeadline)
	if err := p.sendHeartbeat(heartbeatCtx); err != nil {
		p.Logger.Warn("draining worker heartbeat failed", "error", err)
	}
	cancelHeartbeat()

	if p.ShutdownGracePeriod <= 0 {
		stopRuntime()
		// Give cooperative handlers and background loops a brief opportunity to
		// exit so callers retain the historical synchronous cancellation behavior.
		cleanupTimer := time.NewTimer(250 * time.Millisecond)
		completed := 0
		for completed < p.Concurrency {
			select {
			case <-leaseDone:
				completed++
			case <-cleanupTimer.C:
				completed = p.Concurrency
			}
		}
		cleanupTimer.Stop()
		backgroundWG.Wait()
		return ctx.Err()
	}

	completed := 0
	for completed < p.Concurrency {
		select {
		case <-leaseDone:
			completed++
		case <-graceTimer.C:
			// Do not release leases here. A handler that ignored cancellation may
			// still be executing, so its lease must simply expire before reclamation.
			p.shutdownExpired.Store(true)
			cancelExecutions()
			stopRuntime()
			// The remaining handler may ignore context forever. Run must still
			// return at the configured bound; lifecycle writes are fenced below.
			return ctx.Err()
		}
	}
	cancelExecutions()
	stopRuntime()
	backgroundWG.Wait()
	return ctx.Err()
}

func (p *Pool) workerRecord() model.Worker {
	p.mu.RLock()
	capabilities := p.capabilities
	status := model.WorkerStatusAlive
	if p.draining {
		status = model.WorkerStatusDraining
	}
	labels := make(map[string]string, len(capabilities.Labels))
	for key, value := range capabilities.Labels {
		labels[key] = value
	}
	p.mu.RUnlock()

	return model.Worker{
		ID:               p.WorkerID,
		Hostname:         p.hostname,
		Status:           status,
		StartedAt:        p.startedAt,
		CPUCapacity:      capabilities.CPUCapacity,
		MemoryCapacityMB: capabilities.MemoryCapacityMB,
		GPUCount:         capabilities.GPUCount,
		GPUType:          capabilities.GPUType,
		GPUMemoryMB:      capabilities.GPUMemoryMB,
		Labels:           labels,
	}
}

func (p *Pool) beginDraining() {
	p.mu.Lock()
	p.draining = true
	p.mu.Unlock()
}

func (p *Pool) shutdownHasExpired() bool {
	if p.shutdownExpired.Load() {
		return true
	}
	deadline := p.shutdownDeadline.Load()
	return deadline != 0 && time.Now().UnixNano() >= deadline
}

func (p *Pool) sendHeartbeat(ctx context.Context) error {
	if p.heartbeatGate != nil {
		select {
		case p.heartbeatGate <- struct{}{}:
			defer func() { <-p.heartbeatGate }()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.Store.UpsertWorkerHeartbeat(ctx, p.workerRecord())
}

func (p *Pool) heartbeatRequestTimeout() time.Duration {
	timeout := p.PollInterval * 4
	if timeout < 100*time.Millisecond {
		timeout = 100 * time.Millisecond
	}
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	return timeout
}

func (p *Pool) leaseRenewalRequestTimeout() time.Duration {
	timeout := p.LeaseDuration / 2
	if timeout <= 0 {
		timeout = time.Second
	}
	if timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	return timeout
}

// leaseLoop is the body run by each of the Concurrency worker goroutines.
func (p *Pool) leaseLoop(ctx, executionCtx context.Context) {
	for {
		if !p.acquireExecutionSlot(ctx) {
			return
		}

		var run *model.JobRun
		var job *model.Job
		var err error
		func() {
			defer p.releaseExecutionSlot()
			run, job, err = p.Store.LeaseNextRun(ctx, p.WorkerID, p.LeaseDuration)
			if err == nil && run != nil && ctx.Err() == nil {
				p.executeOne(executionCtx, run, job)
			}
		}()
		if ctx.Err() != nil {
			return
		}
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
	}
}

// acquireExecutionSlot reserves one bounded execution slot before the worker
// asks the store for a lease. This avoids holding a database lease while waiting
// for an in-process goroutine slot.
func (p *Pool) acquireExecutionSlot(ctx context.Context) bool {
	if p.executionSlots == nil {
		return false
	}
	select {
	case p.executionSlots <- struct{}{}:
		// If cancellation raced with the send, return the slot rather than
		// starting another store call during shutdown.
		if ctx.Err() != nil {
			p.releaseExecutionSlot()
			return false
		}
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *Pool) releaseExecutionSlot() {
	<-p.executionSlots
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
	if p.shutdownHasExpired() {
		return
	}
	ctx = tracing.ContextWithTraceContext(ctx, run.TraceParent, run.TraceState)
	ctx, span := otel.Tracer("atlas/worker").Start(ctx, "worker.executeOne", trace.WithAttributes(
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
		if err := p.persistFailure(ctx, run, msg, false, 0); err != nil {
			p.Logger.Error("fail run failed", "run_id", run.ID, "error", err)
			return
		}
		metrics.JobsFailed.Inc()
		if err := p.persistDeadLetter(ctx, run, "no handler"); err != nil {
			p.Logger.Error("mark dead failed", "run_id", run.ID, "error", err)
			return
		}
		metrics.DeadLetterTotal.Inc()
		metrics.RunsCompleted.WithLabelValues("dead").Inc()
		span.SetStatus(codes.Error, msg)
		p.Logger.Error("job run dead: no handler registered",
			"run_id", run.ID, "job_id", job.ID, "job_name", job.Name)
		return
	}

	releaseResources, err := p.resources.tryAcquire(job)
	if err != nil {
		// LeaseNextRun has already atomically reserved this run in the database.
		// This local guard is a second line of defense for one process; requeue a
		// mismatch so the database reservation is released immediately.
		p.rejectResourceAdmission(ctx, run, job, err, span)
		return
	}
	defer releaseResources()

	if p.shutdownHasExpired() {
		return
	}
	if err := p.Store.MarkRunning(ctx, run.ID, p.WorkerID, run.Attempt); err != nil {
		if errors.Is(err, store.ErrCanceled) {
			p.recordCanceled(run)
			return
		}
		p.Logger.Error("mark running failed", "run_id", run.ID, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "mark running failed")
		return
	}
	metrics.RunsLeased.WithLabelValues(p.WorkerID).Inc()
	metrics.JobQueueWaitSeconds.WithLabelValues(metrics.ResourceClass(job.RequiredGPUCount)).Observe(
		metrics.QueueWaitDurationSeconds(job.CreatedAt, run.ScheduledAt, time.Now(), job.CronExpr == nil && run.Attempt == 1),
	)
	if p.shutdownHasExpired() {
		return
	}

	runCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutSeconds)*time.Second)
	defer cancel()
	stopLeaseRenewal := p.startLeaseRenewal(runCtx, cancel, run.ID, run.Attempt)

	start := time.Now()
	handlerCtx, handlerSpan := otel.Tracer("atlas/worker").Start(runCtx, "worker.handler", trace.WithAttributes(
		attribute.String("job.id", job.ID),
		attribute.String("run.id", run.ID),
		attribute.Int("run.attempt", int(run.Attempt)),
	))
	result, err := h(handlerCtx, job, run)
	handlerErr := err
	if handlerErr == nil && handlerCtx.Err() != nil {
		handlerErr = handlerCtx.Err()
	}
	finishWorkerSpan(handlerSpan, handlerErr)
	leaseErr := stopLeaseRenewal()
	executionSeconds := time.Since(start).Seconds()
	metrics.RunDuration.Observe(executionSeconds)
	metrics.JobExecutionSeconds.Observe(executionSeconds)
	if p.shutdownHasExpired() {
		// The lease is left for expiry and recovery. A handler that returned after
		// the shutdown deadline no longer has authority to write run state.
		return
	}
	if errors.Is(leaseErr, errCancellationRequested) {
		if p.shutdownHasExpired() {
			return
		}
		if cerr := p.Store.MarkCanceled(ctx, run.ID, p.WorkerID, run.Attempt); cerr != nil && !errors.Is(cerr, store.ErrNotFound) {
			p.Logger.Error("acknowledge run cancellation failed", "run_id", run.ID, "error", cerr)
		}
		p.recordCanceled(run)
		return
	}
	if leaseErr != nil {
		// A failed renewal makes ownership uncertain. The janitor is responsible
		// for recovery; do not write a completion or failure transition for a
		// lease that may already belong to another attempt.
		span.RecordError(leaseErr)
		span.SetStatus(codes.Error, "lease renewal failed")
		p.Logger.Warn("stopped handler after lease renewal failed",
			"run_id", run.ID, "attempt", run.Attempt, "error", leaseErr)
		return
	}

	// Defensive: a handler that ignores ctx and returns nil error on a timed-out
	// run should still be treated as a failure, not a success.
	if err == nil && runCtx.Err() != nil {
		err = runCtx.Err()
	}

	if err == nil {
		if cerr := p.persistResult(ctx, run, result); cerr != nil {
			if errors.Is(cerr, store.ErrCanceled) {
				p.recordCanceled(run)
				return
			}
			p.Logger.Error("complete run failed", "run_id", run.ID, "error", cerr)
			span.RecordError(cerr)
			span.SetStatus(codes.Error, "complete run failed")
			return
		}
		metrics.RunsCompleted.WithLabelValues("succeeded").Inc()
		metrics.JobsCompleted.Inc()
		span.SetStatus(codes.Ok, "")
		p.Logger.Info("job run succeeded", "run_id", run.ID, "job_id", job.ID, "job_name", job.Name)
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())

	// run.Attempt already reflects this attempt (LeaseNextRun increments it),
	// so comparing it directly to MaxAttempts tells us if another try remains.
	maxAttempts := p.maxAttempts(job)
	if run.Attempt < maxAttempts {
		backoff, backoffErr := p.retryDelay(run.Attempt)
		if backoffErr != nil {
			p.Logger.Error("calculate retry delay failed; retrying immediately", "run_id", run.ID, "error", backoffErr)
			backoff = 0
		}
		if ferr := p.persistFailure(ctx, run, err.Error(), true, backoff); ferr != nil {
			if errors.Is(ferr, store.ErrCanceled) {
				p.recordCanceled(run)
				return
			}
			p.Logger.Error("fail run failed", "run_id", run.ID, "error", ferr)
			return
		}
		metrics.RunsCompleted.WithLabelValues("failed").Inc()
		metrics.JobsFailed.Inc()
		metrics.RetryTotal.Inc()
		p.Logger.Warn("job run failed, will retry",
			"run_id", run.ID, "job_id", job.ID, "job_name", job.Name,
			"attempt", run.Attempt, "max_attempts", maxAttempts,
			"backoff", backoff, "error", err)
		return
	}

	if ferr := p.persistFailure(ctx, run, err.Error(), false, 0); ferr != nil {
		if errors.Is(ferr, store.ErrCanceled) {
			p.recordCanceled(run)
			return
		}
		p.Logger.Error("fail run failed", "run_id", run.ID, "error", ferr)
		return
	}
	metrics.JobsFailed.Inc()
	if derr := p.persistDeadLetter(ctx, run, "max attempts exceeded"); derr != nil {
		p.Logger.Error("mark dead failed", "run_id", run.ID, "error", derr)
		return
	}
	metrics.DeadLetterTotal.Inc()
	metrics.RunsCompleted.WithLabelValues("dead").Inc()
	span.SetStatus(codes.Error, err.Error())
	p.Logger.Error("job run dead: max attempts exceeded",
		"run_id", run.ID, "job_id", job.ID, "job_name", job.Name,
		"attempt", run.Attempt, "max_attempts", maxAttempts, "error", err)
}

func (p *Pool) persistResult(ctx context.Context, run *model.JobRun, result map[string]any) error {
	if p.shutdownHasExpired() {
		return errShutdownExpired
	}
	ctx, span := otel.Tracer("atlas/worker").Start(ctx, "worker.result.persist", trace.WithAttributes(
		attribute.String("run.id", run.ID),
		attribute.Int("run.attempt", int(run.Attempt)),
	))
	err := p.Store.CompleteRun(ctx, run.ID, p.WorkerID, run.Attempt, result)
	finishWorkerSpan(span, err)
	return err
}

func (p *Pool) persistFailure(ctx context.Context, run *model.JobRun, message string, requeue bool, backoff time.Duration) error {
	if p.shutdownHasExpired() {
		return errShutdownExpired
	}
	ctx, span := otel.Tracer("atlas/worker").Start(ctx, "worker.failure.persist", trace.WithAttributes(
		attribute.String("run.id", run.ID),
		attribute.Int("run.attempt", int(run.Attempt)),
		attribute.Bool("run.requeued", requeue),
	))
	err := p.Store.FailRun(ctx, run.ID, p.WorkerID, run.Attempt, message, requeue, backoff)
	finishWorkerSpan(span, err)
	return err
}

func (p *Pool) persistDeadLetter(ctx context.Context, run *model.JobRun, reason string) error {
	if p.shutdownHasExpired() {
		return errShutdownExpired
	}
	ctx, span := otel.Tracer("atlas/worker").Start(ctx, "worker.dead_letter.persist", trace.WithAttributes(
		attribute.String("run.id", run.ID),
		attribute.Int("run.attempt", int(run.Attempt)),
	))
	err := p.Store.MarkDead(ctx, run.ID, reason)
	finishWorkerSpan(span, err)
	return err
}

func finishWorkerSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

func (p *Pool) recordCanceled(run *model.JobRun) {
	metrics.RunsCompleted.WithLabelValues("canceled").Inc()
	p.Logger.Info("job run canceled", "run_id", run.ID)
}

func (p *Pool) rejectResourceAdmission(ctx context.Context, run *model.JobRun, job *model.Job, admissionErr error, span trace.Span) {
	message := "worker resource admission rejected: " + admissionErr.Error()
	span.RecordError(admissionErr)
	span.SetStatus(codes.Error, "resource admission rejected")

	maxAttempts := p.maxAttempts(job)
	if run.Attempt < maxAttempts {
		backoff, backoffErr := p.retryDelay(run.Attempt)
		if backoffErr != nil {
			p.Logger.Error("calculate retry delay failed; retrying immediately", "run_id", run.ID, "error", backoffErr)
			backoff = 0
		}
		if err := p.persistFailure(ctx, run, message, true, backoff); err != nil {
			if errors.Is(err, store.ErrCanceled) {
				p.recordCanceled(run)
				return
			}
			p.Logger.Error("requeue run after resource admission rejection failed",
				"run_id", run.ID, "admission_error", admissionErr, "error", err)
			return
		}
		metrics.RunsCompleted.WithLabelValues("failed").Inc()
		metrics.JobsFailed.Inc()
		metrics.RetryTotal.Inc()
		p.Logger.Error("run rejected by local resource admission; retrying",
			"run_id", run.ID, "job_id", job.ID, "attempt", run.Attempt,
			"max_attempts", maxAttempts, "error", admissionErr, "backoff", backoff)
		return
	}

	if err := p.persistFailure(ctx, run, message, false, 0); err != nil {
		if errors.Is(err, store.ErrCanceled) {
			p.recordCanceled(run)
			return
		}
		p.Logger.Error("fail run after resource admission rejection failed",
			"run_id", run.ID, "admission_error", admissionErr, "error", err)
		return
	}
	metrics.JobsFailed.Inc()
	if err := p.persistDeadLetter(ctx, run, "worker resource admission rejected after max attempts"); err != nil {
		p.Logger.Error("mark resource-rejected run dead failed", "run_id", run.ID, "error", err)
		return
	}
	metrics.DeadLetterTotal.Inc()
	metrics.RunsCompleted.WithLabelValues("dead").Inc()
	p.Logger.Error("run rejected by local resource admission after max attempts",
		"run_id", run.ID, "job_id", job.ID, "attempt", run.Attempt,
		"max_attempts", maxAttempts, "error", admissionErr)
}

// startLeaseRenewal keeps an executing run leased while its handler is active.
// A failed renewal cancels the handler and stops renewal. The returned function
// joins the goroutine before executeOne performs its final lifecycle transition.
func (p *Pool) startLeaseRenewal(ctx context.Context, cancelHandler context.CancelFunc, runID string, attempt int16) func() error {
	interval := p.LeaseDuration / 3
	if interval <= 0 {
		interval = time.Second
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	var stopOnce sync.Once
	var renewalErr error
	var errMu sync.Mutex
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				if p.shutdownHasExpired() {
					cancelHandler()
					return
				}
				requestCtx, cancelRequest := context.WithTimeout(ctx, p.leaseRenewalRequestTimeout())
				requested, err := p.Store.CancellationRequested(requestCtx, runID, p.WorkerID, attempt)
				if err == nil && requested {
					cancelRequest()
					errMu.Lock()
					renewalErr = errCancellationRequested
					errMu.Unlock()
					cancelHandler()
					return
				}
				if err == nil && p.LeaseDuration > 0 {
					err = p.Store.ExtendLease(requestCtx, runID, p.WorkerID, attempt, p.LeaseDuration)
				}
				cancelRequest()
				if err != nil {
					// Handler timeout or shutdown cancels the renewal request too.
					// Let executeOne handle that cancellation as a normal run
					// failure; only a live context's renewal error means the
					// lease may have been lost independently of the handler.
					if ctx.Err() != nil {
						return
					}
					errMu.Lock()
					renewalErr = err
					errMu.Unlock()
					if errors.Is(err, store.ErrNotFound) {
						p.Logger.Warn("lease ownership lost during renewal", "run_id", runID, "attempt", attempt)
					} else {
						p.Logger.Error("lease renewal failed", "run_id", runID, "attempt", attempt, "error", err)
					}
					cancelHandler()
					return
				}
			}
		}
	}()
	return func() error {
		stopOnce.Do(func() { close(stop) })
		<-done
		errMu.Lock()
		defer errMu.Unlock()
		return renewalErr
	}
}
