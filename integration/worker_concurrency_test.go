package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/scheduler"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/G1lollipop/atlas/internal/worker"
	"github.com/google/uuid"
)

const (
	concurrencyWorkerCount = 32
	concurrencyJobCount    = 96
	concurrencyWorkerCPU   = int32(1000)
)

type observedWorkerStore struct {
	store.Store

	leaseCalls            atomic.Int64
	successfulRenewals    atomic.Int64
	renewalsWhileDraining atomic.Int64
	reclaimedLeases       atomic.Int64
	drainingSeen          atomic.Bool
}

func (s *observedWorkerStore) LeaseNextRun(ctx context.Context, workerID string, lease time.Duration) (*model.JobRun, *model.Job, error) {
	s.leaseCalls.Add(1)
	return s.Store.LeaseNextRun(ctx, workerID, lease)
}

func (s *observedWorkerStore) ExtendLease(ctx context.Context, runID, workerID string, attempt int16, lease time.Duration) error {
	err := s.Store.ExtendLease(ctx, runID, workerID, attempt, lease)
	if err == nil {
		s.successfulRenewals.Add(1)
		if s.drainingSeen.Load() {
			s.renewalsWhileDraining.Add(1)
		}
	}
	return err
}

func (s *observedWorkerStore) ReclaimExpiredLeases(ctx context.Context) (int, error) {
	n, err := s.Store.ReclaimExpiredLeases(ctx)
	s.reclaimedLeases.Add(int64(n))
	return n, err
}

func (s *observedWorkerStore) UpsertWorkerHeartbeat(ctx context.Context, heartbeat model.Worker) error {
	if heartbeat.Status == model.WorkerStatusDraining {
		s.drainingSeen.Store(true)
	}
	return s.Store.UpsertWorkerHeartbeat(ctx, heartbeat)
}

func openWorkerConcurrencyTestStore(t *testing.T) (*store.PostgresStore, context.Context) {
	t.Helper()
	databaseURL := os.Getenv("ATLAS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set ATLAS_TEST_DATABASE_URL to run Postgres worker concurrency integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	admin, err := store.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to ATLAS_TEST_DATABASE_URL: %v", err)
	}
	schema := "atlas_worker_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Pool().Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatalf("create isolated worker schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Pool().Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("drop isolated worker schema %s: %v", schema, err)
		}
		admin.Close()
	})

	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse ATLAS_TEST_DATABASE_URL: %v", err)
	}
	query := parsedURL.Query()
	query.Set("search_path", schema)
	parsedURL.RawQuery = query.Encode()
	st, err := store.New(ctx, parsedURL.String())
	if err != nil {
		t.Fatalf("connect to isolated worker schema: %v", err)
	}
	t.Cleanup(st.Close)
	if err := store.RunMigrations(ctx, st.Pool(), "../migrations"); err != nil {
		t.Fatalf("run migrations in isolated worker schema: %v", err)
	}
	var currentSchema string
	var isolatedJobsTable bool
	if err := st.Pool().QueryRow(ctx, `SELECT current_schema(), to_regclass($1 || '.jobs') IS NOT NULL`, schema).
		Scan(&currentSchema, &isolatedJobsTable); err != nil {
		t.Fatalf("verify isolated schema migrations: %v", err)
	}
	if currentSchema != schema || !isolatedJobsTable {
		t.Fatalf("migration search path escaped isolated schema: current_schema=%q isolated_jobs_table=%t", currentSchema, isolatedJobsTable)
	}
	return st, ctx
}

func TestThirtyTwoWorkersProcessNinetySixResourceBoundRuns(t *testing.T) {
	st, ctx := openWorkerConcurrencyTestStore(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	observed := &observedWorkerStore{Store: st}
	workerIDs := make([]string, 0, concurrencyWorkerCount)
	for i := 0; i < concurrencyWorkerCount; i++ {
		workerID := fmt.Sprintf("concurrency-%02d-%s", i, uuid.NewString())
		workerIDs = append(workerIDs, workerID)
		if err := st.UpsertWorkerHeartbeat(ctx, model.Worker{
			ID: workerID, Hostname: "worker-concurrency-test", Status: model.WorkerStatusAlive,
			CPUCapacity: concurrencyWorkerCPU, MemoryCapacityMB: 1024,
		}); err != nil {
			t.Fatalf("register worker %s: %v", workerID, err)
		}
	}

	for i := 0; i < concurrencyJobCount; i++ {
		_, err := st.CreateJob(ctx, model.NewJobInput{
			Name: "worker-concurrency-handler", Payload: map[string]any{"index": i},
			RequiredCPUMillis: concurrencyWorkerCPU, RequiredMemoryMB: 128,
			MaxAttempts: 1, TimeoutSeconds: 15,
		})
		if err != nil {
			t.Fatalf("create concurrency job %d: %v", i, err)
		}
	}

	promoter := scheduler.NewPromoter(observed, nil, log, 10*time.Millisecond)
	promoter.Policy = scheduler.LeastLoaded{}
	if promoted, err := promoter.PromoteOnce(ctx); err != nil || promoted != concurrencyJobCount {
		t.Fatalf("PromoteOnce() = %d, %v; want %d, nil", promoted, err, concurrencyJobCount)
	}
	assigned, err := promoter.DispatchOnce(ctx)
	if err != nil || assigned != concurrencyWorkerCount {
		t.Fatalf("initial DispatchOnce() = %d, %v; want %d assignments", assigned, err, concurrencyWorkerCount)
	}
	totalAssigned := assigned
	assertWorkerReservationCeilings(t, ctx, st, concurrencyWorkerCount, concurrencyWorkerCPU, true)

	var stateMu sync.Mutex
	activeRuns := make(map[string]bool)
	handlerCalls := make(map[string]int)
	var activeCount, peakActive int
	var handlerIssues []string
	firstBatchStarted := atomic.Int32{}
	firstBatchReady := make(chan struct{})
	firstBatchRelease := make(chan struct{})
	var releaseFirstBatchOnce sync.Once
	releaseFirstBatch := func() { releaseFirstBatchOnce.Do(func() { close(firstBatchRelease) }) }
	var poolCancel context.CancelFunc
	poolDone := make(chan error, concurrencyWorkerCount)
	poolStarted := false
	var stopPoolsOnce sync.Once
	stopPools := func() {
		stopPoolsOnce.Do(func() {
			if poolCancel != nil {
				poolCancel()
			}
			releaseFirstBatch()
			if !poolStarted {
				return
			}
			shutdownTimer := time.NewTimer(5 * time.Second)
			defer shutdownTimer.Stop()
			for i := 0; i < concurrencyWorkerCount; i++ {
				select {
				case err := <-poolDone:
					if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("worker Pool.Run() error = %v", err)
					}
				case <-shutdownTimer.C:
					t.Errorf("timed out waiting for worker pools to stop (%d remain)", concurrencyWorkerCount-i)
					return
				}
			}
		})
	}
	defer stopPools()

	poolCtx, cancel := context.WithCancel(ctx)
	poolCancel = cancel
	for _, workerID := range workerIDs {
		workerID := workerID
		pool := worker.NewPool(observed, workerID, 1, 5*time.Second, 15*time.Millisecond, log)
		pool.SetCapabilities(model.Worker{CPUCapacity: concurrencyWorkerCPU, MemoryCapacityMB: 1024})
		pool.RegisterHandler("worker-concurrency-handler", func(handlerCtx context.Context, _ *model.Job, run *model.JobRun) (map[string]any, error) {
			stateMu.Lock()
			if activeRuns[run.ID] {
				handlerIssues = append(handlerIssues, "run "+run.ID+" was active in more than one handler")
			}
			activeRuns[run.ID] = true
			handlerCalls[run.ID]++
			activeCount++
			if activeCount > peakActive {
				peakActive = activeCount
			}
			stateMu.Unlock()
			defer func() {
				stateMu.Lock()
				delete(activeRuns, run.ID)
				activeCount--
				stateMu.Unlock()
			}()

			current, err := st.GetRun(handlerCtx, run.ID)
			if err != nil {
				stateMu.Lock()
				handlerIssues = append(handlerIssues, fmt.Sprintf("read active run %s: %v", run.ID, err))
				stateMu.Unlock()
			} else if current.Status != model.RunStatusRunning || current.Attempt != run.Attempt || current.LeasedBy == nil ||
				*current.LeasedBy != workerID || current.LeaseExpiresAt == nil || !current.LeaseExpiresAt.After(time.Now()) {
				stateMu.Lock()
				handlerIssues = append(handlerIssues, fmt.Sprintf("run %s did not have a valid live lease in its handler: %+v", run.ID, current))
				stateMu.Unlock()
			}
			if run.Attempt != 1 {
				stateMu.Lock()
				handlerIssues = append(handlerIssues, fmt.Sprintf("run %s started at attempt %d, want 1", run.ID, run.Attempt))
				stateMu.Unlock()
			}

			started := firstBatchStarted.Add(1)
			if started <= concurrencyWorkerCount {
				if started == concurrencyWorkerCount {
					close(firstBatchReady)
				}
				select {
				case <-firstBatchRelease:
				case <-handlerCtx.Done():
					return nil, handlerCtx.Err()
				}
			}
			timer := time.NewTimer(25 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-handlerCtx.Done():
				return nil, handlerCtx.Err()
			case <-timer.C:
			}
			return map[string]any{"completed": true}, nil
		})
		go func() { poolDone <- pool.Run(poolCtx) }()
	}
	poolStarted = true

	select {
	case <-firstBatchReady:
	case <-time.After(5 * time.Second):
		t.Fatal("all 32 workers did not enter their first leased handler")
	}
	assertWorkerReservationCeilings(t, ctx, st, concurrencyWorkerCount, concurrencyWorkerCPU, true)
	releaseFirstBatch()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		assigned, err := promoter.DispatchOnce(ctx)
		if err != nil {
			t.Fatalf("DispatchOnce() while workers run: %v", err)
		}
		totalAssigned += assigned
		assertWorkerReservationCeilings(t, ctx, st, concurrencyWorkerCount, concurrencyWorkerCPU, false)

		var total, succeeded, canceled, wrongAttempts, cancelRequested int
		if err := st.Pool().QueryRow(ctx, `
			SELECT count(*),
			       count(*) FILTER (WHERE status = 'succeeded'),
			       count(*) FILTER (WHERE status = 'canceled'),
			       count(*) FILTER (WHERE attempt <> 1),
			       count(*) FILTER (WHERE cancel_requested_at IS NOT NULL)
			FROM job_runs
		`).Scan(&total, &succeeded, &canceled, &wrongAttempts, &cancelRequested); err != nil {
			t.Fatalf("read run progress: %v", err)
		}
		if total == concurrencyJobCount && succeeded == concurrencyJobCount {
			if canceled != 0 || wrongAttempts != 0 || cancelRequested != 0 {
				t.Fatalf("run completion counts: canceled=%d wrong_attempts=%d cancel_requested=%d", canceled, wrongAttempts, cancelRequested)
			}
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	var total, succeeded, canceled, wrongAttempts, cancelRequested int
	if err := st.Pool().QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE status = 'succeeded'),
		       count(*) FILTER (WHERE status = 'canceled'),
		       count(*) FILTER (WHERE attempt <> 1),
		       count(*) FILTER (WHERE cancel_requested_at IS NOT NULL)
		FROM job_runs
	`).Scan(&total, &succeeded, &canceled, &wrongAttempts, &cancelRequested); err != nil {
		t.Fatalf("read final run counts: %v", err)
	}
	if total != concurrencyJobCount || succeeded != concurrencyJobCount || canceled != 0 || wrongAttempts != 0 || cancelRequested != 0 {
		t.Fatalf("final run counts total/succeeded/canceled/wrong_attempts/cancel_requested = %d/%d/%d/%d/%d; want %d/%d/0/0/0",
			total, succeeded, canceled, wrongAttempts, cancelRequested, concurrencyJobCount, concurrencyJobCount)
	}
	if totalAssigned != concurrencyJobCount {
		t.Fatalf("total dispatch assignments = %d, want one assignment for each of %d jobs", totalAssigned, concurrencyJobCount)
	}
	if got := observed.reclaimedLeases.Load(); got != 0 {
		t.Fatalf("expired leases reclaimed = %d, want 0", got)
	}
	stateMu.Lock()
	if len(handlerIssues) != 0 {
		stateMu.Unlock()
		t.Fatalf("handler lease/exclusivity checks failed: %v", handlerIssues)
	}
	if len(handlerCalls) != concurrencyJobCount {
		stateMu.Unlock()
		t.Fatalf("distinct runs handled = %d, want %d", len(handlerCalls), concurrencyJobCount)
	}
	for runID, calls := range handlerCalls {
		if calls != 1 {
			stateMu.Unlock()
			t.Fatalf("handler calls for run %s = %d, want exactly 1", runID, calls)
		}
	}
	if peakActive != concurrencyWorkerCount {
		stateMu.Unlock()
		t.Fatalf("peak concurrent handlers = %d, want all %d worker slots active", peakActive, concurrencyWorkerCount)
	}
	stateMu.Unlock()
	stopPools()
}

func TestConcurrentLeaseNextRunHasOneWinner(t *testing.T) {
	st, ctx := openWorkerConcurrencyTestStore(t)
	workerID := "claim-race-" + uuid.NewString()
	if err := st.UpsertWorkerHeartbeat(ctx, model.Worker{
		ID: workerID, Hostname: "claim-race-test", Status: model.WorkerStatusAlive,
		CPUCapacity: 1000, MemoryCapacityMB: 512,
	}); err != nil {
		t.Fatalf("register claim-race worker: %v", err)
	}
	job, err := st.CreateJob(ctx, model.NewJobInput{
		Name: "claim-race-" + uuid.NewString(), RequiredCPUMillis: 1000, RequiredMemoryMB: 64,
		MaxAttempts: 2, TimeoutSeconds: 5,
	})
	if err != nil {
		t.Fatalf("create claim-race job: %v", err)
	}
	run, err := st.CreateRun(ctx, job.ID, job.Priority, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("create claim-race run: %v", err)
	}
	dispatcher := scheduler.NewDispatcherWithPolicy(st, slog.New(slog.NewTextHandler(io.Discard, nil)), scheduler.LeastLoaded{})
	if assigned, err := dispatcher.DispatchOnce(ctx); err != nil || assigned != 1 {
		t.Fatalf("DispatchOnce() = %d, %v; want one assignment", assigned, err)
	}

	type leaseResult struct {
		run *model.JobRun
		job *model.Job
		err error
	}
	results := make(chan leaseResult, concurrencyWorkerCount)
	start := make(chan struct{})
	var callers sync.WaitGroup
	for i := 0; i < concurrencyWorkerCount; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-start
			leasedRun, leasedJob, err := st.LeaseNextRun(ctx, workerID, 5*time.Second)
			results <- leaseResult{run: leasedRun, job: leasedJob, err: err}
		}()
	}
	close(start)
	callers.Wait()
	close(results)
	winners := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent LeaseNextRun() error: %v", result.err)
		}
		if result.run == nil {
			if result.job != nil {
				t.Fatalf("empty lease returned unexpected job %+v", result.job)
			}
			continue
		}
		winners++
		if result.job == nil || result.run.ID != run.ID || result.run.Attempt != 1 || result.run.LeasedBy == nil || *result.run.LeasedBy != workerID {
			t.Fatalf("winning claim = run %+v, job %+v; want run %s attempt 1 owned by %s", result.run, result.job, run.ID, workerID)
		}
	}
	if winners != 1 {
		t.Fatalf("successful concurrent claims = %d, want exactly one", winners)
	}
	claimed, err := st.GetRun(ctx, run.ID)
	if err != nil || claimed.Status != model.RunStatusLeased || claimed.Attempt != 1 || claimed.LeasedBy == nil || *claimed.LeasedBy != workerID {
		t.Fatalf("persisted winning claim = %+v, %v", claimed, err)
	}
	if err := st.MarkRunning(ctx, claimed.ID, workerID, claimed.Attempt); err != nil {
		t.Fatalf("mark winning claim running: %v", err)
	}
	if err := st.CompleteRun(ctx, claimed.ID, workerID, claimed.Attempt, map[string]any{"one_winner": true}); err != nil {
		t.Fatalf("complete winning claim: %v", err)
	}
	completed, err := st.GetRun(ctx, run.ID)
	if err != nil || completed.Status != model.RunStatusSucceeded || completed.Attempt != 1 {
		t.Fatalf("completed claim = %+v, %v; want succeeded attempt 1", completed, err)
	}
}

func TestWorkerDrainRenewsLeaseAndStopsNewClaims(t *testing.T) {
	st, ctx := openWorkerConcurrencyTestStore(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	observed := &observedWorkerStore{Store: st}
	workerID := "drain-integration-" + uuid.NewString()
	if err := st.UpsertWorkerHeartbeat(ctx, model.Worker{
		ID: workerID, Hostname: "worker-drain-test", Status: model.WorkerStatusAlive,
		CPUCapacity: 1000, MemoryCapacityMB: 512,
	}); err != nil {
		t.Fatalf("register draining worker: %v", err)
	}
	jobName := "worker-drain-handler-" + uuid.NewString()
	jobIDs := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		job, err := st.CreateJob(ctx, model.NewJobInput{
			Name: jobName, RequiredCPUMillis: 1000, RequiredMemoryMB: 64,
			MaxAttempts: 1, TimeoutSeconds: 10,
		})
		if err != nil {
			t.Fatalf("create draining job %d: %v", i, err)
		}
		jobIDs = append(jobIDs, job.ID)
	}
	promoter := scheduler.NewPromoter(observed, nil, log, 10*time.Millisecond)
	promoter.Policy = scheduler.LeastLoaded{}
	if promoted, err := promoter.PromoteOnce(ctx); err != nil || promoted != len(jobIDs) {
		t.Fatalf("PromoteOnce() = %d, %v; want %d", promoted, err, len(jobIDs))
	}
	if assigned, err := promoter.DispatchOnce(ctx); err != nil || assigned != 1 {
		t.Fatalf("initial DispatchOnce() = %d, %v; want one assignment at capacity one", assigned, err)
	}
	var activeRunID, activeJobID string
	var scheduledRunID string
	for _, jobID := range jobIDs {
		runs, err := st.ListJobRuns(ctx, jobID, 1)
		if err != nil || len(runs) != 1 {
			t.Fatalf("list one-shot run for job %s: runs=%+v error=%v", jobID, runs, err)
		}
		switch runs[0].Status {
		case model.RunStatusAssigned:
			activeRunID = runs[0].ID
			activeJobID = jobID
		case model.RunStatusScheduled:
			scheduledRunID = runs[0].ID
		default:
			t.Fatalf("unexpected initial run status %q for %s", runs[0].Status, jobID)
		}
	}
	if activeRunID == "" || scheduledRunID == "" {
		t.Fatalf("expected one assigned and one scheduled run, got active=%q scheduled=%q", activeRunID, scheduledRunID)
	}

	pool := worker.NewPool(observed, workerID, 1, time.Second, 20*time.Millisecond, log)
	if err := pool.SetShutdownGracePeriod(2500 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	pool.SetCapabilities(model.Worker{CPUCapacity: 1000, MemoryCapacityMB: 512})
	started := make(chan struct{})
	release := make(chan struct{})
	pool.RegisterHandler(jobName, func(ctx context.Context, _ *model.Job, _ *model.JobRun) (map[string]any, error) {
		close(started)
		select {
		case <-release:
			return map[string]any{"drained": true}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	poolCtx, cancelPool := context.WithCancel(ctx)
	poolDone := make(chan error, 1)
	go func() { poolDone <- pool.Run(poolCtx) }()
	finished := false
	defer func() {
		cancelPool()
		select {
		case <-release:
		default:
			close(release)
		}
		if !finished {
			select {
			case <-poolDone:
			case <-time.After(3 * time.Second):
				t.Errorf("draining Pool.Run() did not stop during cleanup")
			}
		}
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start the assigned handler")
	}
	initial, err := st.GetRun(ctx, activeRunID)
	if err != nil || initial.Status != model.RunStatusRunning || initial.LeaseExpiresAt == nil {
		t.Fatalf("initial running lease = %+v, %v", initial, err)
	}
	initialExpiry := *initial.LeaseExpiresAt
	leaseCallsAtSignal := observed.leaseCalls.Load()
	if leaseCallsAtSignal != 1 {
		t.Fatalf("lease calls before shutdown = %d, want exactly one", leaseCallsAtSignal)
	}
	cancelPool()

	waitUntil := time.Now().Add(2 * time.Second)
	var draining bool
	for time.Now().Before(waitUntil) {
		workers, err := st.ListWorkers(ctx)
		if err != nil {
			t.Fatalf("list workers while draining: %v", err)
		}
		for _, candidate := range workers {
			if candidate.ID == workerID && candidate.Status == model.WorkerStatusDraining {
				draining = true
				break
			}
		}
		if draining {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !draining {
		t.Fatal("worker heartbeat did not persist draining status")
	}

	if assigned, err := promoter.DispatchOnce(ctx); err != nil || assigned != 0 {
		t.Fatalf("DispatchOnce() during drain = %d, %v; want no assignment to draining worker", assigned, err)
	}
	pending, err := st.GetRun(ctx, scheduledRunID)
	if err != nil || pending.Status != model.RunStatusScheduled || pending.AssignedWorkerID != nil {
		t.Fatalf("unassigned run during drain = %+v, %v; want it to remain scheduled", pending, err)
	}

	leaseDeadline := initialExpiry.Add(200 * time.Millisecond)
	if wait := time.Until(leaseDeadline); wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			t.Fatalf("test context ended while waiting beyond initial lease: %v", ctx.Err())
		}
	}
	current, err := st.GetRun(ctx, activeRunID)
	if err != nil || current.Status != model.RunStatusRunning || current.Attempt != 1 || current.LeaseExpiresAt == nil ||
		!current.LeaseExpiresAt.After(initialExpiry) || !current.LeaseExpiresAt.After(time.Now()) {
		t.Fatalf("active run did not retain a renewed lease beyond its initial expiry: run=%+v error=%v initial_expiry=%s",
			current, err, initialExpiry)
	}
	if got := observed.renewalsWhileDraining.Load(); got == 0 {
		t.Fatal("no successful lease renewal was observed after the worker entered draining state")
	}
	if assigned, err := promoter.DispatchOnce(ctx); err != nil || assigned != 0 {
		t.Fatalf("second DispatchOnce() during drain = %d, %v; want no new assignment", assigned, err)
	}
	pending, err = st.GetRun(ctx, scheduledRunID)
	if err != nil || pending.Status != model.RunStatusScheduled || pending.AssignedWorkerID != nil {
		t.Fatalf("scheduled run was assigned during drain: run=%+v error=%v", pending, err)
	}

	close(release)
	status := waitForTerminalRun(t, ctx, st, activeJobID, 10*time.Second)
	if status != model.RunStatusSucceeded {
		t.Fatalf("drained run status = %q, want succeeded", status)
	}
	select {
	case err := <-poolDone:
		finished = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Pool.Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not finish its in-flight handler within the grace period")
	}
	if got := observed.leaseCalls.Load(); got != leaseCallsAtSignal {
		t.Fatalf("lease calls after shutdown signal = %d, want unchanged count %d", got, leaseCallsAtSignal)
	}
	if got := observed.reclaimedLeases.Load(); got != 0 {
		t.Fatalf("expired leases reclaimed during drain = %d, want 0", got)
	}
	completed, err := st.GetRun(ctx, activeRunID)
	if err != nil || completed.Status != model.RunStatusSucceeded || completed.Attempt != 1 || completed.CancelRequestedAt != nil {
		t.Fatalf("final drained run = %+v, %v; want succeeded attempt 1 without cancellation", completed, err)
	}
}

func assertWorkerReservationCeilings(t *testing.T, ctx context.Context, st *store.PostgresStore, expectedWorkers int, cpuCapacity int32, exactCapacity bool) {
	t.Helper()
	workers, err := st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list worker reservations: %v", err)
	}
	if len(workers) != expectedWorkers {
		t.Fatalf("registered workers = %d, want %d", len(workers), expectedWorkers)
	}
	var totalCPUReserved int64
	for _, worker := range workers {
		if worker.CPUCapacity != cpuCapacity || worker.GPUCount != 0 || worker.GPUType != "" || worker.GPUMemoryMB != 0 {
			t.Fatalf("worker %s capacity = CPU %d GPU %d/%s/%d; want CPU %d and no GPU", worker.ID,
				worker.CPUCapacity, worker.GPUCount, worker.GPUType, worker.GPUMemoryMB, cpuCapacity)
		}
		if worker.CurrentCPUReserved > int64(worker.CPUCapacity) || worker.AvailableCPUMillis != int64(worker.CPUCapacity)-worker.CurrentCPUReserved {
			t.Fatalf("worker %s CPU reservation oversubscribed: reserved=%d capacity=%d available=%d", worker.ID,
				worker.CurrentCPUReserved, worker.CPUCapacity, worker.AvailableCPUMillis)
		}
		if worker.CurrentMemoryReserved > int64(worker.MemoryCapacityMB) || worker.CurrentGPUReserved != 0 || worker.CurrentGPUMemoryReserved != 0 {
			t.Fatalf("worker %s resource reservation oversubscribed: %+v", worker.ID, worker)
		}
		totalCPUReserved += worker.CurrentCPUReserved
	}
	if totalCPUReserved > int64(expectedWorkers)*int64(cpuCapacity) {
		t.Fatalf("total CPU reservation %d exceeds fleet capacity %d", totalCPUReserved, expectedWorkers*int(cpuCapacity))
	}
	if exactCapacity && totalCPUReserved != int64(expectedWorkers)*int64(cpuCapacity) {
		t.Fatalf("initial total CPU reservation = %d, want every worker's full capacity %d", totalCPUReserved,
			int64(expectedWorkers)*int64(cpuCapacity))
	}
}
