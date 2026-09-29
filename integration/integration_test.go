// Package integration exercises the real Postgres-backed store, the HTTP API, the
// scheduler's promotion logic, and the worker pool together against a live database —
// unlike the unit tests in internal/scheduler and internal/worker, which use an
// in-memory fake store. It requires a reachable Postgres and is skipped otherwise
// (CI provides one via a service container; locally, `docker compose up -d postgres`
// plus DATABASE_URL=postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable).
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/api"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/scheduler"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/G1lollipop/atlas/internal/worker"
	"github.com/google/uuid"
)

const testSecret = "integration-test-secret"

func TestJobLifecycleEndToEnd(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	var err error

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	router := api.NewRouter(st, log, testSecret, 1000, 1000)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token, err := api.MintToken(testSecret, "integration-test", time.Hour)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}

	job := createJob(t, srv.URL, token, map[string]any{
		"name":            "echo",
		"payload":         map[string]any{"hello": "integration"},
		"max_attempts":    2,
		"timeout_seconds": 5,
	})
	workerID := "integration-worker"
	if err := st.UpsertWorkerHeartbeat(ctx, model.Worker{ID: workerID, Hostname: "integration-host", CPUCapacity: 4000, MemoryCapacityMB: 8192}); err != nil {
		t.Fatalf("register integration worker: %v", err)
	}

	promoter := &scheduler.Promoter{Store: st, Logger: log}
	promoted, err := promoter.PromoteOnce(ctx)
	if err != nil {
		t.Fatalf("PromoteOnce: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("expected exactly 1 run promoted, got %d", promoted)
	}
	if _, err := promoter.DispatchOnce(ctx); err != nil {
		t.Fatalf("DispatchOnce: %v", err)
	}
	runs, err := st.ListJobRuns(ctx, job.ID, 1)
	if err != nil || len(runs) != 1 || runs[0].Status != model.RunStatusAssigned || runs[0].AssignedWorkerID == nil || *runs[0].AssignedWorkerID != workerID {
		t.Fatalf("scheduler should assign the run to %s before leasing: runs=%+v error=%v", workerID, runs, err)
	}

	pool := worker.NewPool(st, "integration-worker", 1, 30*time.Second, 100*time.Millisecond, log)
	pool.RegisterHandler("echo", worker.EchoHandler)

	workerCtx, workerCancel := context.WithTimeout(ctx, 3*time.Second)
	done := make(chan error, 1)
	go func() { done <- pool.Run(workerCtx) }()

	status := waitForTerminalRun(t, ctx, st, job.ID, 3*time.Second)
	workerCancel()
	<-done

	if status != model.RunStatusSucceeded {
		t.Fatalf("expected run to succeed, got status %q", status)
	}
}

func TestDeadLetterOnUnregisteredHandler(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	var err error

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	router := api.NewRouter(st, log, testSecret, 1000, 1000)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token, err := api.MintToken(testSecret, "integration-test", time.Hour)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}

	job := createJob(t, srv.URL, token, map[string]any{
		"name":            "no_such_handler",
		"max_attempts":    1,
		"timeout_seconds": 5,
	})
	workerID := "integration-worker-2"
	if err := st.UpsertWorkerHeartbeat(ctx, model.Worker{ID: workerID, Hostname: "integration-host-2", CPUCapacity: 4000, MemoryCapacityMB: 8192}); err != nil {
		t.Fatalf("register integration worker: %v", err)
	}

	promoter := &scheduler.Promoter{Store: st, Logger: log}
	if _, err := promoter.PromoteOnce(ctx); err != nil {
		t.Fatalf("PromoteOnce: %v", err)
	}
	if _, err := promoter.DispatchOnce(ctx); err != nil {
		t.Fatalf("DispatchOnce: %v", err)
	}

	// No handler registered for "no_such_handler" — the pool has zero handlers.
	pool := worker.NewPool(st, "integration-worker-2", 1, 30*time.Second, 100*time.Millisecond, log)

	workerCtx, workerCancel := context.WithTimeout(ctx, 3*time.Second)
	done := make(chan error, 1)
	go func() { done <- pool.Run(workerCtx) }()

	status := waitForTerminalRun(t, ctx, st, job.ID, 3*time.Second)
	workerCancel()
	<-done

	if status != model.RunStatusDead {
		t.Fatalf("expected run to be dead-lettered, got status %q", status)
	}
}

func TestResourceAwareAssignmentsAndReservationRelease(t *testing.T) {
	st, ctx := openResourceTestStore(t)

	cpuID := "assign-cpu-" + uuid.NewString()
	gpu8ID := "assign-gpu8-" + uuid.NewString()
	gpu16ID := "assign-gpu16-" + uuid.NewString()
	wrongGPUTypeID := "assign-gpu-l40-" + uuid.NewString()
	registerTestWorker(t, ctx, st, model.Worker{ID: cpuID, Hostname: "cpu-host", CPUCapacity: 2000, MemoryCapacityMB: 4096})
	registerTestWorker(t, ctx, st, model.Worker{ID: gpu8ID, Hostname: "gpu8-host", CPUCapacity: 4000, MemoryCapacityMB: 8192, GPUCount: 1, GPUType: "NVIDIA-A10", GPUMemoryMB: 8192})
	registerTestWorker(t, ctx, st, model.Worker{ID: gpu16ID, Hostname: "gpu16-host", CPUCapacity: 4000, MemoryCapacityMB: 16384, GPUCount: 1, GPUType: "nvidia-a100", GPUMemoryMB: 16384})
	registerTestWorker(t, ctx, st, model.Worker{ID: wrongGPUTypeID, Hostname: "l40-host", CPUCapacity: 4000, MemoryCapacityMB: 32768, GPUCount: 1, GPUType: "nvidia-l40", GPUMemoryMB: 49152})

	cpuJob := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "embedding-" + uuid.NewString(), WorkloadType: "embedding", Priority: 10,
		RequiredCPUMillis: 1000, RequiredMemoryMB: 512, MaxAttempts: 2, TimeoutSeconds: 30,
	})
	smallGPUJob := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "small-inference-" + uuid.NewString(), WorkloadType: "inference", Priority: 20,
		RequiredCPUMillis: 100, RequiredMemoryMB: 128, RequiredGPUCount: 1,
		RequiredGPUMemoryMB: 4096, MaxAttempts: 2, TimeoutSeconds: 30,
	})
	largeGPUJob := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "large-inference-" + uuid.NewString(), WorkloadType: "inference", Priority: 30,
		RequiredCPUMillis: 500, RequiredMemoryMB: 1024, RequiredGPUCount: 1,
		RequiredGPUMemoryMB: 16384, RequiredAccelerator: "NVIDIA-A100", MaxAttempts: 2, TimeoutSeconds: 30,
	})
	cpuRun := createResourceTestRun(t, ctx, st, cpuJob.ID, 10)
	smallGPURun := createResourceTestRun(t, ctx, st, smallGPUJob.ID, 20)
	largeGPURun := createResourceTestRun(t, ctx, st, largeGPUJob.ID, 30)

	scheduled, err := st.ScheduleDueRuns(ctx)
	if err != nil || scheduled != 3 {
		t.Fatalf("schedule three due runs: count=%d error=%v", scheduled, err)
	}
	if ok, err := st.AssignRun(ctx, largeGPURun.ID, wrongGPUTypeID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL); err != nil || ok {
		t.Fatalf("incompatible accelerator must be rejected atomically: assigned=%v error=%v", ok, err)
	}
	promoter := &scheduler.Promoter{Store: st, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	assigned, err := promoter.DispatchOnce(ctx)
	if err != nil || assigned != 3 {
		t.Fatalf("dispatch three runs: count=%d error=%v", assigned, err)
	}

	assertRunAssignedTo(t, ctx, st, cpuRun.ID, cpuID)
	// The best-fit policy keeps a 4 GB task on the 8 GB device, preserving the
	// larger A100 for workloads that need its remaining VRAM.
	assertRunAssignedTo(t, ctx, st, smallGPURun.ID, gpu8ID)
	assertRunAssignedTo(t, ctx, st, largeGPURun.ID, gpu16ID)

	workers, err := st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers after assignment: %v", err)
	}
	assertWorkerResources(t, workers, gpu8ID, 3900, 8064, 0, 4096)
	assertWorkerResources(t, workers, gpu16ID, 3500, 15360, 0, 0)
	if w := findWorker(t, workers, gpu16ID); w.CurrentGPUReserved != 1 || w.CurrentGPUMemoryReserved != 16384 {
		t.Fatalf("assigned run must reserve GPU before lease: worker=%+v", w)
	}

	// A worker cannot pull a run assigned to another worker, even when it has more VRAM.
	run, job, err := st.LeaseNextRun(ctx, wrongGPUTypeID, time.Minute)
	if err != nil || run != nil || job != nil {
		t.Fatalf("unassigned worker should not lease anything: run=%+v job=%+v error=%v", run, job, err)
	}
	run, job, err = st.LeaseNextRun(ctx, gpu8ID, time.Minute)
	if err != nil || run == nil || job == nil || run.ID != smallGPURun.ID {
		t.Fatalf("8 GB worker should lease its assigned 4 GB task: run=%+v job=%+v error=%v", run, job, err)
	}
	run, job, err = st.LeaseNextRun(ctx, gpu16ID, time.Minute)
	if err != nil || run == nil || job == nil || run.ID != largeGPURun.ID {
		t.Fatalf("A100 worker should lease its assigned task: run=%+v job=%+v error=%v", run, job, err)
	}
	if err := st.MarkRunning(ctx, run.ID, gpu16ID, run.Attempt); err != nil {
		t.Fatalf("mark GPU task running: %v", err)
	}
	if err := st.CompleteRun(ctx, run.ID, gpu16ID, run.Attempt, map[string]any{"ok": true}); err != nil {
		t.Fatalf("complete GPU task: %v", err)
	}
	completed, err := st.GetRun(ctx, largeGPURun.ID)
	if err != nil || completed.AssignedWorkerID == nil || *completed.AssignedWorkerID != gpu16ID {
		t.Fatalf("terminal run should retain assignment history: run=%+v error=%v", completed, err)
	}

	// A retry clears placement, returns to queued, and must pass the scheduler again.
	if err := st.FailRun(ctx, smallGPURun.ID, gpu8ID, runAttemptFor(t, ctx, st, smallGPURun.ID), "retry check", true, 0); err != nil {
		t.Fatalf("requeue GPU task: %v", err)
	}
	retried, err := st.GetRun(ctx, smallGPURun.ID)
	if err != nil || retried.Status != model.RunStatusQueued || retried.AssignedWorkerID != nil {
		t.Fatalf("retry should be queued with assignment cleared: run=%+v error=%v", retried, err)
	}
	if _, err := st.ScheduleDueRuns(ctx); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}
	ok, err := st.AssignRun(ctx, smallGPURun.ID, gpu8ID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL)
	if err != nil || !ok {
		t.Fatalf("reassign retried task: assigned=%v error=%v", ok, err)
	}
	retryLease, _, err := st.LeaseNextRun(ctx, gpu8ID, time.Minute)
	if err != nil || retryLease == nil || retryLease.ID != smallGPURun.ID {
		t.Fatalf("lease retried task: run=%+v error=%v", retryLease, err)
	}
	if err := st.MarkRunning(ctx, retryLease.ID, gpu8ID, retryLease.Attempt); err != nil {
		t.Fatalf("mark retry running: %v", err)
	}
	if err := st.CompleteRun(ctx, retryLease.ID, gpu8ID, retryLease.Attempt, map[string]any{"ok": true}); err != nil {
		t.Fatalf("complete retry: %v", err)
	}
}

func TestConcurrentMemoryReservationsAcrossPostgresPools(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	peerStore := openPeerResourceTestStore(t, ctx, st)
	workerID := "memory-race-" + uuid.NewString()
	registerTestWorker(t, ctx, st, model.Worker{
		ID: workerID, Hostname: "memory-race-host", CPUCapacity: 4000, MemoryCapacityMB: 16 * 1024,
	})

	firstJob := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "memory-heavy-a-" + uuid.NewString(), RequiredCPUMillis: 1000,
		RequiredMemoryMB: 12 * 1024, MaxAttempts: 1, TimeoutSeconds: 30,
	})
	secondJob := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "memory-heavy-b-" + uuid.NewString(), RequiredCPUMillis: 1000,
		RequiredMemoryMB: 12 * 1024, MaxAttempts: 1, TimeoutSeconds: 30,
	})
	firstRun := createResourceTestRun(t, ctx, st, firstJob.ID, 30000)
	secondRun := createResourceTestRun(t, ctx, st, secondJob.ID, 29999)
	if scheduled, err := st.ScheduleDueRuns(ctx); err != nil || scheduled != 2 {
		t.Fatalf("schedule memory reservations: count=%d error=%v", scheduled, err)
	}

	type assignResult struct {
		runID string
		ok    bool
		err   error
	}
	start := make(chan struct{})
	ready := make(chan struct{}, 2)
	results := make(chan assignResult, 2)
	for i, runID := range []string{firstRun.ID, secondRun.ID} {
		assignmentStore := st
		if i == 1 {
			assignmentStore = peerStore
		}
		go func(runID string, assignmentStore *store.PostgresStore) {
			ready <- struct{}{}
			<-start
			ok, err := assignmentStore.AssignRun(ctx, runID, workerID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL)
			results <- assignResult{runID: runID, ok: ok, err: err}
		}(runID, assignmentStore)
	}
	<-ready
	<-ready
	close(start)

	assigned := make([]string, 0, 1)
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent assignment for %s: %v", result.runID, result.err)
		}
		if result.ok {
			assigned = append(assigned, result.runID)
		}
	}
	if len(assigned) != 1 {
		t.Fatalf("a 16 GiB worker must reserve capacity for exactly one concurrent 12 GiB job, assigned=%v", assigned)
	}
	assignedID := assigned[0]
	unassignedID := firstRun.ID
	if assignedID == unassignedID {
		unassignedID = secondRun.ID
	}
	workers, err := st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list worker resources after concurrent assignment: %v", err)
	}
	assertWorkerResources(t, workers, workerID, 3000, 4*1024, 0, 0)
	assertRunAssignedTo(t, ctx, st, assignedID, workerID)
	if run, err := st.GetRun(ctx, unassignedID); err != nil || run.Status != model.RunStatusScheduled {
		t.Fatalf("losing reservation must remain schedulable: run=%+v error=%v", run, err)
	}
	if ok, err := peerStore.AssignRun(ctx, unassignedID, workerID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL); err != nil || ok {
		t.Fatalf("retry losing reservation while capacity is occupied: assigned=%v error=%v", ok, err)
	}
	workers, err = st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list worker resources after rejected reservation retry: %v", err)
	}
	assertWorkerResources(t, workers, workerID, 3000, 4*1024, 0, 0)

	// Completing the assigned run releases its reservation, allowing the other
	// 12 GiB job to be assigned through the independent pool.
	leased, _, err := st.LeaseNextRun(ctx, workerID, time.Minute)
	if err != nil || leased == nil || leased.ID != assignedID {
		t.Fatalf("lease the assigned memory-heavy run: run=%+v error=%v", leased, err)
	}
	if err := st.MarkRunning(ctx, leased.ID, workerID, leased.Attempt); err != nil {
		t.Fatalf("mark memory-heavy run running: %v", err)
	}
	if err := st.CompleteRun(ctx, leased.ID, workerID, leased.Attempt, map[string]any{"ok": true}); err != nil {
		t.Fatalf("complete memory-heavy run: %v", err)
	}
	workers, err = st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list worker resources after completion: %v", err)
	}
	assertWorkerResources(t, workers, workerID, 4000, 16*1024, 0, 0)
	if ok, err := peerStore.AssignRun(ctx, unassignedID, workerID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL); err != nil || !ok {
		t.Fatalf("completion must release memory reservation: assigned=%v error=%v", ok, err)
	}
}

func TestAssignmentExpiryReclaimAndConcurrentOwnership(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	workerID := "assignment-concurrent-" + uuid.NewString()
	peerID := "assignment-peer-" + uuid.NewString()
	registerTestWorker(t, ctx, st, model.Worker{ID: workerID, Hostname: "concurrent-host", CPUCapacity: 2000, MemoryCapacityMB: 2048})
	registerTestWorker(t, ctx, st, model.Worker{ID: peerID, Hostname: "peer-host", CPUCapacity: 2000, MemoryCapacityMB: 2048})

	first := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "cpu-heavy-a-" + uuid.NewString(), RequiredCPUMillis: 1500, RequiredMemoryMB: 1024,
		MaxAttempts: 3, TimeoutSeconds: 30,
	})
	second := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "cpu-heavy-b-" + uuid.NewString(), RequiredCPUMillis: 1500, RequiredMemoryMB: 1024,
		MaxAttempts: 3, TimeoutSeconds: 30,
	})
	third := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "cpu-heavy-c-" + uuid.NewString(), RequiredCPUMillis: 1500, RequiredMemoryMB: 1024,
		MaxAttempts: 3, TimeoutSeconds: 30,
	})
	firstRun := createResourceTestRun(t, ctx, st, first.ID, 30000)
	secondRun := createResourceTestRun(t, ctx, st, second.ID, 29999)
	thirdRun := createResourceTestRun(t, ctx, st, third.ID, 29998)
	if _, err := st.ScheduleDueRuns(ctx); err != nil {
		t.Fatalf("schedule concurrent runs: %v", err)
	}

	type assignResult struct {
		id  string
		ok  bool
		err error
	}
	assignments := make(chan assignResult, 2)
	for _, id := range []string{firstRun.ID, secondRun.ID} {
		go func(runID string) {
			ok, err := st.AssignRun(ctx, runID, workerID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL)
			assignments <- assignResult{id: runID, ok: ok, err: err}
		}(id)
	}
	assignedRuns := make([]string, 0, 1)
	for i := 0; i < 2; i++ {
		result := <-assignments
		if result.err != nil {
			t.Fatalf("concurrent assignment: %v", result.err)
		}
		if result.ok {
			assignedRuns = append(assignedRuns, result.id)
		}
	}
	if len(assignedRuns) != 1 {
		t.Fatalf("worker capacity should allow exactly one concurrent assignment, got %v", assignedRuns)
	}
	assignedID := assignedRuns[0]
	unassignedID := firstRun.ID
	if assignedID == unassignedID {
		unassignedID = secondRun.ID
	}
	workers, err := st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers after assignment: %v", err)
	}
	assertWorkerResources(t, workers, workerID, 500, 1024, 0, 0)
	if ok, err := st.AssignRun(ctx, thirdRun.ID, workerID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL); err != nil || ok {
		t.Fatalf("assigned reservations must prevent overcommit: assigned=%v error=%v", ok, err)
	}
	if run, job, err := st.LeaseNextRun(ctx, peerID, time.Minute); err != nil || run != nil || job != nil {
		t.Fatalf("peer worker must not pull another worker's assignment: run=%+v job=%+v error=%v", run, job, err)
	}

	type leaseResult struct {
		run *model.JobRun
		err error
	}
	leases := make(chan leaseResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			run, _, err := st.LeaseNextRun(ctx, workerID, time.Minute)
			leases <- leaseResult{run: run, err: err}
		}()
	}
	var leased *model.JobRun
	leaseCount := 0
	for i := 0; i < 2; i++ {
		result := <-leases
		if result.err != nil {
			t.Fatalf("concurrent lease: %v", result.err)
		}
		if result.run != nil {
			leaseCount++
			leased = result.run
		}
	}
	if leaseCount != 1 || leased == nil || leased.ID != assignedID {
		t.Fatalf("one assigned run should be leased exactly once: count=%d run=%+v", leaseCount, leased)
	}
	if err := st.MarkRunning(ctx, leased.ID, workerID, leased.Attempt); err != nil {
		t.Fatalf("mark leased run running: %v", err)
	}

	if _, err := st.Pool().Exec(ctx, `UPDATE job_runs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, leased.ID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	reclaimed, err := st.ReclaimExpiredLeases(ctx)
	if err != nil || reclaimed != 1 {
		t.Fatalf("reclaim expired lease: count=%d error=%v", reclaimed, err)
	}
	reclaimedRun, err := st.GetRun(ctx, leased.ID)
	if err != nil || reclaimedRun.Status != model.RunStatusQueued || reclaimedRun.AssignedWorkerID != nil {
		t.Fatalf("expired lease should return to queued and clear assignment: run=%+v error=%v", reclaimedRun, err)
	}
	if err := st.FailRun(ctx, leased.ID, workerID, leased.Attempt, "stale attempt", false, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expired attempt must not mutate reclaimed run, got error %v", err)
	}

	// Reassign after reclaim and prove the old attempt stays fenced.
	if _, err := st.ScheduleDueRuns(ctx); err != nil {
		t.Fatalf("schedule reclaimed run: %v", err)
	}
	ok, err := st.AssignRun(ctx, leased.ID, workerID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL)
	if err != nil || !ok {
		t.Fatalf("reassign reclaimed run: assigned=%v error=%v", ok, err)
	}
	releasedAgain, _, err := st.LeaseNextRun(ctx, workerID, time.Minute)
	if err != nil || releasedAgain == nil || releasedAgain.ID != leased.ID || releasedAgain.Attempt != leased.Attempt+1 {
		t.Fatalf("re-lease reclaimed run with incremented attempt: run=%+v error=%v", releasedAgain, err)
	}
	if err := st.FailRun(ctx, leased.ID, workerID, leased.Attempt, "stale attempt", false, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stale attempt must not mutate new lease, got error %v", err)
	}
	if err := st.MarkRunning(ctx, releasedAgain.ID, workerID, releasedAgain.Attempt); err != nil {
		t.Fatalf("mark re-leased run running: %v", err)
	}
	if err := st.CompleteRun(ctx, releasedAgain.ID, workerID, releasedAgain.Attempt, map[string]any{"ok": true}); err != nil {
		t.Fatalf("complete re-leased run: %v", err)
	}

	// An assignment expiration is separate from worker lease recovery. Once expired,
	// it returns to queued and another compatible worker can receive it.
	// Put the remaining run on the worker, then expire and reassign it.
	ok, err = st.AssignRun(ctx, unassignedID, workerID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL)
	if err != nil || !ok {
		t.Fatalf("assign second run: assigned=%v error=%v", ok, err)
	}
	if _, err := st.Pool().Exec(ctx, `UPDATE job_runs SET assignment_expires_at = now() - interval '1 second' WHERE id = $1`, unassignedID); err != nil {
		t.Fatalf("expire second assignment: %v", err)
	}
	expired, err := st.RequeueExpiredAssignments(ctx)
	if err != nil || expired != 1 {
		t.Fatalf("requeue expired assignment: count=%d error=%v", expired, err)
	}
	expiredRun, err := st.GetRun(ctx, unassignedID)
	if err != nil || expiredRun.Status != model.RunStatusQueued || expiredRun.AssignedWorkerID != nil {
		t.Fatalf("expired assignment should return to queued: run=%+v error=%v", expiredRun, err)
	}
	if _, err := st.ScheduleDueRuns(ctx); err != nil {
		t.Fatalf("schedule reassignment: %v", err)
	}
	ok, err = st.AssignRun(ctx, unassignedID, peerID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL)
	if err != nil || !ok {
		t.Fatalf("reassign expired run to peer: assigned=%v error=%v", ok, err)
	}
	if run, _, err := st.LeaseNextRun(ctx, workerID, time.Minute); err != nil {
		t.Fatalf("old worker poll after reassignment: %v", err)
	} else if run != nil && run.ID == unassignedID {
		t.Fatalf("old worker must not lease reassigned run %s", unassignedID)
	}
	peerLease, _, err := st.LeaseNextRun(ctx, peerID, time.Minute)
	if err != nil || peerLease == nil || peerLease.ID != unassignedID {
		t.Fatalf("new worker should lease reassigned run: run=%+v error=%v", peerLease, err)
	}
}

func TestCapacityShrinkRequeuesInfeasibleAssignment(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	workerID := "capacity-shrink-" + uuid.NewString()
	registerTestWorker(t, ctx, st, model.Worker{ID: workerID, Hostname: "capacity-host", CPUCapacity: 3000, MemoryCapacityMB: 2048})

	largeJob := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "large-cpu-" + uuid.NewString(), RequiredCPUMillis: 2500, RequiredMemoryMB: 1024,
		MaxAttempts: 2, TimeoutSeconds: 30,
	})
	smallJob := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "small-cpu-" + uuid.NewString(), RequiredCPUMillis: 500, RequiredMemoryMB: 512,
		MaxAttempts: 2, TimeoutSeconds: 30,
	})
	largeRun := createResourceTestRun(t, ctx, st, largeJob.ID, 30000)
	smallRun := createResourceTestRun(t, ctx, st, smallJob.ID, 29999)
	if _, err := st.ScheduleDueRuns(ctx); err != nil {
		t.Fatalf("schedule runs: %v", err)
	}
	for _, runID := range []string{largeRun.ID, smallRun.ID} {
		if ok, err := st.AssignRun(ctx, runID, workerID, scheduler.AssignmentTTL, scheduler.HeartbeatTTL); err != nil || !ok {
			t.Fatalf("assign %s before capacity change: assigned=%v error=%v", runID, ok, err)
		}
	}

	// A fresh heartbeat can report reduced capacity. Leasing requeues the infeasible
	// large assignment so the next poll can claim the smaller eligible run.
	registerTestWorker(t, ctx, st, model.Worker{ID: workerID, Hostname: "capacity-host", CPUCapacity: 1000, MemoryCapacityMB: 2048})
	firstPoll, firstJob, err := st.LeaseNextRun(ctx, workerID, time.Minute)
	if err != nil || firstPoll != nil || firstJob != nil {
		t.Fatalf("first poll should requeue the infeasible assignment: run=%+v job=%+v error=%v", firstPoll, firstJob, err)
	}
	largeAfter, err := st.GetRun(ctx, largeRun.ID)
	if err != nil || largeAfter.Status != model.RunStatusQueued || largeAfter.AssignedWorkerID != nil {
		t.Fatalf("infeasible assignment should be returned to queue: run=%+v error=%v", largeAfter, err)
	}
	leased, job, err := st.LeaseNextRun(ctx, workerID, time.Minute)
	if err != nil || leased == nil || job == nil || leased.ID != smallRun.ID {
		t.Fatalf("next poll should claim the smaller eligible assignment: run=%+v job=%+v error=%v", leased, job, err)
	}
}

func TestScheduledRunListingIncludesAgedLowPriorityWork(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	oldJob := createResourceTestJob(t, ctx, st, model.NewJobInput{Name: "old-low-priority-" + uuid.NewString(), MaxAttempts: 1, TimeoutSeconds: 30})
	newJob := createResourceTestJob(t, ctx, st, model.NewJobInput{Name: "new-high-priority-" + uuid.NewString(), MaxAttempts: 1, TimeoutSeconds: 30})
	oldRun, err := st.CreateRun(ctx, oldJob.ID, 0, time.Now().Add(-3*time.Hour))
	if err != nil {
		t.Fatalf("create aged run: %v", err)
	}
	if _, err := st.CreateRun(ctx, newJob.ID, 1, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("create newer high-priority run: %v", err)
	}
	if _, err := st.ScheduleDueRuns(ctx); err != nil {
		t.Fatalf("schedule due runs: %v", err)
	}
	candidates, err := st.ListScheduledRuns(ctx, 1, 0)
	if err != nil || len(candidates) != 1 || candidates[0].Run.ID != oldRun.ID {
		t.Fatalf("limited listing should expose aged low-priority run first: candidates=%+v error=%v", candidates, err)
	}
}

func openResourceTestStore(t *testing.T) (*store.PostgresStore, context.Context) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set; skipping resource store integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	adminStore, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	schema := "resource_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminStore.Pool().Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		adminStore.Close()
		t.Fatalf("create isolated test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := adminStore.Pool().Exec(cleanupCtx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`); err != nil {
			t.Errorf("drop isolated test schema %s: %v", schema, err)
		}
		adminStore.Close()
	})

	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	query := parsedURL.Query()
	query.Set("search_path", schema)
	parsedURL.RawQuery = query.Encode()
	st, err := store.New(ctx, parsedURL.String())
	if err != nil {
		t.Fatalf("connect to isolated test schema: %v", err)
	}
	t.Cleanup(st.Close)
	if err := store.RunMigrations(ctx, st.Pool(), "../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return st, ctx
}

func openPeerResourceTestStore(t *testing.T, ctx context.Context, st *store.PostgresStore) *store.PostgresStore {
	t.Helper()
	var schema string
	if err := st.Pool().QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("read isolated test schema: %v", err)
	}
	dbURL, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("parse database URL for peer pool: %v", err)
	}
	query := dbURL.Query()
	query.Set("search_path", schema)
	dbURL.RawQuery = query.Encode()
	peerStore, err := store.New(ctx, dbURL.String())
	if err != nil {
		t.Fatalf("connect independent store pool: %v", err)
	}
	t.Cleanup(peerStore.Close)
	return peerStore
}

func registerTestWorker(t *testing.T, ctx context.Context, st *store.PostgresStore, worker model.Worker) {
	t.Helper()
	if err := st.UpsertWorkerHeartbeat(ctx, worker); err != nil {
		t.Fatalf("register worker %s: %v", worker.ID, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := st.Pool().Exec(cleanupCtx, `DELETE FROM workers WHERE id = $1`, worker.ID); err != nil {
			t.Errorf("delete resource test worker %s: %v", worker.ID, err)
		}
	})
}

func createResourceTestJob(t *testing.T, ctx context.Context, st *store.PostgresStore, input model.NewJobInput) *model.Job {
	t.Helper()
	if input.Payload == nil {
		input.Payload = map[string]any{}
	}
	job, err := st.CreateJob(ctx, input)
	if err != nil {
		t.Fatalf("create resource job: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := st.Pool().Exec(cleanupCtx, `DELETE FROM jobs WHERE id = $1`, job.ID); err != nil {
			t.Errorf("delete resource test job %s: %v", job.ID, err)
		}
	})
	return job
}

func createResourceTestRun(t *testing.T, ctx context.Context, st *store.PostgresStore, jobID string, priority int16) *model.JobRun {
	t.Helper()
	run, err := st.CreateRun(ctx, jobID, priority, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("create resource run: %v", err)
	}
	if run.Status != model.RunStatusQueued {
		t.Fatalf("new run should start queued, got %q", run.Status)
	}
	return run
}

func assertRunAssignedTo(t *testing.T, ctx context.Context, st *store.PostgresStore, runID, workerID string) {
	t.Helper()
	run, err := st.GetRun(ctx, runID)
	if err != nil || run.Status != model.RunStatusAssigned || run.AssignedWorkerID == nil || *run.AssignedWorkerID != workerID {
		t.Fatalf("run %s should be assigned to %s: run=%+v error=%v", runID, workerID, run, err)
	}
}

func runAttemptFor(t *testing.T, ctx context.Context, st *store.PostgresStore, runID string) int16 {
	t.Helper()
	run, err := st.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run %s attempt: %v", runID, err)
	}
	return run.Attempt
}

func assertWorkerResources(t *testing.T, workers any, workerID string, cpu, memory, gpu, gpuMemory int64) {
	t.Helper()
	worker := findWorker(t, workers, workerID)
	if worker.AvailableCPUMillis != cpu || worker.AvailableMemoryMB != memory || worker.AvailableGPUCount != gpu || worker.AvailableGPUMemoryMB != gpuMemory {
		t.Fatalf("worker %s resource snapshot mismatch: available cpu=%d memory=%d gpu=%d gpu_memory=%d; want %d/%d/%d/%d; reserved cpu=%d memory=%d gpu=%d gpu_memory=%d",
			workerID, worker.AvailableCPUMillis, worker.AvailableMemoryMB, worker.AvailableGPUCount,
			worker.AvailableGPUMemoryMB, cpu, memory, gpu, gpuMemory,
			worker.CurrentCPUReserved, worker.CurrentMemoryReserved, worker.CurrentGPUReserved,
			worker.CurrentGPUMemoryReserved)
	}
}

func findWorker(t *testing.T, workers any, workerID string) *model.Worker {
	t.Helper()
	var worker *model.Worker
	switch values := workers.(type) {
	case []*model.Worker:
		for _, candidate := range values {
			if candidate.ID == workerID {
				worker = candidate
				break
			}
		}
	case []model.Worker:
		for i := range values {
			if values[i].ID == workerID {
				worker = &values[i]
				break
			}
		}
	}
	if worker == nil {
		t.Fatalf("worker %s missing from resource snapshot", workerID)
	}
	return worker
}

func createJob(t *testing.T, baseURL, token string, body map[string]any) *model.Job {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/jobs: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v1/jobs: expected 201, got %d", resp.StatusCode)
	}

	var job model.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatalf("decode job response: %v", err)
	}
	return &job
}

func waitForTerminalRun(t *testing.T, ctx context.Context, st store.Store, jobID string, timeout time.Duration) model.RunStatus {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		runs, err := st.ListJobRuns(ctx, jobID, 1)
		if err == nil && len(runs) == 1 {
			switch runs[0].Status {
			case model.RunStatusSucceeded, model.RunStatusFailed, model.RunStatusDead:
				return runs[0].Status
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run for job %s did not reach a terminal state within %s", jobID, timeout)
	return ""
}
