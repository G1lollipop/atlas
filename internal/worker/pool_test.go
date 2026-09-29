package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestPool(fs *fakeStore) *Pool {
	p := NewPool(fs, "test-worker", 1, 0, 0, testLogger())
	return p
}

func TestExecuteOne_Success(t *testing.T) {
	fs := newFakeStore()
	p := newTestPool(fs)
	p.RegisterHandler("ok-job", func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		return map[string]any{"done": true}, nil
	})

	job := &model.Job{ID: "job-1", Name: "ok-job", MaxAttempts: 3, TimeoutSeconds: 5}
	run := &model.JobRun{ID: "run-1", JobID: "job-1", Attempt: 1}

	p.executeOne(context.Background(), run, job)

	if len(fs.completeRunCalls) != 1 {
		t.Fatalf("expected CompleteRun to be called once, got %d", len(fs.completeRunCalls))
	}
	if fs.completeRunCalls[0].runID != "run-1" {
		t.Errorf("CompleteRun called with runID %q, want %q", fs.completeRunCalls[0].runID, "run-1")
	}
	if fs.completeRunCalls[0].workerID != "test-worker" || fs.markRunningWorkerIDs[0] != "test-worker" {
		t.Errorf("worker ownership = complete %q, start %q, want test-worker", fs.completeRunCalls[0].workerID, fs.markRunningWorkerIDs[0])
	}
	if fs.completeRunCalls[0].attempt != 1 || fs.markRunningAttempts[0] != 1 {
		t.Errorf("attempt fence = complete %d, start %d, want 1", fs.completeRunCalls[0].attempt, fs.markRunningAttempts[0])
	}
	if len(fs.failRunCalls) != 0 {
		t.Errorf("expected FailRun not to be called, got %d calls", len(fs.failRunCalls))
	}
	if len(fs.markDeadCalls) != 0 {
		t.Errorf("expected MarkDead not to be called, got %d calls", len(fs.markDeadCalls))
	}
	if len(fs.markRunningCalls) != 1 {
		t.Errorf("expected MarkRunning to be called once, got %d", len(fs.markRunningCalls))
	}
}

func TestLeaseNextRunRequiresLiveAssignmentToCallingWorker(t *testing.T) {
	fs := newFakeStore()
	now := time.Now().UTC()
	expiresAt := now.Add(time.Minute)
	assignedElsewhere := "other-worker"

	unassigned := &model.JobRun{ID: "queued-run", Status: model.RunStatusQueued}
	wrongOwner := &model.JobRun{
		ID: "other-worker-run", Status: model.RunStatusAssigned,
		AssignedWorkerID: &assignedElsewhere, AssignmentExpiresAt: &expiresAt,
	}
	fs.leaseCandidates = []leaseCandidate{
		{run: unassigned, job: &model.Job{ID: "job-queued"}},
		{run: wrongOwner, job: &model.Job{ID: "job-assigned"}},
	}

	run, _, err := fs.LeaseNextRun(context.Background(), "test-worker", time.Minute)
	if err != nil {
		t.Fatalf("LeaseNextRun() error = %v", err)
	}
	if run != nil {
		t.Fatalf("LeaseNextRun() returned run %q without an assignment to test-worker", run.ID)
	}
	if unassigned.Status != model.RunStatusQueued {
		t.Errorf("unassigned run status = %q, want queued", unassigned.Status)
	}
	if wrongOwner.Status != model.RunStatusAssigned {
		t.Errorf("wrong-owner run status = %q, want assigned", wrongOwner.Status)
	}

	// Assignment is worker-specific: the intended owner can claim the same run.
	run, _, err = fs.LeaseNextRun(context.Background(), assignedElsewhere, time.Minute)
	if err != nil {
		t.Fatalf("LeaseNextRun() for assigned worker error = %v", err)
	}
	if run == nil || run.ID != wrongOwner.ID {
		t.Fatalf("LeaseNextRun() for assigned worker = %#v, want %q", run, wrongOwner.ID)
	}
	if run.Status != model.RunStatusLeased || run.LeasedBy == nil || *run.LeasedBy != assignedElsewhere {
		t.Errorf("claimed run state = %#v, want leased by %q", run, assignedElsewhere)
	}
}

func TestLeaseNextRunIgnoresExpiredAssignment(t *testing.T) {
	fs := newFakeStore()
	workerID := "test-worker"
	expiredAt := time.Now().Add(-time.Second)
	fs.leaseCandidates = []leaseCandidate{{
		run: &model.JobRun{
			ID: "expired-run", Status: model.RunStatusAssigned,
			AssignedWorkerID: &workerID, AssignmentExpiresAt: &expiredAt,
		},
		job: &model.Job{ID: "job-1"},
	}}

	run, _, err := fs.LeaseNextRun(context.Background(), workerID, time.Minute)
	if err != nil {
		t.Fatalf("LeaseNextRun() error = %v", err)
	}
	if run != nil {
		t.Fatalf("LeaseNextRun() returned expired assignment %q", run.ID)
	}
}

func TestExecuteOne_FailureWithAttemptsRemaining(t *testing.T) {
	fs := newFakeStore()
	p := newTestPool(fs)
	handlerErr := errors.New("boom")
	p.RegisterHandler("bad-job", func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		return nil, handlerErr
	})

	// Attempt 1 of 3 max attempts: a retry should be scheduled (requeue=true).
	job := &model.Job{ID: "job-1", Name: "bad-job", MaxAttempts: 3, TimeoutSeconds: 5}
	run := &model.JobRun{ID: "run-1", JobID: "job-1", Attempt: 1}

	p.executeOne(context.Background(), run, job)

	if len(fs.completeRunCalls) != 0 {
		t.Errorf("expected CompleteRun not to be called, got %d calls", len(fs.completeRunCalls))
	}
	if len(fs.failRunCalls) != 1 {
		t.Fatalf("expected FailRun to be called once, got %d", len(fs.failRunCalls))
	}
	fc := fs.failRunCalls[0]
	if fc.runID != "run-1" {
		t.Errorf("FailRun called with runID %q, want %q", fc.runID, "run-1")
	}
	if fc.workerID != "test-worker" {
		t.Errorf("FailRun called with workerID %q, want test-worker", fc.workerID)
	}
	if fc.attempt != 1 {
		t.Errorf("FailRun called with attempt %d, want 1", fc.attempt)
	}
	if !fc.requeue {
		t.Error("expected FailRun to be called with requeue=true when attempts remain")
	}
	if len(fs.markDeadCalls) != 0 {
		t.Errorf("expected MarkDead not to be called when attempts remain, got %d calls", len(fs.markDeadCalls))
	}
}

func TestExecuteOne_FailureAttemptsExhausted(t *testing.T) {
	fs := newFakeStore()
	p := newTestPool(fs)
	handlerErr := errors.New("boom")
	p.RegisterHandler("bad-job", func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		return nil, handlerErr
	})

	// Attempt 3 of 3 max attempts: no retries left, run should be failed (requeue=false)
	// and then marked dead.
	job := &model.Job{ID: "job-1", Name: "bad-job", MaxAttempts: 3, TimeoutSeconds: 5}
	run := &model.JobRun{ID: "run-1", JobID: "job-1", Attempt: 3}

	p.executeOne(context.Background(), run, job)

	if len(fs.completeRunCalls) != 0 {
		t.Errorf("expected CompleteRun not to be called, got %d calls", len(fs.completeRunCalls))
	}
	if len(fs.failRunCalls) != 1 {
		t.Fatalf("expected FailRun to be called once, got %d", len(fs.failRunCalls))
	}
	fc := fs.failRunCalls[0]
	if fc.requeue {
		t.Error("expected FailRun to be called with requeue=false when attempts exhausted")
	}
	if len(fs.markDeadCalls) != 1 {
		t.Fatalf("expected MarkDead to be called once, got %d", len(fs.markDeadCalls))
	}
	if fs.markDeadCalls[0].runID != "run-1" {
		t.Errorf("MarkDead called with runID %q, want %q", fs.markDeadCalls[0].runID, "run-1")
	}
}

func TestExecuteOne_NoHandlerRegistered(t *testing.T) {
	fs := newFakeStore()
	p := newTestPool(fs)

	job := &model.Job{ID: "job-1", Name: "unregistered-job", MaxAttempts: 3, TimeoutSeconds: 5}
	run := &model.JobRun{ID: "run-1", JobID: "job-1", Attempt: 1}

	p.executeOne(context.Background(), run, job)

	if len(fs.failRunCalls) != 1 {
		t.Fatalf("expected FailRun to be called once for missing handler, got %d", len(fs.failRunCalls))
	}
	if fs.failRunCalls[0].requeue {
		t.Error("expected FailRun requeue=false for missing handler")
	}
	if len(fs.markDeadCalls) != 1 {
		t.Fatalf("expected MarkDead to be called once for missing handler, got %d", len(fs.markDeadCalls))
	}
	if len(fs.markRunningCalls) != 0 {
		t.Errorf("expected MarkRunning not to be called for missing handler, got %d calls", len(fs.markRunningCalls))
	}
}

func TestExecuteOneRenewsLeaseForLongRunningHandler(t *testing.T) {
	fs := newFakeStore()
	p := newTestPool(fs)
	p.LeaseDuration = 60 * time.Millisecond
	p.RegisterHandler("slow-job", func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		select {
		case <-time.After(140 * time.Millisecond):
			return map[string]any{"done": true}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	job := &model.Job{ID: "job-1", Name: "slow-job", MaxAttempts: 3, TimeoutSeconds: 2}
	run := &model.JobRun{ID: "run-1", JobID: "job-1", Attempt: 1}
	p.executeOne(context.Background(), run, job)

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.extendLeaseCalls) < 2 {
		t.Fatalf("ExtendLease calls = %d, want repeated renewal during the handler", len(fs.extendLeaseCalls))
	}
	for _, call := range fs.extendLeaseCalls {
		if call.runID != "run-1" || call.workerID != "test-worker" || call.attempt != 1 || call.leaseDuration != p.LeaseDuration {
			t.Errorf("ExtendLease call = %#v, want run-1/test-worker/attempt-1/%s", call, p.LeaseDuration)
		}
	}
}

func TestExecuteOneAbortsWhenMarkRunningFails(t *testing.T) {
	fs := newFakeStore()
	fs.markRunningErr = errors.New("lease ownership was lost")
	p := newTestPool(fs)
	handlerCalls := 0
	p.RegisterHandler("job", func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		handlerCalls++
		return nil, nil
	})

	p.executeOne(context.Background(), &model.JobRun{ID: "run-1"}, &model.Job{ID: "job-1", Name: "job", TimeoutSeconds: 5})

	if handlerCalls != 0 {
		t.Errorf("handler calls = %d, want 0 after MarkRunning failure", handlerCalls)
	}
	if len(fs.completeRunCalls) != 0 || len(fs.failRunCalls) != 0 || len(fs.extendLeaseCalls) != 0 {
		t.Errorf("run lifecycle continued after MarkRunning failure: complete=%d fail=%d extend=%d", len(fs.completeRunCalls), len(fs.failRunCalls), len(fs.extendLeaseCalls))
	}
}

func TestExecuteOneAbortsStaleAttemptBeforeCallingHandler(t *testing.T) {
	fs := newFakeStore()
	fs.fenceAttempts = true
	fs.currentAttempt = 2
	p := newTestPool(fs)
	handlerCalls := 0
	p.RegisterHandler("job", func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		handlerCalls++
		return nil, nil
	})

	p.executeOne(context.Background(), &model.JobRun{ID: "run-1", Attempt: 1}, &model.Job{ID: "job-1", Name: "job", TimeoutSeconds: 5})

	if handlerCalls != 0 {
		t.Errorf("handler calls = %d, want 0 after stale attempt was rejected", handlerCalls)
	}
	if len(fs.markRunningAttempts) != 1 || fs.markRunningAttempts[0] != 1 {
		t.Errorf("MarkRunning attempts = %v, want [1]", fs.markRunningAttempts)
	}
}

func TestExecuteOneDoesNotMarkDeadWhenFailRunFails(t *testing.T) {
	fs := newFakeStore()
	fs.failRunErr = errors.New("lease ownership was lost")
	p := newTestPool(fs)
	p.RegisterHandler("job", func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		return nil, errors.New("handler failed")
	})

	p.executeOne(context.Background(), &model.JobRun{ID: "run-1", Attempt: 1}, &model.Job{ID: "job-1", Name: "job", MaxAttempts: 1, TimeoutSeconds: 5})

	if len(fs.failRunCalls) != 1 {
		t.Fatalf("FailRun calls = %d, want 1", len(fs.failRunCalls))
	}
	if len(fs.markDeadCalls) != 0 {
		t.Errorf("MarkDead calls = %d, want 0 after FailRun failed", len(fs.markDeadCalls))
	}
}

func TestExecuteOneDoesNotReportSuccessWhenCompleteRunFails(t *testing.T) {
	fs := newFakeStore()
	fs.completeRunErr = errors.New("run lease was lost before completion")
	p := newTestPool(fs)
	p.RegisterHandler("job", func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		return map[string]any{"done": true}, nil
	})

	p.executeOne(context.Background(), &model.JobRun{ID: "run-1", Attempt: 1}, &model.Job{ID: "job-1", Name: "job", MaxAttempts: 1, TimeoutSeconds: 5})

	if len(fs.completeRunCalls) != 1 {
		t.Fatalf("CompleteRun calls = %d, want 1 failed transition attempt", len(fs.completeRunCalls))
	}
	if len(fs.failRunCalls) != 0 || len(fs.markDeadCalls) != 0 {
		t.Errorf("lifecycle calls after CompleteRun error = fail:%d dead:%d, want both zero", len(fs.failRunCalls), len(fs.markDeadCalls))
	}
}

func TestRunRegistersCapabilitiesAndRetriesBeforeLeasing(t *testing.T) {
	fs := newFakeStore()
	fs.upsertErrors = []error{errors.New("temporary registration failure")}
	fs.leaseNotify = make(chan struct{}, 1)
	p := NewPool(fs, "gpu-worker-1", 1, time.Second, 10*time.Millisecond, testLogger())
	p.SetCapabilities(model.Worker{
		CPUCapacity:      8000,
		MemoryCapacityMB: 32768,
		GPUCount:         1,
		GPUType:          "H100",
		GPUMemoryMB:      81920,
		Labels:           map[string]string{"region": "us-central"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	select {
	case <-fs.leaseNotify:
		cancel()
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("worker did not begin polling after successful registration")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Pool.Run() error = %v, want context.Canceled", err)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	var successfulRegistrationIndex, firstLeaseIndex = -1, -1
	heartbeatCount := 0
	for i, event := range fs.operationOrder {
		if event == "heartbeat" {
			// The fake store fails only the first heartbeat; the second must
			// complete before any lease call is allowed.
			heartbeatCount++
			if heartbeatCount == 2 {
				successfulRegistrationIndex = i
			}
		} else if event == "lease" && firstLeaseIndex < 0 {
			firstLeaseIndex = i
		}
	}
	if len(fs.heartbeatCalls) < 2 {
		t.Fatalf("registration attempts = %d, want failed attempt plus successful retry", len(fs.heartbeatCalls))
	}
	if len(fs.leaseWorkerIDs) == 0 {
		t.Fatal("worker never attempted to lease its assigned runs")
	}
	for _, workerID := range fs.leaseWorkerIDs {
		if workerID != "gpu-worker-1" {
			t.Errorf("LeaseNextRun workerID = %q, want gpu-worker-1", workerID)
		}
	}
	if successfulRegistrationIndex < 0 || firstLeaseIndex <= successfulRegistrationIndex {
		t.Fatalf("operation order = %v, want successful registration before first lease", fs.operationOrder)
	}
	registered := fs.heartbeatCalls[1]
	if registered.ID != "gpu-worker-1" || registered.Hostname == "" || registered.Status != model.WorkerStatusAlive {
		t.Errorf("registered identity = %#v, want alive gpu-worker-1 with hostname", registered)
	}
	if registered.CPUCapacity != 8000 || registered.MemoryCapacityMB != 32768 || registered.GPUCount != 1 || registered.GPUType != "H100" || registered.GPUMemoryMB != 81920 {
		t.Errorf("registered resources = %#v, want configured CPU/memory/GPU capacities", registered)
	}
	if registered.Labels["region"] != "us-central" {
		t.Errorf("registered labels = %#v, want region label", registered.Labels)
	}
	if registered.StartedAt.IsZero() {
		t.Error("registered worker is missing its process start time")
	}
}
