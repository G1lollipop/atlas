package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

func TestSchedulingScoreIncludesPriorityAndUnboundedAge(t *testing.T) {
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	job := &model.Job{}
	run := &model.JobRun{Priority: 2, ScheduledAt: now.Add(-90 * time.Minute)}
	got := SchedulingScore(run, job, nil, now)
	want := 2*PriorityWeight.Seconds() + 90*60
	if got != want {
		t.Fatalf("score = %v, want priority + age = %v", got, want)
	}

	oldLowPriority := &model.JobRun{ID: "old-low", Priority: 0, ScheduledAt: now.Add(-2 * time.Hour)}
	newHighPriority := &model.JobRun{ID: "new-high", Priority: 1, ScheduledAt: now.Add(-20 * time.Minute)}
	if SchedulingScore(oldLowPriority, job, nil, now) <= SchedulingScore(newHighPriority, job, nil, now) {
		t.Fatal("expected a long-waiting low-priority run to outrank newly arriving high-priority work")
	}
}

func TestSchedulingScorePrefersLessGPUFragmentation(t *testing.T) {
	now := time.Now()
	run := &model.JobRun{ID: "gpu-run", Priority: 1, ScheduledAt: now.Add(-time.Minute)}
	job := &model.Job{RequiredGPUCount: 1, RequiredGPUMemoryMB: 4096}
	small := testWorker("small", now, 2, 16384, 1, 8192)
	large := testWorker("large", now, 2, 16384, 1, 49152)

	smallScore := SchedulingScore(run, job, small, now)
	largeScore := SchedulingScore(run, job, large, now)
	if smallScore <= largeScore {
		t.Fatalf("8GB worker score %v should beat 48GB worker score %v for a 4GB task", smallScore, largeScore)
	}

	candidate := &model.RunCandidate{Run: run, Job: job}
	chosen, ok := bestAssignment([]*model.RunCandidate{candidate}, []*model.Worker{large, small}, now, nil)
	if !ok || chosen.workerID != small.ID {
		t.Fatalf("best assignment = %+v, %v; want worker %q", chosen, ok, small.ID)
	}
}

func TestBestAssignmentUsesFIFOAndStableIDsOnTies(t *testing.T) {
	now := time.Now()
	worker := testWorker("worker", now, 1000, 4096, 0, 0)
	scheduledAt := now.Add(-time.Minute)
	candidates := []*model.RunCandidate{
		{Run: &model.JobRun{ID: "z-run", Priority: 3, ScheduledAt: scheduledAt}, Job: &model.Job{RequiredCPUMillis: 100}},
		{Run: &model.JobRun{ID: "a-run", Priority: 3, ScheduledAt: scheduledAt}, Job: &model.Job{RequiredCPUMillis: 100}},
	}
	chosen, ok := bestAssignment(candidates, []*model.Worker{worker}, now, nil)
	if !ok || chosen.runID != "a-run" {
		t.Fatalf("tie assignment = %+v, %v; want stable run ID a-run", chosen, ok)
	}

	older := candidates[0]
	older.Run.ScheduledAt = scheduledAt.Add(-time.Minute)
	chosen, ok = bestAssignment(candidates, []*model.Worker{worker}, now, nil)
	if !ok || chosen.runID != "z-run" {
		t.Fatalf("FIFO assignment = %+v, %v; want older z-run", chosen, ok)
	}
}

func TestWorkerCanRunChecksHeartbeatAndPerGPUVRAM(t *testing.T) {
	now := time.Now()
	worker := testWorker("gpu", now, 8000, 32768, 2, 8192)
	job := &model.Job{
		RequiredCPUMillis:   1000,
		RequiredMemoryMB:    1024,
		RequiredGPUCount:    1,
		RequiredGPUMemoryMB: 12288,
		RequiredAccelerator: "nvidia-h100",
	}
	if WorkerCanRun(job, worker, now, HeartbeatTTL) {
		t.Fatal("worker with aggregate free VRAM but only 8GB per GPU must not fit a 12GB per-GPU request")
	}

	job.RequiredGPUMemoryMB = 8192
	worker.GPUType = " NVIDIA-H100 "
	if !WorkerCanRun(job, worker, now, HeartbeatTTL) {
		t.Fatal("matching live worker should fit an 8GB per-GPU request")
	}
	worker.LastHeartbeatAt = now.Add(-HeartbeatTTL - time.Second)
	if WorkerCanRun(job, worker, now, HeartbeatTTL) {
		t.Fatal("stale worker heartbeat must make the worker ineligible")
	}
}

func TestDispatchOnceAssignsUntilSnapshotCapacityIsUsed(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	fs := newFakeStore()
	for _, id := range []string{"run-a", "run-b", "run-c"} {
		jobID := "job-" + id
		fs.addJob(&model.Job{ID: jobID, Name: jobID, Priority: 1, RequiredCPUMillis: 400, RequiredMemoryMB: 100})
		fs.runs = append(fs.runs, &model.JobRun{
			ID: id, JobID: jobID, Status: model.RunStatusQueued, Attempt: 1, Priority: 1,
			ScheduledAt: now.Add(-time.Minute), CreatedAt: now.Add(-time.Minute),
		})
	}
	_ = fs.UpsertWorkerHeartbeat(ctx, model.Worker{
		ID: "cpu", Status: model.WorkerStatusAlive, LastHeartbeatAt: now,
		CPUCapacity: 800, MemoryCapacityMB: 1000,
	})
	dispatcher := NewDispatcher(fs, testLogger())
	dispatcher.Now = func() time.Time { return now }

	assigned, err := dispatcher.DispatchOnce(ctx)
	if err != nil {
		t.Fatalf("DispatchOnce() error = %v", err)
	}
	if assigned != 2 {
		t.Fatalf("assigned %d runs, want 2 while CPU capacity permits two", assigned)
	}
	counts := statusCounts(fs)
	if counts[model.RunStatusAssigned] != 2 || counts[model.RunStatusScheduled] != 1 {
		t.Fatalf("run statuses after dispatch = %v, want 2 assigned and 1 scheduled", counts)
	}
}

func TestDispatchOnceSkipsIneligibleHeadAndRaceRejectedPair(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	fs := newFakeStore()
	fs.addJob(&model.Job{ID: "gpu-job", Priority: 10, RequiredGPUCount: 1, RequiredGPUMemoryMB: 4096})
	fs.addJob(&model.Job{ID: "rejected-job", Priority: 9, RequiredCPUMillis: 1})
	fs.addJob(&model.Job{ID: "eligible-job", Priority: 1, RequiredCPUMillis: 1})
	fs.runs = append(fs.runs,
		&model.JobRun{ID: "gpu", JobID: "gpu-job", Status: model.RunStatusQueued, Priority: 10, ScheduledAt: now.Add(-time.Minute)},
		&model.JobRun{ID: "rejected", JobID: "rejected-job", Status: model.RunStatusQueued, Priority: 9, ScheduledAt: now.Add(-time.Minute)},
		&model.JobRun{ID: "eligible", JobID: "eligible-job", Status: model.RunStatusQueued, Priority: 1, ScheduledAt: now.Add(-time.Minute)},
	)
	fs.rejectRuns["rejected"] = true
	_ = fs.UpsertWorkerHeartbeat(ctx, model.Worker{ID: "cpu", Status: model.WorkerStatusAlive, LastHeartbeatAt: now, CPUCapacity: 100, MemoryCapacityMB: 100})
	dispatcher := NewDispatcher(fs, testLogger())
	dispatcher.Now = func() time.Time { return now }

	assigned, err := dispatcher.DispatchOnce(ctx)
	if err != nil {
		t.Fatalf("DispatchOnce() error = %v", err)
	}
	if assigned != 1 {
		t.Fatalf("assigned %d runs, want eligible run despite earlier ineligible/race-rejected candidates", assigned)
	}
	if fs.assignCalls[assignmentPair{runID: "rejected", workerID: "cpu"}] != 1 {
		t.Fatalf("race-rejected pair attempted %d times, want exactly once", fs.assignCalls[assignmentPair{runID: "rejected", workerID: "cpu"}])
	}
	counts := statusCounts(fs)
	if counts[model.RunStatusAssigned] != 1 || counts[model.RunStatusScheduled] != 2 {
		t.Fatalf("run statuses after dispatch = %v, want one assigned and two scheduled", counts)
	}
}

func TestDispatchOncePaginatesPastIneligibleCandidates(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	fs := newFakeStore()
	for i := 0; i < scheduledRunsLimit+1; i++ {
		jobID := fmt.Sprintf("gpu-job-%04d", i)
		runID := fmt.Sprintf("gpu-run-%04d", i)
		fs.addJob(&model.Job{ID: jobID, Priority: 10, RequiredGPUCount: 1, RequiredGPUMemoryMB: 4096})
		fs.runs = append(fs.runs, &model.JobRun{
			ID: runID, JobID: jobID, Status: model.RunStatusQueued, Priority: 10,
			ScheduledAt: now.Add(-time.Minute), CreatedAt: now.Add(-time.Minute),
		})
	}
	fs.addJob(&model.Job{ID: "cpu-job", Priority: 1, RequiredCPUMillis: 1})
	fs.runs = append(fs.runs, &model.JobRun{
		ID: "cpu-run", JobID: "cpu-job", Status: model.RunStatusQueued, Priority: 1,
		ScheduledAt: now.Add(-time.Minute), CreatedAt: now.Add(-time.Minute),
	})
	_ = fs.UpsertWorkerHeartbeat(ctx, model.Worker{ID: "cpu", Status: model.WorkerStatusAlive, LastHeartbeatAt: now, CPUCapacity: 100, MemoryCapacityMB: 100})
	dispatcher := NewDispatcher(fs, testLogger())
	dispatcher.Now = func() time.Time { return now }

	assigned, err := dispatcher.DispatchOnce(ctx)
	if err != nil {
		t.Fatalf("DispatchOnce() error = %v", err)
	}
	if assigned != 1 {
		t.Fatalf("assigned %d runs, want the feasible CPU run after the first page of GPU-only work", assigned)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.runs[len(fs.runs)-1].Status != model.RunStatusAssigned {
		t.Fatalf("CPU run status = %s, want assigned", fs.runs[len(fs.runs)-1].Status)
	}
}

func TestDispatchOnceRecoversExpiredLeaseBeforeAssignment(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	fs := newFakeStore()
	fs.addJob(&model.Job{ID: "job", RequiredCPUMillis: 500, RequiredMemoryMB: 100})
	oldOwner := "old-worker"
	leasedAt := now.Add(-time.Minute)
	expiredAt := now.Add(-time.Second)
	fs.runs = append(fs.runs, &model.JobRun{
		ID: "expired", JobID: "job", Status: model.RunStatusRunning, Priority: 1,
		ScheduledAt: now.Add(-time.Minute), LeasedBy: &oldOwner, LeasedAt: &leasedAt, LeaseExpiresAt: &expiredAt,
	})
	_ = fs.UpsertWorkerHeartbeat(ctx, model.Worker{ID: "live", Status: model.WorkerStatusAlive, LastHeartbeatAt: now, CPUCapacity: 1000, MemoryCapacityMB: 1000})
	dispatcher := NewDispatcher(fs, testLogger())
	dispatcher.Now = func() time.Time { return now }

	assigned, err := dispatcher.DispatchOnce(ctx)
	if err != nil {
		t.Fatalf("DispatchOnce() error = %v", err)
	}
	if assigned != 1 {
		t.Fatalf("assigned %d recovered runs, want 1", assigned)
	}
	fs.mu.Lock()
	run := fs.runs[0]
	reclaimCalls := fs.reclaimCalls
	fs.mu.Unlock()
	if reclaimCalls != 1 || run.Status != model.RunStatusAssigned || run.AssignedWorkerID == nil || *run.AssignedWorkerID != "live" {
		t.Fatalf("recovered run = %+v, reclaim calls = %d; want assigned to live after one reclaim", run, reclaimCalls)
	}
}

func TestPromoterRunDispatchesOnlyAfterLeadership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now()
	fs := newFakeStore()
	fs.addJob(&model.Job{ID: "job", Name: "job", Status: model.JobStatusActive, CreatedAt: now.Add(-time.Hour)})
	_ = fs.UpsertWorkerHeartbeat(ctx, model.Worker{ID: "worker", Status: model.WorkerStatusAlive, LastHeartbeatAt: now, CPUCapacity: 100, MemoryCapacityMB: 100})
	elector := newFailoverElector()
	promoter := NewPromoter(fs, elector, testLogger(), time.Millisecond)
	runErr := make(chan error, 1)
	go func() { runErr <- promoter.Run(ctx) }()

	select {
	case <-elector.firstTry:
	case <-time.After(time.Second):
		t.Fatal("Run did not attempt leadership")
	}
	select {
	case <-elector.secondTry:
	case <-time.After(time.Second):
		t.Fatal("Run did not retry leadership")
	}
	fs.mu.Lock()
	preLeaderRuns := len(fs.runs)
	preLeaderScheduleCalls := fs.scheduleCalls
	fs.mu.Unlock()
	if preLeaderRuns != 0 || preLeaderScheduleCalls != 0 {
		t.Fatalf("work happened before leadership: runs=%d dispatches=%d", preLeaderRuns, preLeaderScheduleCalls)
	}

	close(elector.grant)
	if !waitFor(time.Second, func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return len(fs.runs) == 1 && fs.runs[0].Status == model.RunStatusAssigned
	}) {
		t.Fatal("leader did not promote and assign the queued run")
	}
	cancel()
	select {
	case <-runErr:
	case <-time.After(time.Second):
		t.Fatal("promoter did not stop after cancellation")
	}
}

func testWorker(id string, now time.Time, cpu, memory, gpuCount, gpuMemory int32) *model.Worker {
	return &model.Worker{
		ID: id, Status: model.WorkerStatusAlive, LastHeartbeatAt: now,
		CPUCapacity: cpu, MemoryCapacityMB: memory, GPUCount: gpuCount, GPUMemoryMB: gpuMemory,
		AvailableCPUMillis: int64(cpu), AvailableMemoryMB: int64(memory),
		AvailableGPUCount: int64(gpuCount), AvailableGPUMemoryMB: int64(gpuCount) * int64(gpuMemory),
	}
}

func statusCounts(fs *fakeStore) map[model.RunStatus]int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	counts := make(map[model.RunStatus]int)
	for _, run := range fs.runs {
		counts[run.Status]++
	}
	return counts
}

func waitFor(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return condition()
}

type failoverElector struct {
	mu        sync.Mutex
	calls     int
	firstTry  chan struct{}
	secondTry chan struct{}
	grant     chan struct{}
}

func newFailoverElector() *failoverElector {
	return &failoverElector{
		firstTry:  make(chan struct{}),
		secondTry: make(chan struct{}),
		grant:     make(chan struct{}),
	}
}

func (e *failoverElector) TryAcquire(ctx context.Context) (bool, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()
	switch call {
	case 1:
		close(e.firstTry)
		return false, nil
	case 2:
		close(e.secondTry)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-e.grant:
			return true, nil
		}
	default:
		return true, nil
	}
}

func (e *failoverElector) Release(context.Context) error { return nil }
