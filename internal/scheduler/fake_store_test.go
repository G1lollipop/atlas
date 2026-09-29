package scheduler

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
)

// fakeStore is a minimal in-memory implementation of store.Store used to unit test
// scheduler logic (NextRunDue, DependenciesSatisfied, PromoteOnce) without a real
// Postgres. Only the behavior the scheduler package actually exercises is real;
// everything else is a zero-value stub so the type satisfies store.Store.
type fakeStore struct {
	mu sync.Mutex

	jobs      map[string]*model.Job
	dependsOn map[string][]string // jobID -> depends on these jobIDs
	workers   map[string]*model.Worker

	runs         []*model.JobRun
	activeRunJob map[string]bool // jobID -> has an active (queued/scheduled/assigned/leased/running) run
	nextRunID    int

	scheduleCalls int
	reclaimCalls  int
	requeueCalls  int
	assignCalls   map[assignmentPair]int
	rejectRuns    map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		jobs:         make(map[string]*model.Job),
		dependsOn:    make(map[string][]string),
		workers:      make(map[string]*model.Worker),
		activeRunJob: make(map[string]bool),
		assignCalls:  make(map[assignmentPair]int),
		rejectRuns:   make(map[string]bool),
	}
}

// addJob registers a job (and its dependency list) directly into the fake store,
// bypassing CreateJob's idempotency-key bookkeeping which scheduler tests don't need.
func (f *fakeStore) addJob(job *model.Job, dependsOn ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs[job.ID] = job
	if len(dependsOn) > 0 {
		f.dependsOn[job.ID] = dependsOn
	}
}

// --- Jobs ---

func (f *fakeStore) CreateJob(ctx context.Context, in model.NewJobInput) (*model.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := "job-" + in.Name
	job := &model.Job{
		ID:                  id,
		Name:                in.Name,
		Payload:             in.Payload,
		CronExpr:            in.CronExpr,
		Priority:            in.Priority,
		WorkloadType:        in.WorkloadType,
		RequiredCPUMillis:   in.RequiredCPUMillis,
		RequiredMemoryMB:    in.RequiredMemoryMB,
		RequiredGPUCount:    in.RequiredGPUCount,
		RequiredGPUMemoryMB: in.RequiredGPUMemoryMB,
		RequiredAccelerator: in.RequiredAccelerator,
		MaxAttempts:         in.MaxAttempts,
		TimeoutSeconds:      in.TimeoutSeconds,
		Status:              model.JobStatusActive,
		IdempotencyKey:      in.IdempotencyKey,
		DependsOn:           in.DependsOn,
		CreatedAt:           time.Now(),
		UpdatedAt:           time.Now(),
	}
	f.jobs[id] = job
	if len(in.DependsOn) > 0 {
		f.dependsOn[id] = in.DependsOn
	}
	return job, nil
}

func (f *fakeStore) GetJob(ctx context.Context, id string) (*model.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	job, ok := f.jobs[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return job, nil
}

func (f *fakeStore) GetJobByIdempotencyKey(ctx context.Context, key string) (*model.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, job := range f.jobs {
		if job.IdempotencyKey != nil && *job.IdempotencyKey == key {
			return job, nil
		}
	}
	return nil, store.ErrNotFound
}

func (f *fakeStore) ListJobs(ctx context.Context, status *model.JobStatus, limit, offset int) ([]*model.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []*model.Job
	for _, job := range f.jobs {
		if status != nil && job.Status != *status {
			continue
		}
		out = append(out, job)
	}
	return out, nil
}

func (f *fakeStore) UpdateJobStatus(ctx context.Context, id string, status model.JobStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	job, ok := f.jobs[id]
	if !ok {
		return store.ErrNotFound
	}
	job.Status = status
	return nil
}

func (f *fakeStore) ListDependencies(ctx context.Context, jobID string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dependsOn[jobID], nil
}

// --- Runs: scheduling / promotion ---

func (f *fakeStore) LatestRunForJob(ctx context.Context, jobID string) (*model.JobRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var latest *model.JobRun
	for _, run := range f.runs {
		if run.JobID != jobID {
			continue
		}
		if latest == nil || run.CreatedAt.After(latest.CreatedAt) {
			latest = run
		}
	}
	if latest == nil {
		return nil, store.ErrNotFound
	}
	return latest, nil
}

func (f *fakeStore) HasActiveRun(ctx context.Context, jobID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.activeRunJob[jobID], nil
}

func (f *fakeStore) CreateRun(ctx context.Context, jobID string, priority int16, scheduledAt time.Time) (*model.JobRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nextRunID++
	run := &model.JobRun{
		ID:          "run-" + jobIDSuffix(f.nextRunID),
		JobID:       jobID,
		Status:      model.RunStatusQueued,
		Attempt:     1,
		Priority:    priority,
		ScheduledAt: scheduledAt,
		CreatedAt:   time.Now(),
	}
	f.runs = append(f.runs, run)
	f.activeRunJob[jobID] = true
	return run, nil
}

func (f *fakeStore) ScheduleDueRuns(ctx context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scheduleCalls++
	now := time.Now()
	count := 0
	for _, run := range f.runs {
		if run.Status == model.RunStatusQueued && !run.ScheduledAt.After(now) {
			run.Status = model.RunStatusScheduled
			count++
		}
	}
	return count, nil
}

func (f *fakeStore) RequeueExpiredAssignments(ctx context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requeueCalls++
	now := time.Now()
	count := 0
	for _, run := range f.runs {
		if run.Status == model.RunStatusAssigned && run.AssignmentExpiresAt != nil && !run.AssignmentExpiresAt.After(now) {
			run.Status = model.RunStatusQueued
			run.AssignedWorkerID = nil
			run.AssignedAt = nil
			run.AssignmentExpiresAt = nil
			count++
		}
	}
	return count, nil
}

func (f *fakeStore) ListScheduledRuns(ctx context.Context, limit, offset int) ([]*model.RunCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	var candidates []*model.RunCandidate
	for _, run := range f.runs {
		if run.Status != model.RunStatusScheduled || run.ScheduledAt.After(now) {
			continue
		}
		job, ok := f.jobs[run.JobID]
		if !ok {
			continue
		}
		candidates = append(candidates, &model.RunCandidate{Run: run, Job: job})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Run.Priority != candidates[j].Run.Priority {
			return candidates[i].Run.Priority > candidates[j].Run.Priority
		}
		if !candidates[i].Run.ScheduledAt.Equal(candidates[j].Run.ScheduledAt) {
			return candidates[i].Run.ScheduledAt.Before(candidates[j].Run.ScheduledAt)
		}
		if !candidates[i].Run.CreatedAt.Equal(candidates[j].Run.CreatedAt) {
			return candidates[i].Run.CreatedAt.Before(candidates[j].Run.CreatedAt)
		}
		return candidates[i].Run.ID < candidates[j].Run.ID
	})
	if offset >= len(candidates) {
		return []*model.RunCandidate{}, nil
	}
	candidates = candidates[offset:]
	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates, nil
}

func (f *fakeStore) AssignRun(ctx context.Context, runID, workerID string, assignmentTTL, heartbeatTTL time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pair := assignmentPair{runID: runID, workerID: workerID}
	f.assignCalls[pair]++

	var run *model.JobRun
	for _, candidate := range f.runs {
		if candidate.ID == runID {
			run = candidate
			break
		}
	}
	_, ok := f.workers[workerID]
	if run == nil || run.Status != model.RunStatusScheduled || !ok || f.rejectRuns[runID] {
		return false, nil
	}
	job, ok := f.jobs[run.JobID]
	if !ok {
		return false, nil
	}
	workerSnapshot := f.workerSnapshotLocked(workerID)
	if !WorkerCanRun(job, workerSnapshot, time.Now(), heartbeatTTL) {
		return false, nil
	}
	now := time.Now()
	assignedWorkerID := workerID
	run.Status = model.RunStatusAssigned
	run.AssignedWorkerID = &assignedWorkerID
	run.AssignedAt = &now
	expires := now.Add(assignmentTTL)
	run.AssignmentExpiresAt = &expires
	return true, nil
}

func (f *fakeStore) workerSnapshotLocked(workerID string) *model.Worker {
	worker, ok := f.workers[workerID]
	if !ok {
		return nil
	}
	copyWorker := *worker
	copyWorker.CurrentCPUReserved = 0
	copyWorker.CurrentMemoryReserved = 0
	copyWorker.CurrentGPUReserved = 0
	copyWorker.CurrentGPUMemoryReserved = 0
	for _, run := range f.runs {
		owner := ""
		if run.Status == model.RunStatusAssigned && run.AssignedWorkerID != nil {
			owner = *run.AssignedWorkerID
		} else if (run.Status == model.RunStatusLeased || run.Status == model.RunStatusRunning) && run.LeasedBy != nil {
			owner = *run.LeasedBy
		}
		if owner != workerID {
			continue
		}
		job := f.jobs[run.JobID]
		if job == nil {
			continue
		}
		copyWorker.CurrentCPUReserved += int64(job.RequiredCPUMillis)
		copyWorker.CurrentMemoryReserved += int64(job.RequiredMemoryMB)
		copyWorker.CurrentGPUReserved += int64(job.RequiredGPUCount)
		copyWorker.CurrentGPUMemoryReserved += int64(job.RequiredGPUCount) * int64(job.RequiredGPUMemoryMB)
	}
	copyWorker.AvailableCPUMillis = maxAvailable(int64(copyWorker.CPUCapacity) - copyWorker.CurrentCPUReserved)
	copyWorker.AvailableMemoryMB = maxAvailable(int64(copyWorker.MemoryCapacityMB) - copyWorker.CurrentMemoryReserved)
	copyWorker.AvailableGPUCount = maxAvailable(int64(copyWorker.GPUCount) - copyWorker.CurrentGPUReserved)
	copyWorker.AvailableGPUMemoryMB = maxAvailable(int64(copyWorker.GPUCount)*int64(copyWorker.GPUMemoryMB) - copyWorker.CurrentGPUMemoryReserved)
	return &copyWorker
}

// setRunStatus is a test helper to move a run (and its job's "active" bookkeeping)
// into a terminal or non-terminal state, mirroring what CompleteRun/FailRun/MarkDead
// would do in the real store.
func (f *fakeStore) setRunStatus(runID string, status model.RunStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, run := range f.runs {
		if run.ID == runID {
			run.Status = status
			switch status {
			case model.RunStatusSucceeded, model.RunStatusFailed, model.RunStatusDead:
				f.activeRunJob[run.JobID] = false
			default:
				f.activeRunJob[run.JobID] = true
			}
			return
		}
	}
}

func jobIDSuffix(n int) string {
	digits := []byte{}
	if n == 0 {
		return "0"
	}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// --- Runs: worker lease lifecycle (unused by scheduler; minimal stubs) ---

func (f *fakeStore) LeaseNextRun(ctx context.Context, workerID string, leaseDuration time.Duration) (*model.JobRun, *model.Job, error) {
	return nil, nil, nil
}

func (f *fakeStore) ExtendLease(ctx context.Context, runID, workerID string, attempt int16, extend time.Duration) error {
	return nil
}

func (f *fakeStore) MarkRunning(ctx context.Context, runID, workerID string, attempt int16) error {
	return nil
}

func (f *fakeStore) CompleteRun(ctx context.Context, runID, workerID string, attempt int16, result map[string]any) error {
	f.setRunStatus(runID, model.RunStatusSucceeded)
	return nil
}

func (f *fakeStore) FailRun(ctx context.Context, runID, workerID string, attempt int16, errMsg string, requeue bool, backoff time.Duration) error {
	if requeue {
		f.setRunStatus(runID, model.RunStatusQueued)
	} else {
		f.setRunStatus(runID, model.RunStatusFailed)
	}
	return nil
}

func (f *fakeStore) MarkDead(ctx context.Context, runID string, reason string) error {
	f.setRunStatus(runID, model.RunStatusDead)
	return nil
}

func (f *fakeStore) ReclaimExpiredLeases(ctx context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reclaimCalls++
	now := time.Now()
	count := 0
	for _, run := range f.runs {
		if (run.Status == model.RunStatusLeased || run.Status == model.RunStatusRunning) && run.LeaseExpiresAt != nil && !run.LeaseExpiresAt.After(now) {
			run.Status = model.RunStatusQueued
			run.LeasedBy = nil
			run.LeasedAt = nil
			run.LeaseExpiresAt = nil
			count++
		}
	}
	return count, nil
}

func (f *fakeStore) GetRun(ctx context.Context, id string) (*model.JobRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, run := range f.runs {
		if run.ID == id {
			return run, nil
		}
	}
	return nil, store.ErrNotFound
}

func (f *fakeStore) ListJobRuns(ctx context.Context, jobID string, limit int) ([]*model.JobRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.JobRun
	for _, run := range f.runs {
		if run.JobID == jobID {
			out = append(out, run)
		}
	}
	return out, nil
}

func (f *fakeStore) ListDeadLetters(ctx context.Context, limit, offset int) ([]*model.DeadLetter, error) {
	return []*model.DeadLetter{}, nil
}

func (f *fakeStore) RetryDeadLetter(ctx context.Context, id string) (*model.JobRun, error) {
	return nil, store.ErrNotFound
}

func (f *fakeStore) DeleteDeadLetter(ctx context.Context, id string) error {
	return store.ErrNotFound
}

func (f *fakeStore) CountPendingRuns(ctx context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, run := range f.runs {
		if run.Status == model.RunStatusQueued || run.Status == model.RunStatusScheduled {
			n++
		}
	}
	return n, nil
}

// --- Workers (unused by scheduler; minimal stubs) ---

func (f *fakeStore) UpsertWorkerHeartbeat(ctx context.Context, worker model.Worker) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	copyWorker := worker
	if copyWorker.LastHeartbeatAt.IsZero() {
		copyWorker.LastHeartbeatAt = time.Now()
	}
	if copyWorker.Status == "" {
		copyWorker.Status = model.WorkerStatusAlive
	}
	f.workers[worker.ID] = &copyWorker
	return nil
}

func (f *fakeStore) ListWorkers(ctx context.Context) ([]*model.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	workers := make([]*model.Worker, 0, len(f.workers))
	for id := range f.workers {
		workers = append(workers, f.workerSnapshotLocked(id))
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	return workers, nil
}

func (f *fakeStore) Close() {}

var _ store.Store = (*fakeStore)(nil)
