package worker

import (
	"context"
	"sync"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
)

// fakeStore is a minimal in-memory store.Store implementation used to drive
// Pool.executeOne without a real Postgres. It records which lifecycle methods were
// called (and with what arguments) so tests can assert executeOne's retry/dead
// decisions without caring about persistence details.
type fakeStore struct {
	mu sync.Mutex

	markRunningCalls     []string
	markRunningWorkerIDs []string
	markRunningAttempts  []int16
	completeRunCalls     []completeRunCall
	failRunCalls         []failRunCall
	markDeadCalls        []markDeadCall
	heartbeatCalls       []model.Worker
	extendLeaseCalls     []extendLeaseCall
	operationOrder       []string
	upsertErrors         []error
	leaseNotify          chan struct{}
	leaseCandidates      []leaseCandidate
	leaseWorkerIDs       []string
	markRunningErr       error
	failRunErr           error
	completeRunErr       error
	fenceAttempts        bool
	currentAttempt       int16
}

type completeRunCall struct {
	runID    string
	workerID string
	attempt  int16
	result   map[string]any
}

type failRunCall struct {
	runID    string
	workerID string
	attempt  int16
	errMsg   string
	requeue  bool
	backoff  time.Duration
}

type markDeadCall struct {
	runID  string
	reason string
}

type leaseCandidate struct {
	run *model.JobRun
	job *model.Job
}

func newFakeStore() *fakeStore {
	return &fakeStore{}
}

// --- Jobs (unused by pool tests; minimal stubs) ---

func (f *fakeStore) CreateJob(ctx context.Context, in model.NewJobInput) (*model.Job, error) {
	return nil, nil
}
func (f *fakeStore) GetJob(ctx context.Context, id string) (*model.Job, error) { return nil, nil }
func (f *fakeStore) GetJobByIdempotencyKey(ctx context.Context, key string) (*model.Job, error) {
	return nil, nil
}
func (f *fakeStore) ListJobs(ctx context.Context, status *model.JobStatus, limit, offset int) ([]*model.Job, error) {
	return nil, nil
}
func (f *fakeStore) UpdateJobStatus(ctx context.Context, id string, status model.JobStatus) error {
	return nil
}
func (f *fakeStore) ListDependencies(ctx context.Context, jobID string) ([]string, error) {
	return nil, nil
}

// --- Runs: scheduling / promotion (unused by pool tests; minimal stubs) ---

func (f *fakeStore) LatestRunForJob(ctx context.Context, jobID string) (*model.JobRun, error) {
	return nil, store.ErrNotFound
}
func (f *fakeStore) HasActiveRun(ctx context.Context, jobID string) (bool, error) {
	return false, nil
}
func (f *fakeStore) CreateRun(ctx context.Context, jobID string, priority int16, scheduledAt time.Time) (*model.JobRun, error) {
	return nil, nil
}
func (f *fakeStore) ScheduleDueRuns(ctx context.Context) (int, error) { return 0, nil }
func (f *fakeStore) RequeueExpiredAssignments(ctx context.Context) (int, error) {
	return 0, nil
}
func (f *fakeStore) ListScheduledRuns(ctx context.Context, limit, offset int) ([]*model.RunCandidate, error) {
	return nil, nil
}
func (f *fakeStore) AssignRun(ctx context.Context, runID, workerID string, assignmentTTL, heartbeatTTL time.Duration) (bool, error) {
	return false, nil
}

// --- Runs: worker lease lifecycle ---

func (f *fakeStore) LeaseNextRun(ctx context.Context, workerID string, leaseDuration time.Duration) (*model.JobRun, *model.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.operationOrder = append(f.operationOrder, "lease")
	f.leaseWorkerIDs = append(f.leaseWorkerIDs, workerID)
	if f.leaseNotify != nil {
		select {
		case f.leaseNotify <- struct{}{}:
		default:
		}
	}
	for i, candidate := range f.leaseCandidates {
		if candidate.run == nil || candidate.job == nil || candidate.run.Status != model.RunStatusAssigned {
			continue
		}
		if candidate.run.AssignedWorkerID == nil || *candidate.run.AssignedWorkerID != workerID {
			continue
		}
		if candidate.run.AssignmentExpiresAt == nil || !candidate.run.AssignmentExpiresAt.After(time.Now()) {
			continue
		}

		now := time.Now().UTC()
		candidate.run.Status = model.RunStatusLeased
		candidate.run.LeasedBy = stringPointer(workerID)
		candidate.run.LeasedAt = &now
		leaseExpiresAt := now.Add(leaseDuration)
		candidate.run.LeaseExpiresAt = &leaseExpiresAt
		candidate.run.Attempt++
		f.leaseCandidates = append(f.leaseCandidates[:i], f.leaseCandidates[i+1:]...)
		return candidate.run, candidate.job, nil
	}
	return nil, nil, nil
}

func stringPointer(value string) *string { return &value }

type extendLeaseCall struct {
	runID         string
	workerID      string
	attempt       int16
	leaseDuration time.Duration
}

func (f *fakeStore) ExtendLease(ctx context.Context, runID, workerID string, attempt int16, extend time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.extendLeaseCalls = append(f.extendLeaseCalls, extendLeaseCall{runID: runID, workerID: workerID, attempt: attempt, leaseDuration: extend})
	if f.fenceAttempts && attempt != f.currentAttempt {
		return store.ErrNotFound
	}
	return nil
}

func (f *fakeStore) MarkRunning(ctx context.Context, runID, workerID string, attempt int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markRunningCalls = append(f.markRunningCalls, runID)
	f.markRunningWorkerIDs = append(f.markRunningWorkerIDs, workerID)
	f.markRunningAttempts = append(f.markRunningAttempts, attempt)
	if f.fenceAttempts && attempt != f.currentAttempt {
		return store.ErrNotFound
	}
	return f.markRunningErr
}

func (f *fakeStore) CompleteRun(ctx context.Context, runID, workerID string, attempt int16, result map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completeRunCalls = append(f.completeRunCalls, completeRunCall{runID: runID, workerID: workerID, attempt: attempt, result: result})
	if f.fenceAttempts && attempt != f.currentAttempt {
		return store.ErrNotFound
	}
	return f.completeRunErr
}

func (f *fakeStore) FailRun(ctx context.Context, runID, workerID string, attempt int16, errMsg string, requeue bool, backoff time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failRunCalls = append(f.failRunCalls, failRunCall{runID: runID, workerID: workerID, attempt: attempt, errMsg: errMsg, requeue: requeue, backoff: backoff})
	if f.fenceAttempts && attempt != f.currentAttempt {
		return store.ErrNotFound
	}
	return f.failRunErr
}

func (f *fakeStore) MarkDead(ctx context.Context, runID string, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markDeadCalls = append(f.markDeadCalls, markDeadCall{runID: runID, reason: reason})
	return nil
}

func (f *fakeStore) ReclaimExpiredLeases(ctx context.Context) (int, error) {
	return 0, nil
}

func (f *fakeStore) GetRun(ctx context.Context, id string) (*model.JobRun, error) {
	return nil, store.ErrNotFound
}

func (f *fakeStore) ListJobRuns(ctx context.Context, jobID string, limit int) ([]*model.JobRun, error) {
	return nil, nil
}

func (f *fakeStore) CountPendingRuns(ctx context.Context) (int, error) {
	return 0, nil
}

// --- Workers (unused by pool tests; minimal stubs) ---

func (f *fakeStore) UpsertWorkerHeartbeat(ctx context.Context, worker model.Worker) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.operationOrder = append(f.operationOrder, "heartbeat")
	labels := make(map[string]string, len(worker.Labels))
	for key, value := range worker.Labels {
		labels[key] = value
	}
	worker.Labels = labels
	f.heartbeatCalls = append(f.heartbeatCalls, worker)
	if len(f.upsertErrors) == 0 {
		return nil
	}
	err := f.upsertErrors[0]
	f.upsertErrors = f.upsertErrors[1:]
	return err
}

func (f *fakeStore) ListWorkers(ctx context.Context) ([]*model.Worker, error) {
	return nil, nil
}

func (f *fakeStore) Close() {}

var _ store.Store = (*fakeStore)(nil)
