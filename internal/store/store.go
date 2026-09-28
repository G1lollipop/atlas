// Package store defines the persistence contract used by the api, scheduler and worker
// services. The only production implementation is Postgres (internal/store/postgres.go),
// but code that only needs Store should depend on this interface, not the concrete type,
// so tests can substitute an in-memory fake.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

// ErrNotFound is returned by Get*/single-row lookups when nothing matches.
var ErrNotFound = errors.New("store: not found")

// ErrIdempotencyConflict is returned by CreateJob when the given idempotency key
// already exists for a different job.
var ErrIdempotencyConflict = errors.New("store: idempotency key already used")

type Store interface {
	// --- Jobs ---
	CreateJob(ctx context.Context, in model.NewJobInput) (*model.Job, error)
	GetJob(ctx context.Context, id string) (*model.Job, error)
	// GetJobByIdempotencyKey returns ErrNotFound if no job has this key. Used by the API
	// to make POST /v1/jobs idempotent: a retried create with the same key returns the
	// original job instead of erroring or creating a duplicate.
	GetJobByIdempotencyKey(ctx context.Context, key string) (*model.Job, error)
	ListJobs(ctx context.Context, status *model.JobStatus, limit, offset int) ([]*model.Job, error)
	UpdateJobStatus(ctx context.Context, id string, status model.JobStatus) error
	ListDependencies(ctx context.Context, jobID string) ([]string, error)

	// --- Runs: scheduling / promotion ---
	// LatestRunForJob returns the most recently created run for a job, or ErrNotFound if none exists.
	LatestRunForJob(ctx context.Context, jobID string) (*model.JobRun, error)
	// HasActiveRun reports whether jobID has a run in pending/leased/running state
	// (used to avoid double-scheduling the same job concurrently).
	HasActiveRun(ctx context.Context, jobID string) (bool, error)
	// CreateRun inserts a new pending run for jobID.
	CreateRun(ctx context.Context, jobID string, priority int16, scheduledAt time.Time) (*model.JobRun, error)

	// --- Runs: worker lease lifecycle ---
	// LeaseNextRun atomically claims the highest-priority, oldest pending run whose
	// resource requirements fit the registered worker's currently available capacity.
	// Returns (nil, nil, nil) if no eligible run is available.
	LeaseNextRun(ctx context.Context, workerID string, leaseDuration time.Duration) (*model.JobRun, *model.Job, error)
	// ExtendLease pushes lease_expires_at forward only for the current owner of an
	// active run; called periodically by the worker while executing.
	ExtendLease(ctx context.Context, runID, workerID string, attempt int16, extend time.Duration) error
	// Lifecycle writes are scoped to the worker that owns the live lease, preventing
	// an expired/reclaimed execution from changing a later attempt's state.
	MarkRunning(ctx context.Context, runID, workerID string, attempt int16) error
	CompleteRun(ctx context.Context, runID, workerID string, attempt int16, result map[string]any) error
	// FailRun records an error. If requeue is true the run goes back to pending with
	// attempt+1 and scheduled_at = now+backoff; otherwise (attempts exhausted) the
	// caller should follow up with MarkDead.
	FailRun(ctx context.Context, runID, workerID string, attempt int16, errMsg string, requeue bool, backoff time.Duration) error
	MarkDead(ctx context.Context, runID string, reason string) error
	// ReclaimExpiredLeases resets any leased/running run whose lease has expired back
	// to pending (crash recovery for workers that died mid-execution). Returns the count reclaimed.
	ReclaimExpiredLeases(ctx context.Context) (int, error)

	GetRun(ctx context.Context, id string) (*model.JobRun, error)
	ListJobRuns(ctx context.Context, jobID string, limit int) ([]*model.JobRun, error)
	// CountPendingRuns is used to publish the atlas_queue_depth gauge.
	CountPendingRuns(ctx context.Context) (int, error)

	// --- Workers ---
	UpsertWorkerHeartbeat(ctx context.Context, worker model.Worker) error
	ListWorkers(ctx context.Context) ([]*model.Worker, error)

	Close()
}
