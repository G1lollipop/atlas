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

// ErrCanceled is returned when a lifecycle write observes a durable cancellation
// request and records the run as canceled instead of completing or retrying it.
var ErrCanceled = errors.New("store: run canceled")

// ErrJobCanceled is returned when an attempt is made to pause or resume a job
// after cancellation, which is a terminal job state.
var ErrJobCanceled = errors.New("store: job canceled")

// ErrJobArchived is returned when a caller attempts to resume an archived job.
// Archived jobs cannot be reactivated because that would bypass admission control
// for accepted one-shot work.
var ErrJobArchived = errors.New("store: job archived")

// ErrRunAlreadyExists means a one-shot job has already been materialized or a
// prior promotion created active work while this caller was waiting for its row lock.
var ErrRunAlreadyExists = errors.New("store: job run already exists")

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
	// CancelJob atomically marks the job canceled, cancels runs that have not started,
	// and requests cancellation for leased/running attempts.
	CancelJob(ctx context.Context, id string) error
	ListDependencies(ctx context.Context, jobID string) ([]string, error)

	// --- Runs: scheduling / promotion ---
	// LatestRunForJob returns the most recently created run for a job, or ErrNotFound if none exists.
	LatestRunForJob(ctx context.Context, jobID string) (*model.JobRun, error)
	// HasActiveRun reports whether jobID has queued, scheduled, assigned, leased,
	// or running work (used to avoid double-scheduling the same job concurrently).
	HasActiveRun(ctx context.Context, jobID string) (bool, error)
	// CreateRun inserts a new queued run for jobID.
	CreateRun(ctx context.Context, jobID string, priority int16, scheduledAt time.Time) (*model.JobRun, error)
	// ScheduleDueRuns advances due queued runs to scheduled.
	ScheduleDueRuns(ctx context.Context) (int, error)
	// RequeueExpiredAssignments returns expired assignments to the queue so the
	// scheduler can place them again.
	RequeueExpiredAssignments(ctx context.Context) (int, error)
	// ListScheduledRuns returns a page of due scheduled runs with their job
	// requirements, ordered by priority plus age for deterministic policy evaluation.
	ListScheduledRuns(ctx context.Context, limit, offset int) ([]*model.RunCandidate, error)
	// AssignRun atomically reserves a scheduled run on a live worker if its current
	// capabilities and unreserved capacity satisfy the job requirements. It returns
	// false for stale workers, contention, or ineligible capacity.
	AssignRun(ctx context.Context, runID, workerID string, assignmentTTL, heartbeatTTL time.Duration) (bool, error)

	// --- Runs: worker lease lifecycle ---
	// LeaseNextRun atomically claims the highest-ranked unexpired assignment made to
	// workerID (priority plus waiting age), rechecking available capacity. It never
	// searches for unassigned runs. Returns (nil, nil, nil) if none is eligible.
	LeaseNextRun(ctx context.Context, workerID string, leaseDuration time.Duration) (*model.JobRun, *model.Job, error)
	// ExtendLease pushes lease_expires_at forward only for the current owner of an
	// active run; called periodically by the worker while executing.
	ExtendLease(ctx context.Context, runID, workerID string, attempt int16, extend time.Duration) error
	// Lifecycle writes are scoped to the worker that owns the live lease, preventing
	// an expired/reclaimed execution from changing a later attempt's state.
	MarkRunning(ctx context.Context, runID, workerID string, attempt int16) error
	// CancellationRequested returns true when the current lease owner has a durable
	// cancellation request. It is polled while a handler is running.
	CancellationRequested(ctx context.Context, runID, workerID string, attempt int16) (bool, error)
	// MarkCanceled acknowledges cancellation for the current leased attempt.
	MarkCanceled(ctx context.Context, runID, workerID string, attempt int16) error
	CompleteRun(ctx context.Context, runID, workerID string, attempt int16, result map[string]any) error
	// FailRun records an error. If requeue is true the run goes back to queued at
	// now+backoff; its attempt increments when a worker next leases it. Otherwise
	// (attempts exhausted) the caller should follow up with MarkDead.
	FailRun(ctx context.Context, runID, workerID string, attempt int16, errMsg string, requeue bool, backoff time.Duration) error
	MarkDead(ctx context.Context, runID string, reason string) error
	// ReclaimExpiredLeases resets any leased/running run whose lease has expired back
	// to queued (crash recovery for workers that died mid-execution). Returns the count reclaimed.
	ReclaimExpiredLeases(ctx context.Context) (int, error)

	GetRun(ctx context.Context, id string) (*model.JobRun, error)
	ListJobRuns(ctx context.Context, jobID string, limit int) ([]*model.JobRun, error)

	// --- Dead letters ---
	// ListDeadLetters returns the current primary-store view, newest first.
	ListDeadLetters(ctx context.Context, limit, offset int) ([]*model.DeadLetter, error)
	// RetryDeadLetter atomically removes the letter and resets its dead run to queued.
	RetryDeadLetter(ctx context.Context, id string) (*model.JobRun, error)
	// DeleteDeadLetter discards a letter while leaving its run in the dead state.
	DeleteDeadLetter(ctx context.Context, id string) error

	// CountPendingRuns counts the unassigned backlog (queued + scheduled) for the
	// atlas_queue_depth gauge.
	CountPendingRuns(ctx context.Context) (int, error)

	// --- Workers ---
	UpsertWorkerHeartbeat(ctx context.Context, worker model.Worker) error
	ListWorkers(ctx context.Context) ([]*model.Worker, error)

	Close()
}
