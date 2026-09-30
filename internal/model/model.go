// Package model holds the shared domain types used across api, scheduler, worker and store.
// It intentionally has zero dependencies on the other internal packages so everything else
// can depend on it without import cycles.
package model

import "time"

type JobStatus string

const (
	JobStatusActive   JobStatus = "active"
	JobStatusPaused   JobStatus = "paused"
	JobStatusArchived JobStatus = "archived"
	JobStatusCanceled JobStatus = "canceled"
)

type RunStatus string

const (
	RunStatusQueued    RunStatus = "queued"
	RunStatusScheduled RunStatus = "scheduled"
	RunStatusAssigned  RunStatus = "assigned"
	RunStatusLeased    RunStatus = "leased"
	RunStatusRunning   RunStatus = "running"
	RunStatusSucceeded RunStatus = "succeeded"
	RunStatusFailed    RunStatus = "failed"
	RunStatusDead      RunStatus = "dead"
	RunStatusCanceled  RunStatus = "canceled"
)

// Job is a job definition: either a one-shot task or a recurring (cron_expr set) template.
// Each time a Job becomes eligible to run, a JobRun is created for it.
type Job struct {
	ID                  string         `json:"id"`
	Name                string         `json:"name"`
	Payload             map[string]any `json:"payload"`
	CronExpr            *string        `json:"cron_expr,omitempty"`
	Priority            int16          `json:"priority"`
	WorkloadType        string         `json:"workload_type"`
	Queue               string         `json:"queue"`
	TenantID            string         `json:"tenant_id"`
	RequiredCPUMillis   int32          `json:"required_cpu_millis"`
	RequiredMemoryMB    int32          `json:"required_memory_mb"`
	RequiredGPUCount    int32          `json:"required_gpu_count"`
	RequiredGPUMemoryMB int32          `json:"required_gpu_memory_mb"`
	RequiredAccelerator string         `json:"required_accelerator"`
	MaxAttempts         int16          `json:"max_attempts"`
	TimeoutSeconds      int32          `json:"timeout_seconds"`
	Status              JobStatus      `json:"status"`
	IdempotencyKey      *string        `json:"idempotency_key,omitempty"`
	DependsOn           []string       `json:"depends_on,omitempty"` // job IDs this job's runs must wait on
	TraceParent         string         `json:"-"`                    // Server-managed W3C context; never accepted from API JSON.
	TraceState          string         `json:"-"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

// JobRun is a single execution attempt (or series of attempts) of a Job.
type JobRun struct {
	ID string `json:"id"`
	// ExecutionKey identifies this run across lease reclamation, retries, and
	// manual dead-letter retries. Handlers can use it to deduplicate side effects.
	ExecutionKey        string         `json:"execution_key"`
	JobID               string         `json:"job_id"`
	Status              RunStatus      `json:"status"`
	Attempt             int16          `json:"attempt"`
	Priority            int16          `json:"priority"`
	ScheduledAt         time.Time      `json:"scheduled_at"`
	LeasedBy            *string        `json:"leased_by,omitempty"`
	LeasedAt            *time.Time     `json:"leased_at,omitempty"`
	LeaseExpiresAt      *time.Time     `json:"lease_expires_at,omitempty"`
	AssignedWorkerID    *string        `json:"assigned_worker_id,omitempty"`
	AssignedAt          *time.Time     `json:"assigned_at,omitempty"`
	AssignmentExpiresAt *time.Time     `json:"assignment_expires_at,omitempty"`
	StartedAt           *time.Time     `json:"started_at,omitempty"`
	FinishedAt          *time.Time     `json:"finished_at,omitempty"`
	Result              map[string]any `json:"result,omitempty"`
	Error               *string        `json:"error,omitempty"`
	TraceParent         string         `json:"-"` // Server-managed W3C context; never accepted from API JSON.
	TraceState          string         `json:"-"`
	CancelRequestedAt   *time.Time     `json:"cancel_requested_at,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
}

// RunCandidate is a scheduled run together with the job requirements used by
// the scheduler to choose a worker.
type RunCandidate struct {
	Run *JobRun `json:"run"`
	Job *Job    `json:"job"`
}

type WorkerStatus string

const (
	WorkerStatusAlive    WorkerStatus = "alive"
	WorkerStatusDraining WorkerStatus = "draining"
	WorkerStatusDead     WorkerStatus = "dead"
)

type Worker struct {
	ID                       string            `json:"id"`
	Hostname                 string            `json:"hostname"`
	Status                   WorkerStatus      `json:"status"`
	LastHeartbeatAt          time.Time         `json:"last_heartbeat_at"`
	StartedAt                time.Time         `json:"started_at"`
	CPUCapacity              int32             `json:"cpu_capacity"`
	MemoryCapacityMB         int32             `json:"memory_capacity_mb"`
	GPUCount                 int32             `json:"gpu_count"`
	GPUType                  string            `json:"gpu_type"`
	GPUMemoryMB              int32             `json:"gpu_memory_mb"`
	Labels                   map[string]string `json:"labels"`
	CurrentCPUReserved       int64             `json:"current_cpu_reserved"`
	CurrentMemoryReserved    int64             `json:"current_memory_reserved"`
	CurrentGPUReserved       int64             `json:"current_gpu_reserved"`
	CurrentGPUMemoryReserved int64             `json:"current_gpu_memory_reserved"`
	AvailableCPUMillis       int64             `json:"available_cpu_millis"`
	AvailableMemoryMB        int64             `json:"available_memory_mb"`
	AvailableGPUCount        int64             `json:"available_gpu_count"`
	AvailableGPUMemoryMB     int64             `json:"available_gpu_memory_mb"`
}

type DeadLetter struct {
	ID        string         `json:"id"`
	JobRunID  string         `json:"job_run_id"`
	Reason    string         `json:"reason"`
	Payload   map[string]any `json:"payload,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

// NewJobInput / NewRunInput are the create-shape payloads used by the API and scheduler,
// kept distinct from the persisted Job/JobRun so server-assigned fields (ID, timestamps)
// can't be forged by a caller.
type NewJobInput struct {
	Name                string         `json:"name"`
	Payload             map[string]any `json:"payload"`
	CronExpr            *string        `json:"cron_expr,omitempty"`
	Priority            int16          `json:"priority"`
	WorkloadType        string         `json:"workload_type,omitempty"`
	Queue               string         `json:"queue,omitempty"`
	TenantID            string         `json:"-"` // Set from the verified JWT subject, never from the request body.
	RequiredCPUMillis   int32          `json:"required_cpu_millis,omitempty"`
	RequiredMemoryMB    int32          `json:"required_memory_mb,omitempty"`
	RequiredGPUCount    int32          `json:"required_gpu_count,omitempty"`
	RequiredGPUMemoryMB int32          `json:"required_gpu_memory_mb,omitempty"`
	RequiredAccelerator string         `json:"required_accelerator,omitempty"`
	MaxAttempts         int16          `json:"max_attempts"`
	TimeoutSeconds      int32          `json:"timeout_seconds"`
	IdempotencyKey      *string        `json:"idempotency_key,omitempty"`
	DependsOn           []string       `json:"depends_on,omitempty"`
}
