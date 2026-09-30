package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/G1lollipop/atlas/internal/model"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const jobColumns = `id, name, payload, cron_expr, priority, workload_type, queue, tenant_id, required_cpu_millis, required_memory_mb, required_gpu_count, required_gpu_memory_mb, required_accelerator, max_attempts, timeout_seconds, status, idempotency_key, traceparent, tracestate, created_at, updated_at`

const runColumns = `id, execution_key, job_id, status, attempt, priority, scheduled_at, leased_by, leased_at, lease_expires_at, assigned_worker_id, assigned_at, assignment_expires_at, started_at, finished_at, result, error, traceparent, tracestate, cancel_requested_at, created_at`

// PostgresStore is the production store.Store implementation backed by Postgres.
type PostgresStore struct {
	pool *pgxpool.Pool
	// queueLimits are applied transactionally by every path that creates active work.
	// Set them once at process startup; New installs safe nonzero defaults.
	queueLimits QueueLimits
	// replicaPool, if set via EnableReadReplica, is used for pure-read queries
	// (GetJob, ListJobs, GetRun, ...) so they don't compete with writes and leasing
	// for primary connections. It is nil unless a read replica is configured -
	// readPool() falls back to pool in that case, so this is purely additive.
	replicaPool *pgxpool.Pool
}

var _ Store = (*PostgresStore)(nil)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, letting helpers run either
// standalone or as part of a caller-managed transaction (e.g. LeaseNextRun, MarkDead).
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// rowScanner is satisfied by both pgx.Row and pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func New(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	poolConfig.ConnConfig.Tracer = newOtelQueryTracer()

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &PostgresStore{pool: pool, queueLimits: DefaultQueueLimits()}, nil
}

func (s *PostgresStore) Close() {
	s.pool.Close()
	if s.replicaPool != nil {
		s.replicaPool.Close()
	}
}

// Pool exposes the underlying connection pool so callers (e.g. cmd/* main.go) can run
// RunMigrations against the same pool the store uses, rather than opening a second one.
func (s *PostgresStore) Pool() *pgxpool.Pool {
	return s.pool
}

// SetQueueLimits configures the shared backlog quotas enforced by this store. API
// and scheduler replicas must use the same values; call before serving requests.
func (s *PostgresStore) SetQueueLimits(limits QueueLimits) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	s.queueLimits = limits
	return nil
}

// EnableReadReplica points all pure-read queries (GetJob, ListJobs, GetRun, ...) at a
// second Postgres connection - a streaming replica in a real deployment - instead of
// the primary pool. Writes and transactional reads (LeaseNextRun, MarkDead, ...)
// always stay on the primary: replication lag makes a replica read of run-leasing
// state unsafe, so only genuinely read-only, staleness-tolerant queries move here.
func (s *PostgresStore) EnableReadReplica(ctx context.Context, replicaURL string) error {
	poolConfig, err := pgxpool.ParseConfig(replicaURL)
	if err != nil {
		return fmt.Errorf("parse replica database url: %w", err)
	}
	poolConfig.ConnConfig.Tracer = newOtelQueryTracer()

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("create replica pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ping replica database: %w", err)
	}
	s.replicaPool = pool
	return nil
}

// readPool returns the replica pool if one is configured, else the primary - so every
// read method below can call s.readPool() unconditionally.
func (s *PostgresStore) readPool() *pgxpool.Pool {
	if s.replicaPool != nil {
		return s.replicaPool
	}
	return s.pool
}

func scanJobRow(ctx context.Context, q querier, row rowScanner) (*model.Job, error) {
	var job model.Job
	var payloadRaw []byte
	if err := row.Scan(&job.ID, &job.Name, &payloadRaw, &job.CronExpr, &job.Priority, &job.WorkloadType, &job.Queue, &job.TenantID,
		&job.RequiredCPUMillis, &job.RequiredMemoryMB, &job.RequiredGPUCount, &job.RequiredGPUMemoryMB,
		&job.RequiredAccelerator, &job.MaxAttempts, &job.TimeoutSeconds, &job.Status, &job.IdempotencyKey,
		&job.TraceParent, &job.TraceState, &job.CreatedAt, &job.UpdatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(payloadRaw, &job.Payload); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}
	deps, err := fetchDependencies(ctx, q, job.ID)
	if err != nil {
		return nil, err
	}
	job.DependsOn = deps
	return &job, nil
}

func fetchJob(ctx context.Context, q querier, id string) (*model.Job, error) {
	row := q.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	return scanJobRow(ctx, q, row)
}

func fetchJobByIdempotencyKey(ctx context.Context, q querier, key string) (*model.Job, error) {
	row := q.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE idempotency_key = $1`, key)
	return scanJobRow(ctx, q, row)
}

func fetchDependencies(ctx context.Context, q querier, jobID string) ([]string, error) {
	rows, err := q.Query(ctx, `SELECT depends_on_id FROM job_dependencies WHERE job_id = $1`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deps []string
	for rows.Next() {
		var dep string
		if err := rows.Scan(&dep); err != nil {
			return nil, err
		}
		deps = append(deps, dep)
	}
	return deps, rows.Err()
}

func scanRun(row rowScanner) (*model.JobRun, error) {
	var run model.JobRun
	var resultRaw []byte
	if err := row.Scan(&run.ID, &run.ExecutionKey, &run.JobID, &run.Status, &run.Attempt, &run.Priority, &run.ScheduledAt,
		&run.LeasedBy, &run.LeasedAt, &run.LeaseExpiresAt, &run.AssignedWorkerID, &run.AssignedAt,
		&run.AssignmentExpiresAt, &run.StartedAt, &run.FinishedAt, &resultRaw, &run.Error,
		&run.TraceParent, &run.TraceState, &run.CancelRequestedAt, &run.CreatedAt); err != nil {
		return nil, err
	}
	if resultRaw != nil {
		if err := json.Unmarshal(resultRaw, &run.Result); err != nil {
			return nil, fmt.Errorf("unmarshal result: %w", err)
		}
	}
	return &run, nil
}

func fetchRun(ctx context.Context, q querier, id string) (*model.JobRun, error) {
	row := q.QueryRow(ctx, `SELECT `+runColumns+` FROM job_runs WHERE id = $1`, id)
	return scanRun(row)
}

func (s *PostgresStore) CreateJob(ctx context.Context, in model.NewJobInput) (out *model.Job, retErr error) {
	opCtx, span := otel.Tracer("atlas/store").Start(ctx, "api.CreateJob")
	defer func() { finishTransitionSpan(span, retErr) }()
	ctx = opCtx
	payloadRaw, err := json.Marshal(in.Payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	workloadType := strings.TrimSpace(in.WorkloadType)
	if workloadType == "" {
		workloadType = "generic"
	}
	queue := NormalizeQueue(in.Queue)
	tenant := NormalizeTenant(in.TenantID)
	if err := ValidateQueue(queue); err != nil {
		return nil, err
	}
	if err := ValidateTenant(tenant); err != nil {
		return nil, err
	}
	traceparent, tracestate := persistedTraceContext(ctx)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize capacity-bearing submissions with scheduler promotion. Also serialize
	// idempotency-key checks so a concurrent replay sees the original job before quota
	// evaluation instead of being rejected for capacity it does not consume.
	needsAdmissionLock := in.CronExpr == nil || (in.IdempotencyKey != nil && *in.IdempotencyKey != "")
	if needsAdmissionLock {
		if err := lockQueueAdmissions(ctx, tx); err != nil {
			return nil, err
		}
		if in.IdempotencyKey != nil && *in.IdempotencyKey != "" {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE idempotency_key = $1)`, *in.IdempotencyKey).Scan(&exists); err != nil {
				return nil, err
			}
			if exists {
				return nil, ErrIdempotencyConflict
			}
		}
		if in.CronExpr == nil {
			if err := checkQueueAdmissionCapacity(ctx, tx, s.queueLimits, queue, tenant, false); err != nil {
				return nil, err
			}
		}
	}

	var job model.Job
	var outRaw []byte
	row := tx.QueryRow(ctx, `
		INSERT INTO jobs (name, payload, cron_expr, priority, workload_type, queue, tenant_id, required_cpu_millis,
			required_memory_mb, required_gpu_count, required_gpu_memory_mb, required_accelerator,
			max_attempts, timeout_seconds, idempotency_key, traceparent, tracestate)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
		RETURNING `+jobColumns, in.Name, payloadRaw, in.CronExpr, in.Priority, workloadType,
		queue, tenant, in.RequiredCPUMillis, in.RequiredMemoryMB, in.RequiredGPUCount, in.RequiredGPUMemoryMB,
		in.RequiredAccelerator, in.MaxAttempts, in.TimeoutSeconds, in.IdempotencyKey, traceparent, tracestate)

	if err := row.Scan(&job.ID, &job.Name, &outRaw, &job.CronExpr, &job.Priority, &job.WorkloadType, &job.Queue, &job.TenantID,
		&job.RequiredCPUMillis, &job.RequiredMemoryMB, &job.RequiredGPUCount, &job.RequiredGPUMemoryMB,
		&job.RequiredAccelerator, &job.MaxAttempts, &job.TimeoutSeconds, &job.Status, &job.IdempotencyKey,
		&job.TraceParent, &job.TraceState, &job.CreatedAt, &job.UpdatedAt); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, ErrIdempotencyConflict
		}
		return nil, fmt.Errorf("insert job: %w", err)
	}

	if err := json.Unmarshal(outRaw, &job.Payload); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	for _, dep := range in.DependsOn {
		if _, err := tx.Exec(ctx, `INSERT INTO job_dependencies (job_id, depends_on_id) VALUES ($1, $2)`, job.ID, dep); err != nil {
			return nil, fmt.Errorf("insert dependency: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	job.DependsOn = in.DependsOn
	return &job, nil
}

func (s *PostgresStore) GetJob(ctx context.Context, id string) (*model.Job, error) {
	job, err := fetchJob(ctx, s.readPool(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return job, nil
}

func (s *PostgresStore) GetJobByIdempotencyKey(ctx context.Context, key string) (*model.Job, error) {
	job, err := fetchJobByIdempotencyKey(ctx, s.readPool(), key)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return job, nil
}

func (s *PostgresStore) ListJobs(ctx context.Context, status *model.JobStatus, limit, offset int) ([]*model.Job, error) {
	var rows pgx.Rows
	var err error
	if status != nil {
		rows, err = s.readPool().Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE status = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
			*status, limit, offset)
	} else {
		rows, err = s.readPool().Query(ctx, `SELECT `+jobColumns+` FROM jobs ORDER BY created_at DESC LIMIT $1 OFFSET $2`,
			limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		var job model.Job
		var payloadRaw []byte
		if err := rows.Scan(&job.ID, &job.Name, &payloadRaw, &job.CronExpr, &job.Priority, &job.WorkloadType, &job.Queue, &job.TenantID,
			&job.RequiredCPUMillis, &job.RequiredMemoryMB, &job.RequiredGPUCount, &job.RequiredGPUMemoryMB,
			&job.RequiredAccelerator, &job.MaxAttempts, &job.TimeoutSeconds, &job.Status, &job.IdempotencyKey,
			&job.TraceParent, &job.TraceState, &job.CreatedAt, &job.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payloadRaw, &job.Payload); err != nil {
			return nil, fmt.Errorf("unmarshal payload: %w", err)
		}
		jobs = append(jobs, &job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, j := range jobs {
		deps, err := fetchDependencies(ctx, s.readPool(), j.ID)
		if err != nil {
			return nil, err
		}
		j.DependsOn = deps
	}
	return jobs, nil
}

func (s *PostgresStore) UpdateJobStatus(ctx context.Context, id string, status model.JobStatus) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = $1, updated_at = now()
		WHERE id = $2
		  AND status <> 'canceled'
		  AND NOT (status = 'archived' AND $1 <> 'archived')
	`, status, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var current model.JobStatus
		if err := s.pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, id).Scan(&current); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if current == model.JobStatusCanceled {
			return ErrJobCanceled
		}
		if current == model.JobStatusArchived && status != model.JobStatusArchived {
			return ErrJobArchived
		}
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) ListDependencies(ctx context.Context, jobID string) ([]string, error) {
	return fetchDependencies(ctx, s.readPool(), jobID)
}

func (s *PostgresStore) LatestRunForJob(ctx context.Context, jobID string) (*model.JobRun, error) {
	row := s.readPool().QueryRow(ctx, `SELECT `+runColumns+` FROM job_runs WHERE job_id = $1 ORDER BY created_at DESC LIMIT 1`, jobID)
	run, err := scanRun(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return run, nil
}

func (s *PostgresStore) HasActiveRun(ctx context.Context, jobID string) (bool, error) {
	var exists bool
	err := s.readPool().QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM job_runs WHERE job_id = $1
			AND status IN ('queued', 'scheduled', 'assigned', 'leased', 'running'))
	`, jobID).Scan(&exists)
	return exists, err
}

func (s *PostgresStore) CreateRun(ctx context.Context, jobID string, priority int16, scheduledAt time.Time) (out *model.JobRun, retErr error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status model.JobStatus
	var cronExpr *string
	var queue, tenant string
	var jobTraceparent, jobTracestate string
	err = tx.QueryRow(ctx, `SELECT status, cron_expr, queue, tenant_id, traceparent, tracestate FROM jobs WHERE id = $1 FOR UPDATE`, jobID).
		Scan(&status, &cronExpr, &queue, &tenant, &jobTraceparent, &jobTracestate)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	promotionCtx, promotionSpan := startStoredSpan(ctx, jobTraceparent, jobTracestate, "scheduler.PromoteJob",
		attribute.String("job.id", jobID))
	defer func() { finishTransitionSpan(promotionSpan, retErr) }()
	opCtx, span := otel.Tracer("atlas/store").Start(promotionCtx, "scheduler.CreateRun", trace.WithAttributes(
		attribute.String("job.id", jobID),
	))
	defer func() { finishTransitionSpan(span, retErr) }()
	if status != model.JobStatusActive {
		return nil, ErrNotFound
	}
	var hasPriorRun bool
	if err := tx.QueryRow(opCtx, `SELECT EXISTS(SELECT 1 FROM job_runs WHERE job_id = $1)`, jobID).Scan(&hasPriorRun); err != nil {
		return nil, err
	}
	var hasActiveRun bool
	if err := tx.QueryRow(opCtx, `
		SELECT EXISTS(SELECT 1 FROM job_runs WHERE job_id = $1
			AND status IN ('queued', 'scheduled', 'assigned', 'leased', 'running'))
	`, jobID).Scan(&hasActiveRun); err != nil {
		return nil, err
	}
	if hasActiveRun || (cronExpr == nil && hasPriorRun) {
		return nil, ErrRunAlreadyExists
	}
	reservationHeld := cronExpr == nil && !hasPriorRun
	if err := admitQueueWork(opCtx, tx, s.queueLimits, queue, tenant, reservationHeld); err != nil {
		return nil, err
	}
	traceparent, tracestate := persistedTraceContext(opCtx)

	row := tx.QueryRow(opCtx, `
		INSERT INTO job_runs (job_id, status, priority, scheduled_at, traceparent, tracestate)
		VALUES ($1, 'queued', $2, $3, $4, $5)
		RETURNING `+runColumns, jobID, priority, scheduledAt, traceparent, tracestate)
	run, err := scanRun(row)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(opCtx); err != nil {
		return nil, err
	}
	return run, nil
}

func (s *PostgresStore) ScheduleDueRuns(ctx context.Context) (int, error) {
	return transitionStoredRuns(ctx, s.pool, `
		SELECT id, traceparent, tracestate FROM job_runs
		WHERE status = 'queued' AND scheduled_at <= now()
		ORDER BY scheduled_at, id
		FOR UPDATE SKIP LOCKED
	`, `
		UPDATE job_runs AS run
		SET status = 'scheduled', traceparent = transition.traceparent, tracestate = transition.tracestate
		FROM unnest($1::text[], $2::text[], $3::text[]) AS transition(run_id, traceparent, tracestate)
		WHERE run.id = transition.run_id::uuid AND run.status = 'queued'
	`, "scheduler.ScheduleRun")
}

func (s *PostgresStore) RequeueExpiredAssignments(ctx context.Context) (int, error) {
	return transitionStoredRuns(ctx, s.pool, `
		SELECT id, traceparent, tracestate FROM job_runs
		WHERE status = 'assigned' AND assignment_expires_at <= now()
		ORDER BY assignment_expires_at, id
		FOR UPDATE SKIP LOCKED
	`, `
		UPDATE job_runs AS run
		SET status = 'queued', assigned_worker_id = NULL, assigned_at = NULL,
		    assignment_expires_at = NULL, traceparent = transition.traceparent, tracestate = transition.tracestate
		FROM unnest($1::text[], $2::text[], $3::text[]) AS transition(run_id, traceparent, tracestate)
		WHERE run.id = transition.run_id::uuid AND run.status = 'assigned'
	`, "scheduler.RequeueExpiredAssignment")
}

func (s *PostgresStore) ListScheduledRuns(ctx context.Context, limit, offset int) ([]*model.RunCandidate, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.id, r.execution_key, r.job_id, r.status, r.attempt, r.priority, r.scheduled_at,
		       r.leased_by, r.leased_at, r.lease_expires_at, r.assigned_worker_id,
	       r.assigned_at, r.assignment_expires_at, r.started_at, r.finished_at,
	       r.result, r.error, r.traceparent, r.tracestate, r.cancel_requested_at, r.created_at,
		       j.id, j.name, j.payload, j.cron_expr, j.priority, j.workload_type, j.queue, j.tenant_id,
		       j.required_cpu_millis, j.required_memory_mb, j.required_gpu_count,
		       j.required_gpu_memory_mb, j.required_accelerator, j.max_attempts,
	       j.timeout_seconds, j.status, j.idempotency_key, j.traceparent, j.tracestate, j.created_at, j.updated_at
		FROM job_runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE r.status = 'scheduled' AND r.scheduled_at <= now()
		ORDER BY ((r.priority::bigint * 3600) - EXTRACT(EPOCH FROM r.scheduled_at)) DESC,
		         r.scheduled_at ASC, r.created_at ASC, r.id ASC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candidates := make([]*model.RunCandidate, 0)
	for rows.Next() {
		var run model.JobRun
		var job model.Job
		var resultRaw, payloadRaw []byte
		if err := rows.Scan(&run.ID, &run.ExecutionKey, &run.JobID, &run.Status, &run.Attempt, &run.Priority,
			&run.ScheduledAt, &run.LeasedBy, &run.LeasedAt, &run.LeaseExpiresAt,
			&run.AssignedWorkerID, &run.AssignedAt, &run.AssignmentExpiresAt,
			&run.StartedAt, &run.FinishedAt, &resultRaw, &run.Error, &run.TraceParent, &run.TraceState, &run.CancelRequestedAt, &run.CreatedAt,
			&job.ID, &job.Name, &payloadRaw, &job.CronExpr, &job.Priority, &job.WorkloadType, &job.Queue, &job.TenantID,
			&job.RequiredCPUMillis, &job.RequiredMemoryMB, &job.RequiredGPUCount,
			&job.RequiredGPUMemoryMB, &job.RequiredAccelerator, &job.MaxAttempts,
			&job.TimeoutSeconds, &job.Status, &job.IdempotencyKey, &job.TraceParent, &job.TraceState, &job.CreatedAt, &job.UpdatedAt); err != nil {
			return nil, err
		}
		if resultRaw != nil {
			if err := json.Unmarshal(resultRaw, &run.Result); err != nil {
				return nil, fmt.Errorf("unmarshal result: %w", err)
			}
		}
		if err := json.Unmarshal(payloadRaw, &job.Payload); err != nil {
			return nil, fmt.Errorf("unmarshal payload: %w", err)
		}
		candidates = append(candidates, &model.RunCandidate{Run: &run, Job: &job})
	}
	return candidates, rows.Err()
}

func (s *PostgresStore) AssignRun(ctx context.Context, runID, workerID string, assignmentTTL, heartbeatTTL time.Duration) (assigned bool, retErr error) {
	var span trace.Span
	defer func() { finishTransitionSpan(span, retErr) }()
	if assignmentTTL <= 0 || heartbeatTTL <= 0 {
		return false, fmt.Errorf("assignment and heartbeat TTLs must be positive")
	}
	assignmentTTLMillis := durationMilliseconds(assignmentTTL)
	heartbeatTTLMillis := durationMilliseconds(heartbeatTTL)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var worker model.Worker
	err = tx.QueryRow(ctx, `
		SELECT cpu_capacity, memory_capacity_mb, gpu_count, gpu_type, gpu_memory_mb
		FROM workers
		WHERE id = $1 AND status = 'alive'
		  AND last_heartbeat_at > now() - ($2::bigint * INTERVAL '1 millisecond')
		FOR UPDATE SKIP LOCKED
	`, workerID, heartbeatTTLMillis).Scan(&worker.CPUCapacity, &worker.MemoryCapacityMB,
		&worker.GPUCount, &worker.GPUType, &worker.GPUMemoryMB)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	var jobID, runTraceparent, runTracestate string
	err = tx.QueryRow(ctx, `
		SELECT job_id, traceparent, tracestate
		FROM job_runs
		WHERE id = $1 AND status = 'scheduled' AND scheduled_at <= now()
		FOR UPDATE SKIP LOCKED
	`, runID).Scan(&jobID, &runTraceparent, &runTracestate)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	opCtx, started := startStoredSpan(ctx, runTraceparent, runTracestate, "scheduler.AssignRun",
		attribute.String("run.id", runID), attribute.String("worker.id", workerID))
	span = started

	var requiredCPU, requiredMemory, requiredGPU, requiredGPUMemory int64
	var requiredAccelerator string
	err = tx.QueryRow(opCtx, `
		SELECT required_cpu_millis, required_memory_mb, required_gpu_count,
		       required_gpu_memory_mb, required_accelerator
		FROM jobs WHERE id = $1
	`, jobID).Scan(&requiredCPU, &requiredMemory, &requiredGPU,
		&requiredGPUMemory, &requiredAccelerator)
	if err != nil {
		return false, err
	}

	var reservedCPU, reservedMemory, reservedGPU, reservedGPUMemory int64
	err = tx.QueryRow(opCtx, `
		SELECT COALESCE(SUM(j.required_cpu_millis), 0)::bigint,
		       COALESCE(SUM(j.required_memory_mb), 0)::bigint,
		       COALESCE(SUM(j.required_gpu_count), 0)::bigint,
	       COALESCE(SUM(j.required_gpu_count::bigint * j.required_gpu_memory_mb::bigint), 0)::bigint
		FROM job_runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE (r.status = 'assigned' AND r.assigned_worker_id = $1)
		   OR (r.status IN ('leased', 'running') AND r.leased_by = $1)
	`, workerID).Scan(&reservedCPU, &reservedMemory, &reservedGPU, &reservedGPUMemory)
	if err != nil {
		return false, err
	}

	if requiredCPU > int64(worker.CPUCapacity)-reservedCPU ||
		requiredMemory > int64(worker.MemoryCapacityMB)-reservedMemory ||
		requiredGPU > int64(worker.GPUCount)-reservedGPU ||
		(requiredGPU > 0 && requiredGPUMemory > int64(worker.GPUMemoryMB)) ||
		requiredGPU*requiredGPUMemory > int64(worker.GPUCount)*int64(worker.GPUMemoryMB)-reservedGPUMemory ||
		(requiredAccelerator != "" && !strings.EqualFold(strings.TrimSpace(requiredAccelerator), strings.TrimSpace(worker.GPUType))) {
		return false, nil
	}

	traceparent, tracestate := persistedTraceContext(opCtx)
	tag, err := tx.Exec(opCtx, `
		UPDATE job_runs
		SET status = 'assigned', assigned_worker_id = $1, assigned_at = now(),
		    assignment_expires_at = now() + ($2::bigint * INTERVAL '1 millisecond'),
		    traceparent = $3, tracestate = $4
		WHERE id = $5 AND status = 'scheduled'
	`, workerID, assignmentTTLMillis, traceparent, tracestate, runID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if err := tx.Commit(opCtx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *PostgresStore) LeaseNextRun(ctx context.Context, workerID string, leaseDuration time.Duration) (leasedRun *model.JobRun, leasedJob *model.Job, retErr error) {
	var span trace.Span
	defer func() { finishTransitionSpan(span, retErr) }()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Keep lock order consistent with AssignRun: worker first, then run. Concurrent
	// pollers for one worker serialize here before they inspect reserved capacity.
	var worker model.Worker
	err = tx.QueryRow(ctx, `
		SELECT cpu_capacity, memory_capacity_mb, gpu_count, gpu_type, gpu_memory_mb
		FROM workers
		WHERE id = $1 AND status = 'alive'
		FOR UPDATE SKIP LOCKED
	`, workerID).Scan(&worker.CPUCapacity, &worker.MemoryCapacityMB,
		&worker.GPUCount, &worker.GPUType, &worker.GPUMemoryMB)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	var runID, jobID, runTraceparent, runTracestate string
	// Assignments are already placed by the scheduler policy. Preserve the same
	// priority-plus-age ordering when one worker has multiple assignments.
	err = tx.QueryRow(ctx, `
		SELECT r.id, r.job_id, r.traceparent, r.tracestate
		FROM job_runs r
		WHERE r.status = 'assigned' AND r.assigned_worker_id = $1
		  AND r.assignment_expires_at > now() AND r.scheduled_at <= now()
		ORDER BY ((r.priority::bigint * 3600) - EXTRACT(EPOCH FROM r.scheduled_at)) DESC,
		         r.scheduled_at ASC, r.created_at ASC, r.id ASC
		LIMIT 1
		FOR UPDATE OF r SKIP LOCKED
	`, workerID).Scan(&runID, &jobID, &runTraceparent, &runTracestate)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	opCtx, started := startStoredSpan(ctx, runTraceparent, runTracestate, "worker.LeaseRun",
		attribute.String("run.id", runID), attribute.String("worker.id", workerID))
	span = started

	var requiredCPU, requiredMemory, requiredGPU, requiredGPUMemory int64
	var requiredAccelerator string
	if err := tx.QueryRow(opCtx, `
		SELECT required_cpu_millis, required_memory_mb, required_gpu_count,
		       required_gpu_memory_mb, required_accelerator
		FROM jobs WHERE id = $1
	`, jobID).Scan(&requiredCPU, &requiredMemory, &requiredGPU,
		&requiredGPUMemory, &requiredAccelerator); err != nil {
		return nil, nil, err
	}

	var reservedCPU, reservedMemory, reservedGPU, reservedGPUMemory int64
	if err := tx.QueryRow(opCtx, `
		SELECT COALESCE(SUM(j.required_cpu_millis), 0)::bigint,
		       COALESCE(SUM(j.required_memory_mb), 0)::bigint,
		       COALESCE(SUM(j.required_gpu_count), 0)::bigint,
		       COALESCE(SUM(j.required_gpu_count::bigint * j.required_gpu_memory_mb::bigint), 0)::bigint
		FROM job_runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE r.id <> $1 AND (
		    (r.status = 'assigned' AND r.assigned_worker_id = $2)
		    OR (r.status IN ('leased', 'running') AND r.leased_by = $2)
		)
	`, runID, workerID).Scan(&reservedCPU, &reservedMemory, &reservedGPU, &reservedGPUMemory); err != nil {
		return nil, nil, err
	}

	fits := requiredCPU <= int64(worker.CPUCapacity)-reservedCPU &&
		requiredMemory <= int64(worker.MemoryCapacityMB)-reservedMemory &&
		requiredGPU <= int64(worker.GPUCount)-reservedGPU &&
		(requiredGPU == 0 || requiredGPUMemory <= int64(worker.GPUMemoryMB)) &&
		requiredGPU*requiredGPUMemory <= int64(worker.GPUCount)*int64(worker.GPUMemoryMB)-reservedGPUMemory &&
		(requiredAccelerator == "" || strings.EqualFold(strings.TrimSpace(requiredAccelerator), strings.TrimSpace(worker.GPUType)))
	if !fits {
		traceparent, tracestate := persistedTraceContext(opCtx)
		if _, err := tx.Exec(opCtx, `
			UPDATE job_runs
			SET status = 'queued', scheduled_at = now(), assigned_worker_id = NULL,
			    assigned_at = NULL, assignment_expires_at = NULL, traceparent = $3, tracestate = $4
			WHERE id = $1 AND status = 'assigned' AND assigned_worker_id = $2
		`, runID, workerID, traceparent, tracestate); err != nil {
			return nil, nil, err
		}
		if err := tx.Commit(opCtx); err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	}

	traceparent, tracestate := persistedTraceContext(opCtx)
	if _, err := tx.Exec(opCtx, `
		UPDATE job_runs
		SET status = 'leased', leased_by = $1, leased_at = now(),
		    lease_expires_at = now() + ($2::bigint * INTERVAL '1 millisecond'), attempt = attempt + 1,
		    traceparent = $3, tracestate = $4
		WHERE id = $5 AND status = 'assigned' AND assigned_worker_id = $1
	`, workerID, durationMilliseconds(leaseDuration), traceparent, tracestate, runID); err != nil {
		return nil, nil, err
	}
	run, err := fetchRun(opCtx, tx, runID)
	if err != nil {
		return nil, nil, err
	}
	job, err := fetchJob(opCtx, tx, jobID)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(opCtx); err != nil {
		return nil, nil, err
	}
	return run, job, nil
}
func (s *PostgresStore) ExtendLease(ctx context.Context, runID, workerID string, attempt int16, extend time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs SET lease_expires_at = now() + ($1::bigint * INTERVAL '1 millisecond')
		WHERE id = $2 AND leased_by = $3 AND attempt = $4
		  AND status IN ('leased', 'running') AND lease_expires_at > now()
	`, durationMilliseconds(extend), runID, workerID, attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) MarkRunning(ctx context.Context, runID, workerID string, attempt int16) error {
	traceparent, tracestate := persistedTraceContext(ctx)
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs
		SET status = 'running', started_at = now(),
		    traceparent = CASE
		      WHEN $1 <> '' AND (job_runs.traceparent = '' OR split_part(job_runs.traceparent, '-', 2) = split_part($1, '-', 2)) THEN $1
		      ELSE job_runs.traceparent
		    END,
		    tracestate = CASE
		      WHEN $1 <> '' AND (job_runs.traceparent = '' OR split_part(job_runs.traceparent, '-', 2) = split_part($1, '-', 2)) THEN $2
		      ELSE job_runs.tracestate
		    END
		WHERE id = $3 AND leased_by = $4 AND attempt = $5
		  AND status = 'leased' AND lease_expires_at > now()
		  AND cancel_requested_at IS NULL
	`, traceparent, tracestate, runID, workerID, attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if err := s.MarkCanceled(ctx, runID, workerID, attempt); err == nil {
			return ErrCanceled
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) CompleteRun(ctx context.Context, runID, workerID string, attempt int16, result map[string]any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	traceparent, tracestate := persistedTraceContext(ctx)
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs
		SET status = 'succeeded', finished_at = now(), result = $1,
		    traceparent = CASE
		      WHEN $2 <> '' AND (job_runs.traceparent = '' OR split_part(job_runs.traceparent, '-', 2) = split_part($2, '-', 2)) THEN $2
		      ELSE job_runs.traceparent
		    END,
		    tracestate = CASE
		      WHEN $2 <> '' AND (job_runs.traceparent = '' OR split_part(job_runs.traceparent, '-', 2) = split_part($2, '-', 2)) THEN $3
		      ELSE job_runs.tracestate
		    END,
		    leased_by = NULL, leased_at = NULL, lease_expires_at = NULL
		WHERE id = $4 AND leased_by = $5 AND attempt = $6
		  AND status = 'running' AND lease_expires_at > now()
		  AND cancel_requested_at IS NULL
	`, raw, traceparent, tracestate, runID, workerID, attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if err := s.MarkCanceled(ctx, runID, workerID, attempt); err == nil {
			return ErrCanceled
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) FailRun(ctx context.Context, runID, workerID string, attempt int16, errMsg string, requeue bool, backoff time.Duration) error {
	var tag pgconn.CommandTag
	var err error
	traceparent, tracestate := persistedTraceContext(ctx)
	if requeue {
		tag, err = s.pool.Exec(ctx, `
			UPDATE job_runs
			SET status = 'queued', scheduled_at = now() + ($1::bigint * INTERVAL '1 millisecond'), error = $2,
			    traceparent = CASE
			      WHEN $3 <> '' AND (job_runs.traceparent = '' OR split_part(job_runs.traceparent, '-', 2) = split_part($3, '-', 2)) THEN $3
			      ELSE job_runs.traceparent
			    END,
			    tracestate = CASE
			      WHEN $3 <> '' AND (job_runs.traceparent = '' OR split_part(job_runs.traceparent, '-', 2) = split_part($3, '-', 2)) THEN $4
			      ELSE job_runs.tracestate
			    END,
			    leased_by = NULL, leased_at = NULL, lease_expires_at = NULL,
			    assigned_worker_id = NULL, assigned_at = NULL, assignment_expires_at = NULL
			WHERE id = $5 AND leased_by = $6 AND attempt = $7
			  AND status IN ('leased', 'running') AND lease_expires_at > now()
			  AND cancel_requested_at IS NULL
			  AND EXISTS (SELECT 1 FROM jobs WHERE jobs.id = job_runs.job_id AND jobs.status <> 'canceled')
		`, durationMilliseconds(backoff), errMsg, traceparent, tracestate, runID, workerID, attempt)
	} else {
		tag, err = s.pool.Exec(ctx, `
			UPDATE job_runs
			SET status = 'failed', finished_at = now(), error = $1,
			    traceparent = CASE
			      WHEN $2 <> '' AND (job_runs.traceparent = '' OR split_part(job_runs.traceparent, '-', 2) = split_part($2, '-', 2)) THEN $2
			      ELSE job_runs.traceparent
			    END,
			    tracestate = CASE
			      WHEN $2 <> '' AND (job_runs.traceparent = '' OR split_part(job_runs.traceparent, '-', 2) = split_part($2, '-', 2)) THEN $3
			      ELSE job_runs.tracestate
			    END,
			    leased_by = NULL, leased_at = NULL, lease_expires_at = NULL
			WHERE id = $4 AND leased_by = $5 AND attempt = $6
			  AND status IN ('leased', 'running') AND lease_expires_at > now()
			  AND cancel_requested_at IS NULL
			  AND EXISTS (SELECT 1 FROM jobs WHERE jobs.id = job_runs.job_id AND jobs.status <> 'canceled')
		`, errMsg, traceparent, tracestate, runID, workerID, attempt)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if err := s.MarkCanceled(ctx, runID, workerID, attempt); err == nil {
			return ErrCanceled
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) MarkDead(ctx context.Context, runID string, reason string) (retErr error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	lockedRun, err := scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM job_runs WHERE id = $1 FOR UPDATE`, runID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if lockedRun.Status != model.RunStatusFailed {
		return ErrNotFound
	}
	opCtx, span := startStoredSpan(ctx, lockedRun.TraceParent, lockedRun.TraceState, "worker.MarkDead",
		attribute.String("run.id", runID))
	defer func() { finishTransitionSpan(span, retErr) }()

	payloadRaw, err := json.Marshal(map[string]any{"result": lockedRun.Result, "error": lockedRun.Error})
	if err != nil {
		return fmt.Errorf("marshal dead letter payload: %w", err)
	}

	if _, err := tx.Exec(opCtx, `
		INSERT INTO dead_letters (job_run_id, reason, payload) VALUES ($1, $2, $3)
	`, runID, reason, payloadRaw); err != nil {
		return err
	}

	traceparent, tracestate := persistedTraceContext(opCtx)
	tag, err := tx.Exec(opCtx, `
		UPDATE job_runs SET status = 'dead', traceparent = $1, tracestate = $2
		WHERE id = $3 AND status = 'failed'
	`, traceparent, tracestate, runID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return tx.Commit(opCtx)
}

func (s *PostgresStore) ReclaimExpiredLeases(ctx context.Context) (int, error) {
	return transitionStoredRuns(ctx, s.pool, `
		SELECT id, traceparent, tracestate FROM job_runs
		WHERE status IN ('leased', 'running') AND lease_expires_at < now()
		ORDER BY lease_expires_at, id
		FOR UPDATE SKIP LOCKED
	`, `
		UPDATE job_runs AS run
		SET status = CASE WHEN run.cancel_requested_at IS NULL THEN 'queued' ELSE 'canceled' END,
		    finished_at = CASE WHEN run.cancel_requested_at IS NULL THEN run.finished_at ELSE COALESCE(run.finished_at, now()) END,
		    leased_by = NULL, leased_at = NULL, lease_expires_at = NULL,
		    assigned_worker_id = NULL, assigned_at = NULL, assignment_expires_at = NULL,
		    traceparent = transition.traceparent, tracestate = transition.tracestate
		FROM unnest($1::text[], $2::text[], $3::text[]) AS transition(run_id, traceparent, tracestate)
		WHERE run.id = transition.run_id::uuid AND run.status IN ('leased', 'running')
	`, "worker.ReclaimExpiredLease")
}

func (s *PostgresStore) GetRun(ctx context.Context, id string) (*model.JobRun, error) {
	run, err := fetchRun(ctx, s.readPool(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return run, nil
}

func (s *PostgresStore) ListJobRuns(ctx context.Context, jobID string, limit int) ([]*model.JobRun, error) {
	rows, err := s.readPool().Query(ctx, `SELECT `+runColumns+` FROM job_runs WHERE job_id = $1 ORDER BY created_at DESC LIMIT $2`,
		jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*model.JobRun
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *PostgresStore) CountPendingRuns(ctx context.Context) (int, error) {
	var count int
	err := s.readPool().QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE status IN ('queued', 'scheduled')`).Scan(&count)
	return count, err
}

func (s *PostgresStore) UpsertWorkerHeartbeat(ctx context.Context, worker model.Worker) error {
	labels, err := json.Marshal(worker.Labels)
	if err != nil {
		return fmt.Errorf("marshal worker labels: %w", err)
	}
	if len(labels) == 0 || string(labels) == "null" {
		labels = []byte(`{}`)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO workers (
			id, hostname, status, last_heartbeat_at, cpu_capacity, memory_capacity_mb,
			gpu_count, gpu_type, gpu_memory_mb, labels
		)
		VALUES ($1, $2, 'alive', now(), $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			hostname = EXCLUDED.hostname,
			status = 'alive',
			last_heartbeat_at = now(),
			cpu_capacity = EXCLUDED.cpu_capacity,
			memory_capacity_mb = EXCLUDED.memory_capacity_mb,
			gpu_count = EXCLUDED.gpu_count,
			gpu_type = EXCLUDED.gpu_type,
			gpu_memory_mb = EXCLUDED.gpu_memory_mb,
			labels = EXCLUDED.labels
	`, worker.ID, worker.Hostname, worker.CPUCapacity, worker.MemoryCapacityMB,
		worker.GPUCount, worker.GPUType, worker.GPUMemoryMB, labels)
	return err
}

func (s *PostgresStore) ListWorkers(ctx context.Context) ([]*model.Worker, error) {
	// Availability must reflect primary-side lease changes immediately; using a
	// potentially lagging read replica could advertise resources already reserved.
	rows, err := s.pool.Query(ctx, `
		SELECT w.id, w.hostname, w.status, w.last_heartbeat_at, w.started_at,
		       w.cpu_capacity, w.memory_capacity_mb, w.gpu_count, w.gpu_type,
		       w.gpu_memory_mb, w.labels,
		       COALESCE(res.cpu_reserved, 0)::bigint,
		       COALESCE(res.memory_reserved, 0)::bigint,
		       COALESCE(res.gpu_reserved, 0)::bigint,
		       COALESCE(res.gpu_memory_reserved, 0)::bigint
		FROM workers w
		LEFT JOIN LATERAL (
			SELECT SUM(j.required_cpu_millis)::bigint AS cpu_reserved,
			       SUM(j.required_memory_mb)::bigint AS memory_reserved,
			       SUM(j.required_gpu_count)::bigint AS gpu_reserved,
			       SUM(j.required_gpu_count::bigint * j.required_gpu_memory_mb::bigint)::bigint AS gpu_memory_reserved
			FROM job_runs r
			JOIN jobs j ON j.id = r.job_id
			WHERE (r.status = 'assigned' AND r.assigned_worker_id = w.id)
			   OR (r.status IN ('leased', 'running') AND r.leased_by = w.id)
		) res ON TRUE
		ORDER BY w.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workers []*model.Worker
	for rows.Next() {
		var w model.Worker
		var labels []byte
		if err := rows.Scan(&w.ID, &w.Hostname, &w.Status, &w.LastHeartbeatAt, &w.StartedAt,
			&w.CPUCapacity, &w.MemoryCapacityMB, &w.GPUCount, &w.GPUType, &w.GPUMemoryMB, &labels,
			&w.CurrentCPUReserved, &w.CurrentMemoryReserved, &w.CurrentGPUReserved, &w.CurrentGPUMemoryReserved); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(labels, &w.Labels); err != nil {
			return nil, fmt.Errorf("unmarshal worker labels: %w", err)
		}
		w.AvailableCPUMillis = max(0, int64(w.CPUCapacity)-w.CurrentCPUReserved)
		w.AvailableMemoryMB = max(0, int64(w.MemoryCapacityMB)-w.CurrentMemoryReserved)
		w.AvailableGPUCount = max(0, int64(w.GPUCount)-w.CurrentGPUReserved)
		w.AvailableGPUMemoryMB = max(0, int64(w.GPUCount)*int64(w.GPUMemoryMB)-w.CurrentGPUMemoryReserved)
		workers = append(workers, &w)
	}
	return workers, rows.Err()
}

func durationMilliseconds(d time.Duration) int64 {
	millis := int64(d / time.Millisecond)
	if remainder := d % time.Millisecond; remainder != 0 {
		if d > 0 {
			millis++
		} else {
			millis--
		}
	}
	return millis
}
