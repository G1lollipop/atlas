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
)

const jobColumns = `id, name, payload, cron_expr, priority, workload_type, required_cpu_millis, required_memory_mb, required_gpu_count, required_gpu_memory_mb, required_accelerator, max_attempts, timeout_seconds, status, idempotency_key, created_at, updated_at`

const runColumns = `id, execution_key, job_id, status, attempt, priority, scheduled_at, leased_by, leased_at, lease_expires_at, assigned_worker_id, assigned_at, assignment_expires_at, started_at, finished_at, result, error, created_at`

// PostgresStore is the production store.Store implementation backed by Postgres.
type PostgresStore struct {
	pool *pgxpool.Pool
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
	return &PostgresStore{pool: pool}, nil
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
	if err := row.Scan(&job.ID, &job.Name, &payloadRaw, &job.CronExpr, &job.Priority, &job.WorkloadType,
		&job.RequiredCPUMillis, &job.RequiredMemoryMB, &job.RequiredGPUCount, &job.RequiredGPUMemoryMB,
		&job.RequiredAccelerator, &job.MaxAttempts, &job.TimeoutSeconds, &job.Status, &job.IdempotencyKey,
		&job.CreatedAt, &job.UpdatedAt); err != nil {
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
		&run.AssignmentExpiresAt, &run.StartedAt, &run.FinishedAt, &resultRaw, &run.Error, &run.CreatedAt); err != nil {
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

func (s *PostgresStore) CreateJob(ctx context.Context, in model.NewJobInput) (*model.Job, error) {
	payloadRaw, err := json.Marshal(in.Payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	workloadType := strings.TrimSpace(in.WorkloadType)
	if workloadType == "" {
		workloadType = "generic"
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var job model.Job
	var outRaw []byte
	row := tx.QueryRow(ctx, `
		INSERT INTO jobs (name, payload, cron_expr, priority, workload_type, required_cpu_millis,
			required_memory_mb, required_gpu_count, required_gpu_memory_mb, required_accelerator,
			max_attempts, timeout_seconds, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING `+jobColumns, in.Name, payloadRaw, in.CronExpr, in.Priority, workloadType,
		in.RequiredCPUMillis, in.RequiredMemoryMB, in.RequiredGPUCount, in.RequiredGPUMemoryMB,
		in.RequiredAccelerator, in.MaxAttempts, in.TimeoutSeconds, in.IdempotencyKey)

	if err := row.Scan(&job.ID, &job.Name, &outRaw, &job.CronExpr, &job.Priority, &job.WorkloadType,
		&job.RequiredCPUMillis, &job.RequiredMemoryMB, &job.RequiredGPUCount, &job.RequiredGPUMemoryMB,
		&job.RequiredAccelerator, &job.MaxAttempts, &job.TimeoutSeconds, &job.Status, &job.IdempotencyKey,
		&job.CreatedAt, &job.UpdatedAt); err != nil {
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
		if err := rows.Scan(&job.ID, &job.Name, &payloadRaw, &job.CronExpr, &job.Priority, &job.WorkloadType,
			&job.RequiredCPUMillis, &job.RequiredMemoryMB, &job.RequiredGPUCount, &job.RequiredGPUMemoryMB,
			&job.RequiredAccelerator, &job.MaxAttempts, &job.TimeoutSeconds, &job.Status, &job.IdempotencyKey,
			&job.CreatedAt, &job.UpdatedAt); err != nil {
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
	tag, err := s.pool.Exec(ctx, `UPDATE jobs SET status = $1, updated_at = now() WHERE id = $2`, status, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
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

func (s *PostgresStore) CreateRun(ctx context.Context, jobID string, priority int16, scheduledAt time.Time) (*model.JobRun, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO job_runs (job_id, status, priority, scheduled_at)
		VALUES ($1, 'queued', $2, $3)
		RETURNING `+runColumns, jobID, priority, scheduledAt)
	return scanRun(row)
}

func (s *PostgresStore) ScheduleDueRuns(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs
		SET status = 'scheduled'
		WHERE status = 'queued' AND scheduled_at <= now()
	`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStore) RequeueExpiredAssignments(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs
		SET status = 'queued', assigned_worker_id = NULL, assigned_at = NULL,
		    assignment_expires_at = NULL
		WHERE status = 'assigned' AND assignment_expires_at <= now()
	`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
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
		       r.result, r.error, r.created_at,
		       j.id, j.name, j.payload, j.cron_expr, j.priority, j.workload_type,
		       j.required_cpu_millis, j.required_memory_mb, j.required_gpu_count,
		       j.required_gpu_memory_mb, j.required_accelerator, j.max_attempts,
		       j.timeout_seconds, j.status, j.idempotency_key, j.created_at, j.updated_at
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
			&run.StartedAt, &run.FinishedAt, &resultRaw, &run.Error, &run.CreatedAt,
			&job.ID, &job.Name, &payloadRaw, &job.CronExpr, &job.Priority, &job.WorkloadType,
			&job.RequiredCPUMillis, &job.RequiredMemoryMB, &job.RequiredGPUCount,
			&job.RequiredGPUMemoryMB, &job.RequiredAccelerator, &job.MaxAttempts,
			&job.TimeoutSeconds, &job.Status, &job.IdempotencyKey, &job.CreatedAt, &job.UpdatedAt); err != nil {
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

func (s *PostgresStore) AssignRun(ctx context.Context, runID, workerID string, assignmentTTL, heartbeatTTL time.Duration) (bool, error) {
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

	var jobID string
	err = tx.QueryRow(ctx, `
		SELECT job_id
		FROM job_runs
		WHERE id = $1 AND status = 'scheduled' AND scheduled_at <= now()
		FOR UPDATE SKIP LOCKED
	`, runID).Scan(&jobID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	var requiredCPU, requiredMemory, requiredGPU, requiredGPUMemory int64
	var requiredAccelerator string
	err = tx.QueryRow(ctx, `
		SELECT required_cpu_millis, required_memory_mb, required_gpu_count,
		       required_gpu_memory_mb, required_accelerator
		FROM jobs WHERE id = $1
	`, jobID).Scan(&requiredCPU, &requiredMemory, &requiredGPU,
		&requiredGPUMemory, &requiredAccelerator)
	if err != nil {
		return false, err
	}

	var reservedCPU, reservedMemory, reservedGPU, reservedGPUMemory int64
	err = tx.QueryRow(ctx, `
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

	tag, err := tx.Exec(ctx, `
		UPDATE job_runs
		SET status = 'assigned', assigned_worker_id = $1, assigned_at = now(),
		    assignment_expires_at = now() + ($2::bigint * INTERVAL '1 millisecond')
		WHERE id = $3 AND status = 'scheduled'
	`, workerID, assignmentTTLMillis, runID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *PostgresStore) LeaseNextRun(ctx context.Context, workerID string, leaseDuration time.Duration) (*model.JobRun, *model.Job, error) {
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

	var runID, jobID string
	// Assignments are already placed by the scheduler policy. Preserve the same
	// priority-plus-age ordering when one worker has multiple assignments.
	err = tx.QueryRow(ctx, `
		SELECT r.id, r.job_id
		FROM job_runs r
		WHERE r.status = 'assigned' AND r.assigned_worker_id = $1
		  AND r.assignment_expires_at > now() AND r.scheduled_at <= now()
		ORDER BY ((r.priority::bigint * 3600) - EXTRACT(EPOCH FROM r.scheduled_at)) DESC,
		         r.scheduled_at ASC, r.created_at ASC, r.id ASC
		LIMIT 1
		FOR UPDATE OF r SKIP LOCKED
	`, workerID).Scan(&runID, &jobID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	var requiredCPU, requiredMemory, requiredGPU, requiredGPUMemory int64
	var requiredAccelerator string
	if err := tx.QueryRow(ctx, `
		SELECT required_cpu_millis, required_memory_mb, required_gpu_count,
		       required_gpu_memory_mb, required_accelerator
		FROM jobs WHERE id = $1
	`, jobID).Scan(&requiredCPU, &requiredMemory, &requiredGPU,
		&requiredGPUMemory, &requiredAccelerator); err != nil {
		return nil, nil, err
	}

	var reservedCPU, reservedMemory, reservedGPU, reservedGPUMemory int64
	if err := tx.QueryRow(ctx, `
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
		if _, err := tx.Exec(ctx, `
			UPDATE job_runs
			SET status = 'queued', scheduled_at = now(), assigned_worker_id = NULL,
			    assigned_at = NULL, assignment_expires_at = NULL
			WHERE id = $1 AND status = 'assigned' AND assigned_worker_id = $2
		`, runID, workerID); err != nil {
			return nil, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE job_runs
		SET status = 'leased', leased_by = $1, leased_at = now(),
		    lease_expires_at = now() + ($2::bigint * INTERVAL '1 millisecond'), attempt = attempt + 1
		WHERE id = $3 AND status = 'assigned' AND assigned_worker_id = $1
	`, workerID, durationMilliseconds(leaseDuration), runID); err != nil {
		return nil, nil, err
	}
	run, err := fetchRun(ctx, tx, runID)
	if err != nil {
		return nil, nil, err
	}
	job, err := fetchJob(ctx, tx, jobID)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
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
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs SET status = 'running', started_at = now()
		WHERE id = $1 AND leased_by = $2 AND attempt = $3
		  AND status = 'leased' AND lease_expires_at > now()
	`, runID, workerID, attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) CompleteRun(ctx context.Context, runID, workerID string, attempt int16, result map[string]any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs
		SET status = 'succeeded', finished_at = now(), result = $1,
		    leased_by = NULL, leased_at = NULL, lease_expires_at = NULL
		WHERE id = $2 AND leased_by = $3 AND attempt = $4
		  AND status = 'running' AND lease_expires_at > now()
	`, raw, runID, workerID, attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) FailRun(ctx context.Context, runID, workerID string, attempt int16, errMsg string, requeue bool, backoff time.Duration) error {
	var tag pgconn.CommandTag
	var err error
	if requeue {
		tag, err = s.pool.Exec(ctx, `
			UPDATE job_runs
			SET status = 'queued', scheduled_at = now() + ($1::bigint * INTERVAL '1 millisecond'), error = $2,
			    leased_by = NULL, leased_at = NULL, lease_expires_at = NULL,
			    assigned_worker_id = NULL, assigned_at = NULL, assignment_expires_at = NULL
			WHERE id = $3 AND leased_by = $4 AND attempt = $5
			  AND status IN ('leased', 'running') AND lease_expires_at > now()
		`, durationMilliseconds(backoff), errMsg, runID, workerID, attempt)
	} else {
		tag, err = s.pool.Exec(ctx, `
			UPDATE job_runs
			SET status = 'failed', finished_at = now(), error = $1,
			    leased_by = NULL, leased_at = NULL, lease_expires_at = NULL
			WHERE id = $2 AND leased_by = $3 AND attempt = $4
			  AND status IN ('leased', 'running') AND lease_expires_at > now()
		`, errMsg, runID, workerID, attempt)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) MarkDead(ctx context.Context, runID string, reason string) error {
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

	payloadRaw, err := json.Marshal(map[string]any{"result": lockedRun.Result, "error": lockedRun.Error})
	if err != nil {
		return fmt.Errorf("marshal dead letter payload: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO dead_letters (job_run_id, reason, payload) VALUES ($1, $2, $3)
	`, runID, reason, payloadRaw); err != nil {
		return err
	}

	tag, err := tx.Exec(ctx, `UPDATE job_runs SET status = 'dead' WHERE id = $1 AND status = 'failed'`, runID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return tx.Commit(ctx)
}

func (s *PostgresStore) ReclaimExpiredLeases(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE job_runs
		SET status = 'queued', leased_by = NULL, leased_at = NULL, lease_expires_at = NULL,
		    assigned_worker_id = NULL, assigned_at = NULL, assignment_expires_at = NULL
		WHERE status IN ('leased', 'running') AND lease_expires_at < now()
		RETURNING id
	`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
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
		w.AvailableCPUMillis = maxInt64(0, int64(w.CPUCapacity)-w.CurrentCPUReserved)
		w.AvailableMemoryMB = maxInt64(0, int64(w.MemoryCapacityMB)-w.CurrentMemoryReserved)
		w.AvailableGPUCount = maxInt64(0, int64(w.GPUCount)-w.CurrentGPUReserved)
		w.AvailableGPUMemoryMB = maxInt64(0, int64(w.GPUCount)*int64(w.GPUMemoryMB)-w.CurrentGPUMemoryReserved)
		workers = append(workers, &w)
	}
	return workers, rows.Err()
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
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
