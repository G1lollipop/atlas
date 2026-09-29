package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/G1lollipop/atlas/internal/model"
)

// CancelJob records cancellation durably before returning. Unstarted work releases
// its scheduler assignment immediately; leased attempts retain ownership until the
// worker acknowledges cancellation or the lease expires and the janitor reclaims it.
func (s *PostgresStore) CancelJob(ctx context.Context, id string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status model.JobStatus
	if err := tx.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1 FOR UPDATE`, id).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}

	if status != model.JobStatusCanceled {
		if _, err := tx.Exec(ctx, `UPDATE jobs SET status = 'canceled', updated_at = now() WHERE id = $1`, id); err != nil {
			return err
		}
	}

	// A failed row is an intermediate state between FailRun and MarkDead, so it is
	// included here to ensure that a racing cancellation cannot be overwritten by
	// the worker's follow-up dead-letter transition.
	if _, err := tx.Exec(ctx, `
		UPDATE job_runs
		SET status = 'canceled', cancel_requested_at = COALESCE(cancel_requested_at, now()),
		    finished_at = COALESCE(finished_at, now()),
		    leased_by = NULL, leased_at = NULL, lease_expires_at = NULL,
		    assigned_worker_id = NULL, assigned_at = NULL, assignment_expires_at = NULL
		WHERE job_id = $1 AND status IN ('queued', 'scheduled', 'assigned', 'failed')
	`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE job_runs
		SET cancel_requested_at = COALESCE(cancel_requested_at, now())
		WHERE job_id = $1 AND status IN ('leased', 'running')
	`, id); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// CancellationRequested checks only the current lease owner and attempt. A stale
// worker therefore cannot act on a later attempt's cancellation request.
func (s *PostgresStore) CancellationRequested(ctx context.Context, runID, workerID string, attempt int16) (bool, error) {
	var requested bool
	err := s.pool.QueryRow(ctx, `
		SELECT cancel_requested_at IS NOT NULL
		FROM job_runs
		WHERE id = $1 AND leased_by = $2 AND attempt = $3
		  AND status IN ('leased', 'running')
	`, runID, workerID, attempt).Scan(&requested)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, err
	}
	return requested, nil
}

// MarkCanceled acknowledges a request from the current worker attempt. If that
// worker has lost its lease, the janitor remains responsible for finalizing the run.
func (s *PostgresStore) MarkCanceled(ctx context.Context, runID, workerID string, attempt int16) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs
		SET status = 'canceled', finished_at = COALESCE(finished_at, now()),
		    leased_by = NULL, leased_at = NULL, lease_expires_at = NULL,
		    assigned_worker_id = NULL, assigned_at = NULL, assignment_expires_at = NULL
		WHERE id = $1 AND leased_by = $2 AND attempt = $3
		  AND status IN ('leased', 'running') AND cancel_requested_at IS NOT NULL
	`, runID, workerID, attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
