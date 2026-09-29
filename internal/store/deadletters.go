package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/G1lollipop/atlas/internal/model"
)

// ListDeadLetters reads from the primary pool so operators see new letters and
// completed retry/discard operations immediately, even when a read replica is enabled.
func (s *PostgresStore) ListDeadLetters(ctx context.Context, limit, offset int) ([]*model.DeadLetter, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_run_id, reason, payload, created_at
		FROM dead_letters
		ORDER BY created_at DESC, id DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	letters := make([]*model.DeadLetter, 0)
	for rows.Next() {
		var letter model.DeadLetter
		var payloadRaw []byte
		if err := rows.Scan(&letter.ID, &letter.JobRunID, &letter.Reason, &payloadRaw, &letter.CreatedAt); err != nil {
			return nil, err
		}
		if len(payloadRaw) != 0 {
			if err := json.Unmarshal(payloadRaw, &letter.Payload); err != nil {
				return nil, fmt.Errorf("unmarshal dead letter payload: %w", err)
			}
		}
		letters = append(letters, &letter)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return letters, nil
}

// RetryDeadLetter uses the primary and holds a row lock on the dead-letter entry while
// changing both records. A competing retry or discard waits for this transaction and
// then observes that the entry has already been removed.
func (s *PostgresStore) RetryDeadLetter(ctx context.Context, id string) (*model.JobRun, error) {
	parsedID, err := uuid.Parse(id)
	if err != nil {
		return nil, ErrNotFound
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var runID string
	err = tx.QueryRow(ctx, `SELECT job_run_id FROM dead_letters WHERE id = $1 FOR UPDATE`, parsedID).Scan(&runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	var jobStatus model.JobStatus
	var queue, tenant string
	err = tx.QueryRow(ctx, `
		SELECT j.status, j.queue, j.tenant_id
		FROM jobs j JOIN job_runs r ON r.job_id = j.id
		WHERE r.id = $1
		FOR UPDATE OF j
	`, runID).Scan(&jobStatus, &queue, &tenant)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if jobStatus == model.JobStatusCanceled {
		return nil, ErrJobCanceled
	}
	if err := admitQueueWork(ctx, tx, s.queueLimits, queue, tenant, false); err != nil {
		return nil, err
	}

	// Fields outside this SET list, including execution_key, retain their stored value.
	run, err := scanRun(tx.QueryRow(ctx, `
		UPDATE job_runs
		SET status = 'queued', attempt = 0, scheduled_at = now(),
	    leased_by = NULL, leased_at = NULL, lease_expires_at = NULL,
	    assigned_worker_id = NULL, assigned_at = NULL, assignment_expires_at = NULL,
	    started_at = NULL, finished_at = NULL, result = NULL, error = NULL,
	    cancel_requested_at = NULL
		WHERE id = $1 AND status = 'dead'
		RETURNING `+runColumns, runID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	tag, err := tx.Exec(ctx, `DELETE FROM dead_letters WHERE id = $1`, parsedID)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return run, nil
}

// DeleteDeadLetter removes only the letter. Its row deletion is atomic, so a retry
// racing with this operation can consume the same entry only if it wins the row lock.
func (s *PostgresStore) DeleteDeadLetter(ctx context.Context, id string) error {
	parsedID, err := uuid.Parse(id)
	if err != nil {
		return ErrNotFound
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM dead_letters WHERE id = $1`, parsedID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
