package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// TransactionalExecution performs database side effects with the transaction
// that records the execution result. It must not make remote calls while the
// transaction is open; pass the execution key to a remote service that supports
// idempotency instead.
type TransactionalExecution func(ctx context.Context, tx pgx.Tx) (map[string]any, error)

// IdempotencyStore applies a database-backed deduplication boundary to one
// logical run. The first successful call commits the callback's database
// writes and result together. Later calls return that stored result and do not
// invoke the callback. This cannot make external effects exactly once.
type IdempotencyStore interface {
	RunOnce(ctx context.Context, executionKey string, fn TransactionalExecution) (result map[string]any, executed bool, err error)
}

var _ IdempotencyStore = (*PostgresStore)(nil)

// RunOnce runs fn at most once for an execution key, committing its database
// side effects and result atomically. If a worker loses its lease after this
// transaction commits but before it records run completion, the next attempt
// receives the stored result and can finish the run without repeating writes.
func (s *PostgresStore) RunOnce(ctx context.Context, executionKey string, fn TransactionalExecution) (map[string]any, bool, error) {
	key, err := uuid.Parse(executionKey)
	if err != nil {
		return nil, false, fmt.Errorf("parse execution key: %w", err)
	}
	if fn == nil {
		return nil, false, fmt.Errorf("idempotent execution callback is nil")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, false, fmt.Errorf("begin idempotent execution: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var claimed bool
	err = tx.QueryRow(ctx, `
		INSERT INTO execution_idempotency (execution_key)
		VALUES ($1)
		ON CONFLICT (execution_key) DO NOTHING
		RETURNING true
	`, key).Scan(&claimed)
	if err != nil && err != pgx.ErrNoRows {
		return nil, false, fmt.Errorf("claim idempotent execution: %w", err)
	}
	if !claimed {
		var raw []byte
		if err := tx.QueryRow(ctx, `
			SELECT result FROM execution_idempotency WHERE execution_key = $1
		`, key).Scan(&raw); err != nil {
			return nil, false, fmt.Errorf("read prior execution result: %w", err)
		}
		var result map[string]any
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, false, fmt.Errorf("unmarshal prior execution result: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("commit idempotent replay: %w", err)
		}
		return result, false, nil
	}

	result, err := fn(ctx, tx)
	if err != nil {
		return nil, false, err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, false, fmt.Errorf("marshal idempotent execution result: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE execution_idempotency
		SET result = $1, completed_at = now()
		WHERE execution_key = $2
	`, raw, key)
	if err != nil {
		return nil, false, fmt.Errorf("store idempotent execution result: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, false, fmt.Errorf("store idempotent execution result: expected one ledger row, updated %d", tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit idempotent execution: %w", err)
	}
	return result, true, nil
}
