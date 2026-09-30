package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/G1lollipop/atlas/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestExecutionKeySurvivesReclaimAndDeadLetterRetry(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	job, run := createIdempotentRun(t, ctx, st)
	if _, err := uuid.Parse(run.ExecutionKey); err != nil {
		t.Fatalf("CreateRun execution key %q is not a UUID: %v", run.ExecutionKey, err)
	}
	handler := worker.NewIdempotentRecordHandler(st)
	if _, err := handler(ctx, job, run); err != nil {
		t.Fatalf("first idempotent execution: %v", err)
	}

	persisted, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if persisted.ExecutionKey != run.ExecutionKey {
		t.Fatalf("stored execution key = %q, want %q", persisted.ExecutionKey, run.ExecutionKey)
	}

	// Simulate a worker that died after leasing. Reclaiming changes lifecycle
	// state but must not create a new logical execution identity.
	if _, err := st.Pool().Exec(ctx, `
		UPDATE job_runs SET status = 'running', attempt = 1, leased_by = 'idempotency-test',
			lease_expires_at = now() - INTERVAL '1 second' WHERE id = $1
	`, run.ID); err != nil {
		t.Fatalf("prepare expired run: %v", err)
	}
	if count, err := st.ReclaimExpiredLeases(ctx); err != nil || count != 1 {
		t.Fatalf("ReclaimExpiredLeases = (%d, %v), want (1, nil)", count, err)
	}
	afterReclaim, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun after reclaim: %v", err)
	}
	if afterReclaim.ExecutionKey != run.ExecutionKey {
		t.Fatalf("execution key after reclaim = %q, want original %q", afterReclaim.ExecutionKey, run.ExecutionKey)
	}

	if _, err := st.Pool().Exec(ctx, `UPDATE job_runs SET status = 'failed' WHERE id = $1`, run.ID); err != nil {
		t.Fatalf("prepare failed run: %v", err)
	}
	if err := st.MarkDead(ctx, run.ID, "test dead letter"); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}
	letters, err := st.ListDeadLetters(ctx, 10, 0)
	if err != nil || len(letters) != 1 || letters[0].JobRunID != run.ID {
		t.Fatalf("ListDeadLetters = (%+v, %v), want one entry for run %s", letters, err, run.ID)
	}
	// The real manual retry path requeues the same row, retaining its key.
	retried, err := st.RetryDeadLetter(ctx, letters[0].ID)
	if err != nil {
		t.Fatalf("RetryDeadLetter: %v", err)
	}
	if retried == nil || retried.ID != run.ID || retried.ExecutionKey != run.ExecutionKey {
		t.Fatalf("RetryDeadLetter returned run = %+v, want original run ID %s and execution key %s", retried, run.ID, run.ExecutionKey)
	}

	retried, err = st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun after retry: %v", err)
	}
	if retried.ExecutionKey != run.ExecutionKey {
		t.Fatalf("execution key after reclaim/DLQ retry = %q, want original %q", retried.ExecutionKey, run.ExecutionKey)
	}
	if retried.JobID != job.ID {
		t.Fatalf("job id after retry = %q, want %q", retried.JobID, job.ID)
	}
	if _, err := handler(ctx, job, retried); err != nil {
		t.Fatalf("idempotent execution after manual retry: %v", err)
	}
	var effectCount int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM idempotent_handler_effects WHERE execution_key = $1`, run.ExecutionKey).Scan(&effectCount); err != nil {
		t.Fatalf("count effect after manual retry: %v", err)
	}
	if effectCount != 1 {
		t.Fatalf("effects after manual retry = %d, want 1", effectCount)
	}
}

func TestIdempotentRecordHandlerDeduplicatesConcurrentAttempts(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	job, run := createIdempotentRun(t, ctx, st)
	handler := worker.NewIdempotentRecordHandler(st)

	const callers = 12
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := handler(ctx, job, run)
			if err == nil && result["recorded"] != true {
				err = fmt.Errorf("handler result = %v, want recorded=true", result)
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent handler call: %v", err)
		}
	}

	var effectCount, ledgerCount int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM idempotent_handler_effects WHERE execution_key = $1`, run.ExecutionKey).Scan(&effectCount); err != nil {
		t.Fatalf("count handler effects: %v", err)
	}
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM execution_idempotency WHERE execution_key = $1`, run.ExecutionKey).Scan(&ledgerCount); err != nil {
		t.Fatalf("count idempotency ledger: %v", err)
	}
	if effectCount != 1 || ledgerCount != 1 {
		t.Fatalf("effect/ledger counts = %d/%d, want 1/1", effectCount, ledgerCount)
	}

	callbackCalled := false
	result, executed, err := st.RunOnce(ctx, run.ExecutionKey, func(context.Context, pgx.Tx) (map[string]any, error) {
		callbackCalled = true
		return map[string]any{"unexpected": true}, nil
	})
	if err != nil {
		t.Fatalf("RunOnce replay: %v", err)
	}
	if executed || callbackCalled {
		t.Fatalf("RunOnce replay executed=%v callback_called=%v, want both false", executed, callbackCalled)
	}
	if result["recorded"] != true || result["execution_key"] != run.ExecutionKey {
		t.Fatalf("replayed result = %v, want original handler result", result)
	}
}

func TestIdempotentExecutionRollsBackEffectsOnCallbackError(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	_, run := createIdempotentRun(t, ctx, st)
	wantErr := errors.New("reject transaction")

	_, executed, err := st.RunOnce(ctx, run.ExecutionKey, func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
		if _, err := tx.Exec(ctx, `
			INSERT INTO idempotent_handler_effects (execution_key, payload) VALUES ($1, '{}'::jsonb)
		`, run.ExecutionKey); err != nil {
			return nil, err
		}
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) || executed {
		t.Fatalf("failed RunOnce = (executed=%v, err=%v), want (false, callback error)", executed, err)
	}

	var count int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM idempotent_handler_effects WHERE execution_key = $1`, run.ExecutionKey).Scan(&count); err != nil {
		t.Fatalf("count rolled-back effects: %v", err)
	}
	if count != 0 {
		t.Fatalf("effects after callback error = %d, want 0", count)
	}

	_, executed, err = st.RunOnce(ctx, run.ExecutionKey, func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
		if _, err := tx.Exec(ctx, `
			INSERT INTO idempotent_handler_effects (execution_key, payload) VALUES ($1, '{}'::jsonb)
		`, run.ExecutionKey); err != nil {
			return nil, err
		}
		return map[string]any{"ok": true}, nil
	})
	if err != nil || !executed {
		t.Fatalf("RunOnce retry = (executed=%v, err=%v), want (true, nil)", executed, err)
	}
}

func createIdempotentRun(t *testing.T, ctx context.Context, st *store.PostgresStore) (*model.Job, *model.JobRun) {
	t.Helper()
	job, err := st.CreateJob(ctx, model.NewJobInput{
		Name:           "idempotent_record",
		Payload:        map[string]any{"value": "persist once"},
		MaxAttempts:    3,
		TimeoutSeconds: 10,
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	run, err := st.CreateRun(ctx, job.ID, job.Priority, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return job, run
}
