package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/google/uuid"
)

func TestDeadLetterRetryDiscardAndConcurrency(t *testing.T) {
	st, ctx := openResourceTestStore(t)

	retrySource, retryLetter := createDeadLetterForOpsTest(t, ctx, st, "retry")
	letters, err := st.ListDeadLetters(ctx, 10, 0)
	if err != nil || !containsDeadLetter(letters, retryLetter.ID) {
		t.Fatalf("new dead letter should be visible from the primary: letters=%+v error=%v", letters, err)
	}

	retried, err := st.RetryDeadLetter(ctx, retryLetter.ID)
	if err != nil {
		t.Fatalf("retry dead letter: %v", err)
	}
	if retried.ID != retrySource.ID || retried.Status != model.RunStatusQueued || retried.Attempt != 0 || retried.ExecutionKey != retrySource.ExecutionKey {
		t.Fatalf("retry result = %+v, want same run queued at attempt 0", retried)
	}
	if retried.LeasedBy != nil || retried.LeasedAt != nil || retried.LeaseExpiresAt != nil ||
		retried.AssignedWorkerID != nil || retried.AssignedAt != nil || retried.AssignmentExpiresAt != nil ||
		retried.StartedAt != nil || retried.FinishedAt != nil || retried.Result != nil || retried.Error != nil {
		t.Fatalf("retry retained stale run state: %+v", retried)
	}
	if letters, err = st.ListDeadLetters(ctx, 10, 0); err != nil || containsDeadLetter(letters, retryLetter.ID) {
		t.Fatalf("retried letter should disappear from the primary view: letters=%+v error=%v", letters, err)
	}
	if _, err := st.RetryDeadLetter(ctx, retryLetter.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("retrying a consumed letter should return ErrNotFound, got %v", err)
	}

	discardSource, discardLetter := createDeadLetterForOpsTest(t, ctx, st, "discard")
	if err := st.DeleteDeadLetter(ctx, discardLetter.ID); err != nil {
		t.Fatalf("discard dead letter: %v", err)
	}
	stillDead, err := st.GetRun(ctx, discardSource.ID)
	if err != nil || stillDead.Status != model.RunStatusDead {
		t.Fatalf("discard should leave the run dead: run=%+v error=%v", stillDead, err)
	}
	if err := st.DeleteDeadLetter(ctx, discardLetter.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("discarding a consumed letter should return ErrNotFound, got %v", err)
	}

	racedRun, racedLetter := createDeadLetterForOpsTest(t, ctx, st, "race")
	peer := openPeerResourceTestStore(t, ctx, st)
	type opResult struct {
		name string
		err  error
	}
	start := make(chan struct{})
	results := make(chan opResult, 2)
	go func() {
		<-start
		_, err := peer.RetryDeadLetter(ctx, racedLetter.ID)
		results <- opResult{name: "retry", err: err}
	}()
	go func() {
		<-start
		results <- opResult{name: "discard", err: st.DeleteDeadLetter(ctx, racedLetter.ID)}
	}()
	close(start)

	wins := 0
	winner := ""
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err == nil {
			wins++
			winner = result.name
		} else if !errors.Is(result.err, store.ErrNotFound) {
			t.Fatalf("concurrent %s operation failed unexpectedly: %v", result.name, result.err)
		}
	}
	if wins != 1 {
		t.Fatalf("concurrent retry/discard successes = %d, want exactly 1", wins)
	}
	finalRun, err := st.GetRun(ctx, racedRun.ID)
	if err != nil {
		t.Fatalf("read run after concurrent operation: %v", err)
	}
	wantStatus := model.RunStatusDead
	if winner == "retry" {
		wantStatus = model.RunStatusQueued
	}
	if finalRun.Status != wantStatus {
		t.Fatalf("run status after %s won = %q, want %q", winner, finalRun.Status, wantStatus)
	}
}

func createDeadLetterForOpsTest(t *testing.T, ctx context.Context, st *store.PostgresStore, prefix string) (*model.JobRun, *model.DeadLetter) {
	t.Helper()
	job := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "dead-letter-" + prefix + "-" + uuid.NewString(), MaxAttempts: 1, TimeoutSeconds: 30,
	})
	run := createResourceTestRun(t, ctx, st, job.ID, 1)
	workerID := "dead-letter-worker-" + uuid.NewString()
	registerTestWorker(t, ctx, st, model.Worker{ID: workerID, Hostname: "dead-letter-host", CPUCapacity: 1000, MemoryCapacityMB: 1024})
	if _, err := st.Pool().Exec(ctx, `
		UPDATE job_runs
		SET status = 'failed', attempt = 7, scheduled_at = now() - interval '1 hour',
		    leased_by = $2, leased_at = now() - interval '30 minutes', lease_expires_at = now() + interval '1 hour',
		    assigned_worker_id = $2, assigned_at = now() - interval '35 minutes', assignment_expires_at = now() + interval '1 hour',
		    started_at = now() - interval '30 minutes', finished_at = now() - interval '5 minutes',
		    result = '{"partial": true}'::jsonb, error = 'terminal failure'
		WHERE id = $1
	`, run.ID, workerID); err != nil {
		t.Fatalf("prepare failed run: %v", err)
	}
	if err := st.MarkDead(ctx, run.ID, fmt.Sprintf("%s test failure", prefix)); err != nil {
		t.Fatalf("mark run dead: %v", err)
	}
	deadLetters, err := st.ListDeadLetters(ctx, 10, 0)
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}
	for _, letter := range deadLetters {
		if letter.JobRunID == run.ID {
			return run, letter
		}
	}
	t.Fatalf("dead letter for run %s missing from list", run.ID)
	return nil, nil
}

func containsDeadLetter(letters []*model.DeadLetter, id string) bool {
	for _, letter := range letters {
		if letter.ID == id {
			return true
		}
	}
	return false
}
