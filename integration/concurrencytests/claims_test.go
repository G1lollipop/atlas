package concurrencytests

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/google/uuid"
)

const concurrentWorkers = 32

func TestConcurrentAssignmentLeaseAndFencedCompletion(t *testing.T) {
	databaseURL := os.Getenv("ATLAS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set ATLAS_TEST_DATABASE_URL to run the PostgreSQL concurrency integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	admin, err := store.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin store: %v", err)
	}
	schema := "atlas_claims_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Pool().Exec(ctx, "CREATE SCHEMA \""+schema+"\""); err != nil {
		admin.Close()
		t.Fatalf("create isolated schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Pool().Exec(cleanupCtx, "DROP SCHEMA IF EXISTS \""+schema+"\" CASCADE"); err != nil {
			t.Errorf("drop isolated schema: %v", err)
		}
		admin.Close()
	})

	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	query.Set("pool_max_conns", "40")
	parsed.RawQuery = query.Encode()
	st, err := store.New(ctx, parsed.String())
	if err != nil {
		t.Fatalf("connect isolated store: %v", err)
	}
	t.Cleanup(st.Close)
	var selectedSchema string
	if err := st.Pool().QueryRow(ctx, `SELECT current_schema()`).Scan(&selectedSchema); err != nil {
		t.Fatalf("read isolated current schema: %v", err)
	}
	if selectedSchema != schema {
		t.Fatalf("isolated connection current_schema() = %q, want %q", selectedSchema, schema)
	}
	if err := store.RunMigrations(ctx, st.Pool(), "../../migrations"); err != nil {
		t.Fatalf("run isolated migrations: %v", err)
	}

	workerIDs := make([]string, concurrentWorkers)
	for i := range workerIDs {
		workerIDs[i] = fmt.Sprintf("claim-worker-%02d", i)
		if err := st.UpsertWorkerHeartbeat(ctx, model.Worker{
			ID: workerIDs[i], Hostname: "claim-test", Status: model.WorkerStatusAlive,
			CPUCapacity: 1000, MemoryCapacityMB: 1024,
		}); err != nil {
			t.Fatalf("register worker %s: %v", workerIDs[i], err)
		}
	}

	job, err := st.CreateJob(ctx, model.NewJobInput{
		Name: "concurrent-claim-test", Payload: map[string]any{},
		RequiredCPUMillis: 1000, RequiredMemoryMB: 128,
		MaxAttempts: 2, TimeoutSeconds: 30,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	run, err := st.CreateRun(ctx, job.ID, job.Priority, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("create queued run: %v", err)
	}
	if n, err := st.ScheduleDueRuns(ctx); err != nil || n != 1 {
		t.Fatalf("ScheduleDueRuns() = (%d, %v), want (1, nil)", n, err)
	}

	assignedTo := assignConcurrently(t, ctx, st, workerIDs, run.ID)
	if len(assignedTo) != 1 {
		t.Fatalf("concurrent assignments = %d, want exactly one: %v", len(assignedTo), assignedTo)
	}

	var wg sync.WaitGroup
	results := make(chan leaseResult, len(workerIDs))
	start := make(chan struct{})
	for _, id := range workerIDs {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			leased, _, err := st.LeaseNextRun(ctx, id, 5*time.Second)
			if err == nil && leased != nil {
				results <- leaseResult{worker: id, runID: leased.ID, attempt: leased.Attempt}
				return
			}
			results <- leaseResult{worker: id, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var concurrentLeases []leaseResult
	for result := range results {
		if result.err != nil {
			t.Errorf("LeaseNextRun(%s): %v", result.worker, result.err)
		}
		if result.runID != "" {
			concurrentLeases = append(concurrentLeases, result)
		}
	}
	if len(concurrentLeases) != 1 {
		t.Fatalf("concurrent lease claims = %d, want exactly one: %+v", len(concurrentLeases), concurrentLeases)
	}
	if concurrentLeases[0].worker != assignedTo[0] || concurrentLeases[0].runID != run.ID || concurrentLeases[0].attempt != 1 {
		t.Fatalf("winning lease = %+v, want worker %s run %s attempt 1", concurrentLeases[0], assignedTo[0], run.ID)
	}
	firstRun := &model.JobRun{ID: concurrentLeases[0].runID, Attempt: concurrentLeases[0].attempt}

	if err := st.MarkRunning(ctx, run.ID, assignedTo[0], firstRun.Attempt); err != nil {
		t.Fatalf("mark first attempt running: %v", err)
	}
	if _, err := st.Pool().Exec(ctx, `UPDATE job_runs SET lease_expires_at = now() - INTERVAL '1 second' WHERE id = $1`, run.ID); err != nil {
		t.Fatalf("expire first attempt lease: %v", err)
	}
	if n, err := st.ReclaimExpiredLeases(ctx); err != nil || n != 1 {
		t.Fatalf("ReclaimExpiredLeases() = (%d, %v), want (1, nil)", n, err)
	}
	if n, err := st.ScheduleDueRuns(ctx); err != nil || n != 1 {
		t.Fatalf("schedule reclaimed run = (%d, %v), want (1, nil)", n, err)
	}
	assigned, err := st.AssignRun(ctx, run.ID, assignedTo[0], 5*time.Second, time.Minute)
	if err != nil || !assigned {
		t.Fatalf("reassign reclaimed run = (%t, %v), want (true, nil)", assigned, err)
	}
	secondRun, _, err := st.LeaseNextRun(ctx, assignedTo[0], 5*time.Second)
	if err != nil || secondRun == nil || secondRun.Attempt != 2 {
		t.Fatalf("second lease = (%+v, %v), want attempt 2", secondRun, err)
	}
	if err := st.MarkRunning(ctx, run.ID, assignedTo[0], secondRun.Attempt); err != nil {
		t.Fatalf("mark second attempt running: %v", err)
	}

	if err := st.CompleteRun(ctx, run.ID, assignedTo[0], firstRun.Attempt, map[string]any{"attempt": 1}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stale attempt 1 completion error = %v, want ErrNotFound", err)
	}
	if err := st.CompleteRun(ctx, run.ID, assignedTo[0], secondRun.Attempt, map[string]any{"attempt": 2}); err != nil {
		t.Fatalf("complete current attempt: %v", err)
	}
	persisted, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("read completed run: %v", err)
	}
	if persisted.Status != model.RunStatusSucceeded || persisted.Attempt != 2 {
		t.Fatalf("final run status/attempt = %s/%d, want succeeded/2", persisted.Status, persisted.Attempt)
	}
}

type leaseResult struct {
	worker  string
	runID   string
	attempt int16
	err     error
}

func assignConcurrently(t *testing.T, ctx context.Context, st *store.PostgresStore, workerIDs []string, runID string) []string {
	t.Helper()
	type result struct {
		worker   string
		assigned bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, len(workerIDs))
	var wg sync.WaitGroup
	for _, id := range workerIDs {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			assigned, err := st.AssignRun(ctx, runID, id, 5*time.Second, time.Minute)
			results <- result{worker: id, assigned: assigned, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var assigned []string
	for result := range results {
		if result.err != nil {
			t.Errorf("AssignRun(%s): %v", result.worker, result.err)
		}
		if result.assigned {
			assigned = append(assigned, result.worker)
		}
	}
	return assigned
}
