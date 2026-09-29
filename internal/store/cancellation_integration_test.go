package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/G1lollipop/atlas/internal/model"
)

func cancellationIntegrationStore(t *testing.T) *PostgresStore {
	t.Helper()
	databaseURL := os.Getenv("ATLAS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set ATLAS_TEST_DATABASE_URL to run Postgres cancellation integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	adminPool, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	schema := "atlas_cancel_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		adminPool.Close()
		t.Fatalf("create isolated test schema: %v", err)
	}

	testConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse isolated test database URL: %v", err)
	}
	if testConfig.ConnConfig.RuntimeParams == nil {
		testConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	testConfig.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	testPool, err := pgxpool.NewWithConfig(ctx, testConfig)
	if err != nil {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
		t.Fatalf("open isolated test schema: %v", err)
	}
	if err := RunMigrations(ctx, testPool, filepath.Join("..", "..", "migrations")); err != nil {
		testPool.Close()
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
		t.Fatalf("run migrations in isolated schema: %v", err)
	}

	store := &PostgresStore{pool: testPool}
	t.Cleanup(func() {
		store.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = adminPool.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
	})
	return store
}

func createRunningCancellationFixture(t *testing.T, st *PostgresStore, cronExpr *string) (*model.Job, *model.JobRun) {
	t.Helper()
	ctx := t.Context()
	job, err := st.CreateJob(ctx, model.NewJobInput{
		Name:           "cancel-fixture-" + uuid.NewString(),
		CronExpr:       cronExpr,
		MaxAttempts:    3,
		TimeoutSeconds: 60,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	run, err := st.CreateRun(ctx, job.ID, job.Priority, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE job_runs SET status = 'running', attempt = 1, leased_by = 'worker-1',
		    leased_at = now(), lease_expires_at = now() + INTERVAL '5 minutes'
		WHERE id = $1
	`, run.ID); err != nil {
		t.Fatalf("seed running lease: %v", err)
	}
	return job, run
}

func TestCancelJobFencesCompletionAndFuturePromotion(t *testing.T) {
	st := cancellationIntegrationStore(t)
	job, run := createRunningCancellationFixture(t, st, nil)

	if err := st.CancelJob(t.Context(), job.ID); err != nil {
		t.Fatalf("cancel job: %v", err)
	}
	if err := st.CancelJob(t.Context(), job.ID); err != nil {
		t.Fatalf("repeat cancel: %v", err)
	}
	if err := st.CompleteRun(t.Context(), run.ID, "worker-1", 1, map[string]any{"value": 1}); !errors.Is(err, ErrCanceled) {
		t.Fatalf("completion after cancellation = %v, want ErrCanceled", err)
	}

	gotRun, err := st.GetRun(t.Context(), run.ID)
	if err != nil {
		t.Fatalf("get canceled run: %v", err)
	}
	if gotRun.Status != model.RunStatusCanceled || gotRun.CancelRequestedAt == nil {
		t.Fatalf("run status=%q cancel_requested_at=%v, want canceled with request timestamp", gotRun.Status, gotRun.CancelRequestedAt)
	}
	gotJob, err := st.GetJob(t.Context(), job.ID)
	if err != nil {
		t.Fatalf("get canceled job: %v", err)
	}
	if gotJob.Status != model.JobStatusCanceled {
		t.Fatalf("job status=%q, want canceled", gotJob.Status)
	}
	if _, err := st.CreateRun(t.Context(), job.ID, job.Priority, time.Now()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("promoter create after cancellation = %v, want ErrNotFound", err)
	}
}

func TestCancelJobCompletionRaceHasOneFencedOutcome(t *testing.T) {
	st := cancellationIntegrationStore(t)
	for i := 0; i < 8; i++ {
		job, run := createRunningCancellationFixture(t, st, nil)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var cancelErr, completeErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			cancelErr = st.CancelJob(t.Context(), job.ID)
		}()
		go func() {
			defer wg.Done()
			<-start
			completeErr = st.CompleteRun(t.Context(), run.ID, "worker-1", 1, map[string]any{"race": i})
		}()
		close(start)
		wg.Wait()
		if cancelErr != nil {
			t.Fatalf("race %d cancel: %v", i, cancelErr)
		}

		gotRun, err := st.GetRun(t.Context(), run.ID)
		if err != nil {
			t.Fatalf("race %d get run: %v", i, err)
		}
		switch gotRun.Status {
		case model.RunStatusCanceled:
			if !errors.Is(completeErr, ErrCanceled) || gotRun.CancelRequestedAt == nil {
				t.Fatalf("race %d canceled outcome: complete=%v cancel_requested_at=%v", i, completeErr, gotRun.CancelRequestedAt)
			}
		case model.RunStatusSucceeded:
			if completeErr != nil || gotRun.CancelRequestedAt != nil {
				t.Fatalf("race %d completed outcome: complete=%v cancel_requested_at=%v", i, completeErr, gotRun.CancelRequestedAt)
			}
		default:
			t.Fatalf("race %d run status=%q, want succeeded or canceled", i, gotRun.Status)
		}
	}
}

func TestCancelRecurringJobBlocksStalePromotion(t *testing.T) {
	st := cancellationIntegrationStore(t)
	cronExpr := "*/5 * * * *"
	job, _ := createRunningCancellationFixture(t, st, &cronExpr)
	if err := st.CancelJob(t.Context(), job.ID); err != nil {
		t.Fatalf("cancel recurring job: %v", err)
	}
	if _, err := st.CreateRun(t.Context(), job.ID, job.Priority, time.Now()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale recurring promotion = %v, want ErrNotFound", err)
	}
}
