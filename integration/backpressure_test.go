package integration

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/api"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
)

func TestConcurrentQueueSubmissionsRespectLimitsAcrossStoreReplicas(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	peer := openPeerResourceTestStore(t, ctx, st)
	limits := store.QueueLimits{MaxQueueDepth: 5, PerQueueLimit: 5, PerTenantLimit: 5}
	if err := st.SetQueueLimits(limits); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetQueueLimits(limits); err != nil {
		t.Fatal(err)
	}

	const submissions = 24
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	unexpected := make([]error, 0)
	for i := 0; i < submissions; i++ {
		current := st
		if i%2 == 1 {
			current = peer
		}
		wg.Add(1)
		go func(index int, current *store.PostgresStore) {
			defer wg.Done()
			_, err := current.CreateJob(ctx, model.NewJobInput{
				Name:  fmt.Sprintf("queue-concurrent-%d", index),
				Queue: "cpu-default", TenantID: "tenant-concurrent",
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				accepted++
			} else if !errors.Is(err, store.ErrQueueCapacityExceeded) {
				unexpected = append(unexpected, err)
			}
		}(i, current)
	}
	wg.Wait()
	if len(unexpected) > 0 {
		t.Fatalf("unexpected CreateJob errors: %v", unexpected)
	}
	if accepted != limits.MaxQueueDepth {
		t.Fatalf("accepted submissions = %d, want exactly %d", accepted, limits.MaxQueueDepth)
	}
	var outstanding int
	if err := st.Pool().QueryRow(ctx, `
		SELECT count(*) FROM jobs j
		WHERE j.queue = 'cpu-default' AND j.tenant_id = 'tenant-concurrent'
		  AND j.cron_expr IS NULL AND j.status IN ('active', 'paused')
		  AND NOT EXISTS (SELECT 1 FROM job_runs r WHERE r.job_id = j.id)
	`).Scan(&outstanding); err != nil {
		t.Fatal(err)
	}
	if outstanding != limits.MaxQueueDepth {
		t.Fatalf("unpromoted accepted jobs = %d, want %d", outstanding, limits.MaxQueueDepth)
	}
}

func TestQueueAndTenantLimitsAreIndependent(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	limits := store.QueueLimits{MaxQueueDepth: 20, PerQueueLimit: 2, PerTenantLimit: 2}
	if err := st.SetQueueLimits(limits); err != nil {
		t.Fatal(err)
	}

	create := func(name, queue, tenant string) error {
		_, err := st.CreateJob(ctx, model.NewJobInput{Name: name, Queue: queue, TenantID: tenant})
		return err
	}
	for _, item := range []struct {
		name      string
		queue     string
		tenant    string
		wantError bool
	}{
		{name: "queue-a tenant-a", queue: "queue-a", tenant: "tenant-a"},
		{name: "queue-b tenant-a", queue: "queue-b", tenant: "tenant-a"},
		{name: "tenant-a is capped across queues", queue: "queue-c", tenant: "tenant-a", wantError: true},
		{name: "queue-a accepts tenant-b", queue: "queue-a", tenant: "tenant-b"},
		{name: "queue-a is capped", queue: "queue-a", tenant: "tenant-c", wantError: true},
		{name: "queue-b accepts tenant-b", queue: "queue-b", tenant: "tenant-b"},
		{name: "tenant-b is capped across queues", queue: "queue-c", tenant: "tenant-b", wantError: true},
	} {
		t.Run(item.name, func(t *testing.T) {
			err := create(item.name+"-"+fmt.Sprint(time.Now().UnixNano()), item.queue, item.tenant)
			if item.wantError && !errors.Is(err, store.ErrQueueCapacityExceeded) {
				t.Fatalf("CreateJob error = %v, want capacity exceeded", err)
			}
			if !item.wantError && err != nil {
				t.Fatalf("CreateJob rejected allowed combination: %v", err)
			}
		})
	}
}

func TestQueueBackpressureAPIUsesJWTSubjectAndReturns429(t *testing.T) {
	st, _ := openResourceTestStore(t)
	if err := st.SetQueueLimits(store.QueueLimits{MaxQueueDepth: 20, PerQueueLimit: 1, PerTenantLimit: 1}); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := httptest.NewServer(api.NewRouter(st, log, testSecret, 10000, 10000))
	defer server.Close()

	tenantA, err := api.MintToken(testSecret, "tenant-a", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := api.MintToken(testSecret, "tenant-b", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	status, first := postBackpressureJob(t, server.URL, tenantA, map[string]any{
		"name": "tenant-a-first", "queue": "queue-a", "tenant_id": "forged-tenant",
	})
	if status != http.StatusCreated {
		t.Fatalf("first submission status = %d, want 201", status)
	}
	if first.Queue != "queue-a" || first.TenantID != "tenant-a" {
		t.Fatalf("persisted queue/tenant = %q/%q, want queue-a/tenant-a", first.Queue, first.TenantID)
	}
	if status, _ := postBackpressureJob(t, server.URL, tenantA, map[string]any{"name": "tenant-a-second", "queue": "queue-b"}); status != http.StatusTooManyRequests {
		t.Fatalf("second tenant-a submission status = %d, want 429", status)
	}
	if status, _ := postBackpressureJob(t, server.URL, tenantB, map[string]any{"name": "tenant-b-queue-a", "queue": "queue-a"}); status != http.StatusTooManyRequests {
		t.Fatalf("second queue-a submission status = %d, want 429", status)
	}
	if status, job := postBackpressureJob(t, server.URL, tenantB, map[string]any{"name": "tenant-b-queue-b", "queue": "queue-b"}); status != http.StatusCreated || job.TenantID != "tenant-b" {
		t.Fatalf("independent tenant/queue admission status/job = %d/%+v, want 201 for tenant-b", status, job)
	}
}

func TestCreateRunPromotesReservationAndCancellationOrCompletionReleasesIt(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	limits := store.QueueLimits{MaxQueueDepth: 1, PerQueueLimit: 1, PerTenantLimit: 1}
	if err := st.SetQueueLimits(limits); err != nil {
		t.Fatal(err)
	}
	input := model.NewJobInput{Name: "cancel-release", Queue: "release-q", TenantID: "release-tenant"}
	job, err := st.CreateJob(ctx, input)
	if err != nil {
		t.Fatalf("create first reservation: %v", err)
	}
	run, err := st.CreateRun(ctx, job.ID, job.Priority, time.Now())
	if err != nil {
		t.Fatalf("promote reserved one-shot: %v", err)
	}
	if _, err := st.CreateJob(ctx, model.NewJobInput{Name: "must-backpressure", Queue: "release-q", TenantID: "release-tenant"}); !errors.Is(err, store.ErrQueueCapacityExceeded) {
		t.Fatalf("submission after promotion = %v, want capacity exceeded", err)
	}
	if err := st.CancelJob(ctx, job.ID); err != nil {
		t.Fatalf("cancel queued one-shot: %v", err)
	}

	completedJob, err := st.CreateJob(ctx, model.NewJobInput{Name: "complete-release", Queue: "release-q", TenantID: "release-tenant"})
	if err != nil {
		t.Fatalf("capacity was not released by cancellation: %v", err)
	}
	completedRun, err := st.CreateRun(ctx, completedJob.ID, completedJob.Priority, time.Now())
	if err != nil {
		t.Fatalf("promote second reserved one-shot: %v", err)
	}
	if _, err := st.Pool().Exec(ctx, `
		UPDATE job_runs SET status = 'running', attempt = 1, leased_by = 'backpressure-test-worker',
		    leased_at = now(), lease_expires_at = now() + INTERVAL '1 minute', started_at = now()
		WHERE id = $1
	`, completedRun.ID); err != nil {
		t.Fatalf("seed active lease for completion: %v", err)
	}
	if err := st.CompleteRun(ctx, completedRun.ID, "backpressure-test-worker", 1, map[string]any{"ok": true}); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	if _, err := st.CreateJob(ctx, model.NewJobInput{Name: "after-completion", Queue: "release-q", TenantID: "release-tenant"}); err != nil {
		t.Fatalf("capacity was not released by completion: %v", err)
	}
	if run.Status != model.RunStatusQueued {
		t.Fatalf("promoted run status = %q, want queued", run.Status)
	}
}

func TestRecurringPromotionEnforcesQueueCapacity(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	if err := st.SetQueueLimits(store.QueueLimits{MaxQueueDepth: 1, PerQueueLimit: 1, PerTenantLimit: 1}); err != nil {
		t.Fatal(err)
	}
	blocker, err := st.CreateJob(ctx, model.NewJobInput{Name: "recurring-blocker", Queue: "recurring-q", TenantID: "recurring-tenant"})
	if err != nil {
		t.Fatalf("create blocker: %v", err)
	}
	cronExpr := "*/5 * * * *"
	recurring, err := st.CreateJob(ctx, model.NewJobInput{
		Name: "recurring-template", CronExpr: &cronExpr, Queue: "recurring-q", TenantID: "recurring-tenant",
	})
	if err != nil {
		t.Fatalf("create recurring template: %v", err)
	}
	if _, err := st.CreateRun(ctx, recurring.ID, recurring.Priority, time.Now()); !errors.Is(err, store.ErrQueueCapacityExceeded) {
		t.Fatalf("recurring promotion at capacity = %v, want capacity exceeded", err)
	}
	if err := st.CancelJob(ctx, blocker.ID); err != nil {
		t.Fatalf("cancel blocker: %v", err)
	}
	if _, err := st.CreateRun(ctx, recurring.ID, recurring.Priority, time.Now()); err != nil {
		t.Fatalf("recurring promotion after capacity release: %v", err)
	}
}

func TestPauseDoesNotFreeUnpromotedReservation(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	if err := st.SetQueueLimits(store.QueueLimits{MaxQueueDepth: 1, PerQueueLimit: 1, PerTenantLimit: 1}); err != nil {
		t.Fatal(err)
	}
	job, err := st.CreateJob(ctx, model.NewJobInput{Name: "paused-reservation", Queue: "pause-q", TenantID: "pause-tenant"})
	if err != nil {
		t.Fatalf("create reserved job: %v", err)
	}
	if err := st.UpdateJobStatus(ctx, job.ID, model.JobStatusPaused); err != nil {
		t.Fatalf("pause reserved job: %v", err)
	}
	if _, err := st.CreateJob(ctx, model.NewJobInput{Name: "pause-capacity-check", Queue: "pause-q", TenantID: "pause-tenant"}); !errors.Is(err, store.ErrQueueCapacityExceeded) {
		t.Fatalf("submission while first job is paused = %v, want capacity exceeded", err)
	}
	if err := st.CancelJob(ctx, job.ID); err != nil {
		t.Fatalf("cancel paused reservation: %v", err)
	}
	if _, err := st.CreateJob(ctx, model.NewJobInput{Name: "after-paused-cancel", Queue: "pause-q", TenantID: "pause-tenant"}); err != nil {
		t.Fatalf("capacity was not released by canceling paused job: %v", err)
	}
}

func TestArchivedOneShotCannotResumeAfterCapacityIsReused(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	if err := st.SetQueueLimits(store.QueueLimits{MaxQueueDepth: 1, PerQueueLimit: 1, PerTenantLimit: 1}); err != nil {
		t.Fatal(err)
	}
	archived, err := st.CreateJob(ctx, model.NewJobInput{Name: "archived-reservation", Queue: "archive-q", TenantID: "archive-tenant"})
	if err != nil {
		t.Fatalf("create job to archive: %v", err)
	}
	if err := st.UpdateJobStatus(ctx, archived.ID, model.JobStatusArchived); err != nil {
		t.Fatalf("archive job: %v", err)
	}
	if _, err := st.CreateJob(ctx, model.NewJobInput{Name: "capacity-reuser", Queue: "archive-q", TenantID: "archive-tenant"}); err != nil {
		t.Fatalf("submit after archive released reservation: %v", err)
	}
	if err := st.UpdateJobStatus(ctx, archived.ID, model.JobStatusActive); !errors.Is(err, store.ErrJobArchived) {
		t.Fatalf("resume archived job = %v, want archived conflict", err)
	}
	if err := st.UpdateJobStatus(ctx, archived.ID, model.JobStatusPaused); !errors.Is(err, store.ErrJobArchived) {
		t.Fatalf("pause archived job = %v, want archived conflict", err)
	}
	job, err := st.GetJob(ctx, archived.ID)
	if err != nil {
		t.Fatalf("get archived job: %v", err)
	}
	if job.Status != model.JobStatusArchived {
		t.Fatalf("archived job status after rejected resume = %q, want archived", job.Status)
	}
	if _, err := st.CreateJob(ctx, model.NewJobInput{Name: "still-at-capacity", Queue: "archive-q", TenantID: "archive-tenant"}); !errors.Is(err, store.ErrQueueCapacityExceeded) {
		t.Fatalf("submission after rejected resume = %v, want capacity exceeded", err)
	}
}

func TestRetryDeadLetterUsesQueueBackpressure(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	if err := st.SetQueueLimits(store.QueueLimits{MaxQueueDepth: 1, PerQueueLimit: 1, PerTenantLimit: 1}); err != nil {
		t.Fatal(err)
	}
	job, err := st.CreateJob(ctx, model.NewJobInput{Name: "dead-letter-source", Queue: "retry-q", TenantID: "retry-tenant"})
	if err != nil {
		t.Fatalf("create dead-letter source: %v", err)
	}
	run, err := st.CreateRun(ctx, job.ID, job.Priority, time.Now())
	if err != nil {
		t.Fatalf("create source run: %v", err)
	}
	if _, err := st.Pool().Exec(ctx, `UPDATE job_runs SET status = 'dead', finished_at = now() WHERE id = $1`, run.ID); err != nil {
		t.Fatalf("mark source dead: %v", err)
	}
	var letterID string
	if err := st.Pool().QueryRow(ctx, `
		INSERT INTO dead_letters (job_run_id, reason) VALUES ($1, 'backpressure test') RETURNING id
	`, run.ID).Scan(&letterID); err != nil {
		t.Fatalf("create dead letter: %v", err)
	}
	blocker, err := st.CreateJob(ctx, model.NewJobInput{Name: "retry-capacity-blocker", Queue: "retry-q", TenantID: "retry-tenant"})
	if err != nil {
		t.Fatalf("create queue blocker: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := httptest.NewServer(api.NewRouter(st, log, testSecret, 10000, 10000))
	defer server.Close()
	token, err := api.MintToken(testSecret, "retry-tenant", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	status := postBackpressureRetry(t, server.URL, token, letterID)
	if status != http.StatusTooManyRequests {
		t.Fatalf("retry endpoint at capacity status = %d, want 429", status)
	}
	if _, err := st.RetryDeadLetter(ctx, letterID); !errors.Is(err, store.ErrQueueCapacityExceeded) {
		t.Fatalf("direct retry at capacity = %v, want capacity exceeded", err)
	}
	if err := st.CancelJob(ctx, blocker.ID); err != nil {
		t.Fatalf("cancel queue blocker: %v", err)
	}
	if _, err := st.RetryDeadLetter(ctx, letterID); err != nil {
		t.Fatalf("retry after capacity release: %v", err)
	}
}

func postBackpressureRetry(t *testing.T, baseURL, token, letterID string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/dead-letters/"+letterID+"/retry", nil)
	if err != nil {
		t.Fatalf("build retry request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("retry dead letter: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func postBackpressureJob(t *testing.T, baseURL, token string, body map[string]any) (int, model.Job) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var job model.Job
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
			t.Fatalf("decode job response: %v", err)
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return resp.StatusCode, job
}
