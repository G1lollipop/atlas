// Package integration exercises the real Postgres-backed store, the HTTP API, the
// scheduler's promotion logic, and the worker pool together against a live database —
// unlike the unit tests in internal/scheduler and internal/worker, which use an
// in-memory fake store. It requires a reachable Postgres and is skipped otherwise
// (CI provides one via a service container; locally, `docker compose up -d postgres`
// plus DATABASE_URL=postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable).
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/api"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/scheduler"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/G1lollipop/atlas/internal/worker"
	"github.com/google/uuid"
)

const testSecret = "integration-test-secret"

func TestJobLifecycleEndToEnd(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set; skipping integration test (requires a real Postgres)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer st.Close()

	// Tests run with the package directory as the working directory, so migrations/
	// (at the repo root) is one level up.
	if err := store.RunMigrations(ctx, st.Pool(), "../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	router := api.NewRouter(st, log, testSecret, 1000, 1000)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token, err := api.MintToken(testSecret, "integration-test", time.Hour)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}

	job := createJob(t, srv.URL, token, map[string]any{
		"name":            "echo",
		"payload":         map[string]any{"hello": "integration"},
		"max_attempts":    2,
		"timeout_seconds": 5,
	})

	promoter := &scheduler.Promoter{Store: st, Logger: log}
	promoted, err := promoter.PromoteOnce(ctx)
	if err != nil {
		t.Fatalf("PromoteOnce: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("expected exactly 1 run promoted, got %d", promoted)
	}

	pool := worker.NewPool(st, "integration-worker", 1, 30*time.Second, 100*time.Millisecond, log)
	pool.RegisterHandler("echo", worker.EchoHandler)

	workerCtx, workerCancel := context.WithTimeout(ctx, 3*time.Second)
	done := make(chan error, 1)
	go func() { done <- pool.Run(workerCtx) }()

	status := waitForTerminalRun(t, ctx, st, job.ID, 3*time.Second)
	workerCancel()
	<-done

	if status != model.RunStatusSucceeded {
		t.Fatalf("expected run to succeed, got status %q", status)
	}
}

func TestDeadLetterOnUnregisteredHandler(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set; skipping integration test (requires a real Postgres)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer st.Close()

	if err := store.RunMigrations(ctx, st.Pool(), "../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	router := api.NewRouter(st, log, testSecret, 1000, 1000)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token, err := api.MintToken(testSecret, "integration-test", time.Hour)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}

	job := createJob(t, srv.URL, token, map[string]any{
		"name":            "no_such_handler",
		"max_attempts":    2,
		"timeout_seconds": 5,
	})

	promoter := &scheduler.Promoter{Store: st, Logger: log}
	if _, err := promoter.PromoteOnce(ctx); err != nil {
		t.Fatalf("PromoteOnce: %v", err)
	}

	// No handler registered for "no_such_handler" — the pool has zero handlers.
	pool := worker.NewPool(st, "integration-worker-2", 1, 30*time.Second, 100*time.Millisecond, log)

	workerCtx, workerCancel := context.WithTimeout(ctx, 3*time.Second)
	done := make(chan error, 1)
	go func() { done <- pool.Run(workerCtx) }()

	status := waitForTerminalRun(t, ctx, st, job.ID, 3*time.Second)
	workerCancel()
	<-done

	if status != model.RunStatusDead {
		t.Fatalf("expected run to be dead-lettered, got status %q", status)
	}
}

func TestResourceAwareLeasingAndReservationRelease(t *testing.T) {
	st, ctx := openResourceTestStore(t)

	workerID := "resource-cpu-" + uuid.NewString()
	gpu8ID := "resource-gpu8-" + uuid.NewString()
	gpuWrongTypeID := "resource-gpu-wrong-type-" + uuid.NewString()
	gpu16ID := "resource-gpu16-" + uuid.NewString()
	registerTestWorker(t, ctx, st, model.Worker{ID: workerID, Hostname: "cpu-host", CPUCapacity: 2000, MemoryCapacityMB: 4096})
	registerTestWorker(t, ctx, st, model.Worker{ID: gpu8ID, Hostname: "gpu8-host", CPUCapacity: 4000, MemoryCapacityMB: 8192, GPUCount: 1, GPUType: "NVIDIA-A10", GPUMemoryMB: 8192})
	registerTestWorker(t, ctx, st, model.Worker{ID: gpuWrongTypeID, Hostname: "wrong-type-host", CPUCapacity: 4000, MemoryCapacityMB: 32768, GPUCount: 1, GPUType: "nvidia-l40", GPUMemoryMB: 24576})
	registerTestWorker(t, ctx, st, model.Worker{ID: gpu16ID, Hostname: "gpu16-host", CPUCapacity: 4000, MemoryCapacityMB: 16384, GPUCount: 1, GPUType: "nvidia-a100", GPUMemoryMB: 16384, Labels: map[string]string{"zone": "test"}})

	// The high-priority inference run must be skipped by CPU and 8 GB workers so
	// lower-priority work that fits can continue making progress.
	cpuJob := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "embedding-" + uuid.NewString(), WorkloadType: "embedding", Priority: 32766,
		RequiredCPUMillis: 1000, RequiredMemoryMB: 512, MaxAttempts: 2, TimeoutSeconds: 30,
	})
	gpuJob := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "inference-" + uuid.NewString(), WorkloadType: "inference", Priority: 32767,
		RequiredCPUMillis: 500, RequiredMemoryMB: 1024, RequiredGPUCount: 1,
		RequiredGPUMemoryMB: 16384, RequiredAccelerator: "NVIDIA-A100", MaxAttempts: 2, TimeoutSeconds: 30,
	})
	createResourceTestRun(t, ctx, st, cpuJob.ID, 32766)
	gpuRun := createResourceTestRun(t, ctx, st, gpuJob.ID, 32767)

	run, job, err := st.LeaseNextRun(ctx, workerID, time.Minute)
	if err != nil {
		t.Fatalf("CPU worker lease: %v", err)
	}
	if run == nil || job.ID != cpuJob.ID {
		t.Fatalf("CPU worker should skip GPU-only high-priority work and lease embedding; got run=%+v job=%+v", run, job)
	}

	run, job, err = st.LeaseNextRun(ctx, gpu8ID, time.Minute)
	if err != nil {
		t.Fatalf("8 GB GPU worker lease: %v", err)
	}
	if run != nil || job != nil {
		t.Fatalf("8 GB GPU worker must not lease a 16 GB VRAM job; got run=%+v job=%+v", run, job)
	}
	run, job, err = st.LeaseNextRun(ctx, gpuWrongTypeID, time.Minute)
	if err != nil {
		t.Fatalf("wrong accelerator worker lease: %v", err)
	}
	if run != nil || job != nil {
		t.Fatalf("24 GB L40 worker must not lease an A100-constrained job; got run=%+v job=%+v", run, job)
	}

	run, job, err = st.LeaseNextRun(ctx, gpu16ID, time.Minute)
	if err != nil {
		t.Fatalf("16 GB GPU worker lease: %v", err)
	}
	if run == nil || job.ID != gpuJob.ID || run.ID != gpuRun.ID {
		t.Fatalf("16 GB GPU worker should lease inference; got run=%+v job=%+v", run, job)
	}

	workers, err := st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers: %v", err)
	}
	assertWorkerResources(t, workers, gpu16ID, 3500, 15360, 0, 0)
	gpu16Worker := findWorker(t, workers, gpu16ID)
	if gpu16Worker.CurrentCPUReserved != 500 || gpu16Worker.CurrentMemoryReserved != 1024 || gpu16Worker.CurrentGPUReserved != 1 || gpu16Worker.CurrentGPUMemoryReserved != 16384 {
		t.Fatalf("active GPU run reservations mismatch: cpu=%d memory=%d gpu=%d gpu_memory=%d",
			gpu16Worker.CurrentCPUReserved, gpu16Worker.CurrentMemoryReserved,
			gpu16Worker.CurrentGPUReserved, gpu16Worker.CurrentGPUMemoryReserved)
	}

	// The public worker endpoint exposes the same derived availability, and a new
	// heartbeat changes capability metadata without overwriting active reservations.
	registerTestWorker(t, ctx, st, model.Worker{ID: gpu16ID, Hostname: "gpu16-host", CPUCapacity: 4000, MemoryCapacityMB: 16384, GPUCount: 1, GPUType: "nvidia-a100", GPUMemoryMB: 16384, Labels: map[string]string{"zone": "test"}})
	router := api.NewRouter(st, slog.New(slog.NewTextHandler(io.Discard, nil)), testSecret, 1000, 1000)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	token, err := api.MintToken(testSecret, "resource-test", time.Hour)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/workers", nil)
	if err != nil {
		t.Fatalf("build workers request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/workers: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/workers: expected 200, got %d", resp.StatusCode)
	}
	var apiWorkers []model.Worker
	if err := json.NewDecoder(resp.Body).Decode(&apiWorkers); err != nil {
		t.Fatalf("decode workers response: %v", err)
	}
	assertWorkerResources(t, apiWorkers, gpu16ID, 3500, 15360, 0, 0)

	if err := st.FailRun(ctx, gpuRun.ID, gpu16ID, run.Attempt, "retry check", true, 0); err != nil {
		t.Fatalf("requeue GPU run: %v", err)
	}
	workers, err = st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers after retry: %v", err)
	}
	assertWorkerResources(t, workers, gpu16ID, 4000, 16384, 1, 16384)

	run, _, err = st.LeaseNextRun(ctx, gpu16ID, time.Minute)
	if err != nil || run == nil {
		t.Fatalf("lease retried GPU run: run=%+v error=%v", run, err)
	}
	if err := st.MarkRunning(ctx, run.ID, gpu16ID, run.Attempt); err != nil {
		t.Fatalf("mark GPU run running: %v", err)
	}
	if err := st.CompleteRun(ctx, run.ID, gpu16ID, run.Attempt, map[string]any{"ok": true}); err != nil {
		t.Fatalf("complete GPU run: %v", err)
	}
	workers, err = st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers after completion: %v", err)
	}
	assertWorkerResources(t, workers, gpu16ID, 4000, 16384, 1, 16384)
}

func TestSameWorkerLeasesSerializeAndReclaimReleasesResources(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	workerID := "resource-concurrent-" + uuid.NewString()
	registerTestWorker(t, ctx, st, model.Worker{ID: workerID, Hostname: "concurrent-host", CPUCapacity: 2000, MemoryCapacityMB: 2048})
	first := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "cpu-heavy-a-" + uuid.NewString(), RequiredCPUMillis: 1500, RequiredMemoryMB: 1024,
		MaxAttempts: 2, TimeoutSeconds: 30,
	})
	second := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "cpu-heavy-b-" + uuid.NewString(), RequiredCPUMillis: 1500, RequiredMemoryMB: 1024,
		MaxAttempts: 2, TimeoutSeconds: 30,
	})
	third := createResourceTestJob(t, ctx, st, model.NewJobInput{
		Name: "cpu-heavy-c-" + uuid.NewString(), RequiredCPUMillis: 1500, RequiredMemoryMB: 1024,
		MaxAttempts: 2, TimeoutSeconds: 30,
	})
	createResourceTestRun(t, ctx, st, first.ID, 30000)
	createResourceTestRun(t, ctx, st, second.ID, 29999)
	createResourceTestRun(t, ctx, st, third.ID, 29998)

	type leaseResult struct {
		run *model.JobRun
		err error
	}
	results := make(chan leaseResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			run, _, err := st.LeaseNextRun(ctx, workerID, time.Minute)
			results <- leaseResult{run: run, err: err}
		}()
	}
	var leased *model.JobRun
	var leaseCount int
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent lease: %v", result.err)
		}
		if result.run != nil {
			leaseCount++
			leased = result.run
		}
	}
	if leaseCount != 1 {
		t.Fatalf("same worker with 2000m CPU should receive one 1500m lease, got %d leases", leaseCount)
	}

	workers, err := st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers with active lease: %v", err)
	}
	assertWorkerResources(t, workers, workerID, 500, 1024, 0, 0)

	if _, err := st.Pool().Exec(ctx, `UPDATE job_runs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, leased.ID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	reclaimed, err := st.ReclaimExpiredLeases(ctx)
	if err != nil || reclaimed < 1 {
		t.Fatalf("reclaim expired lease: count=%d error=%v", reclaimed, err)
	}
	reclaimedRun, err := st.GetRun(ctx, leased.ID)
	if err != nil || reclaimedRun.Status != model.RunStatusPending {
		t.Fatalf("target run should be reclaimed to pending: run=%+v error=%v", reclaimedRun, err)
	}
	workers, err = st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers after reclaim: %v", err)
	}
	assertWorkerResources(t, workers, workerID, 2000, 2048, 0, 0)

	// A re-lease by the same stable worker ID increments attempt. The stale
	// execution's older attempt must not be allowed to fail the new lease.
	releasedAgain, _, err := st.LeaseNextRun(ctx, workerID, time.Minute)
	if err != nil || releasedAgain == nil {
		t.Fatalf("re-lease reclaimed run: run=%+v error=%v", releasedAgain, err)
	}
	if releasedAgain.ID != leased.ID || releasedAgain.Attempt != leased.Attempt+1 {
		t.Fatalf("expected reclaimed run %s attempt %d, got run=%s attempt=%d", leased.ID,
			leased.Attempt+1, releasedAgain.ID, releasedAgain.Attempt)
	}
	if err := st.FailRun(ctx, leased.ID, workerID, leased.Attempt, "stale attempt", false, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stale attempt must not mutate re-leased run, got error %v", err)
	}

	// Separate workers retain parallel claim throughput while SKIP LOCKED keeps
	// them from receiving the same queued run.
	peerWorkerID := "resource-peer-" + uuid.NewString()
	secondPeerWorkerID := "resource-peer-" + uuid.NewString()
	registerTestWorker(t, ctx, st, model.Worker{ID: peerWorkerID, Hostname: "peer-host", CPUCapacity: 2000, MemoryCapacityMB: 2048})
	registerTestWorker(t, ctx, st, model.Worker{ID: secondPeerWorkerID, Hostname: "peer2-host", CPUCapacity: 2000, MemoryCapacityMB: 2048})
	parallel := make(chan leaseResult, 2)
	for _, id := range []string{peerWorkerID, secondPeerWorkerID} {
		go func(workerID string) {
			run, _, err := st.LeaseNextRun(ctx, workerID, time.Minute)
			parallel <- leaseResult{run: run, err: err}
		}(id)
	}
	claimedIDs := map[string]bool{}
	for i := 0; i < 2; i++ {
		result := <-parallel
		if result.err != nil {
			t.Fatalf("parallel lease by distinct workers: %v", result.err)
		}
		if result.run == nil {
			t.Fatal("distinct workers should claim both queued runs in parallel")
		}
		if claimedIDs[result.run.ID] {
			t.Fatalf("distinct workers received duplicate run %s", result.run.ID)
		}
		claimedIDs[result.run.ID] = true
	}
}

func openResourceTestStore(t *testing.T) (*store.PostgresStore, context.Context) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set; skipping resource store integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	adminStore, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	schema := "resource_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminStore.Pool().Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		adminStore.Close()
		t.Fatalf("create isolated test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := adminStore.Pool().Exec(cleanupCtx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`); err != nil {
			t.Errorf("drop isolated test schema %s: %v", schema, err)
		}
		adminStore.Close()
	})

	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	query := parsedURL.Query()
	query.Set("search_path", schema)
	parsedURL.RawQuery = query.Encode()
	st, err := store.New(ctx, parsedURL.String())
	if err != nil {
		t.Fatalf("connect to isolated test schema: %v", err)
	}
	t.Cleanup(st.Close)
	if err := store.RunMigrations(ctx, st.Pool(), "../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return st, ctx
}

func registerTestWorker(t *testing.T, ctx context.Context, st *store.PostgresStore, worker model.Worker) {
	t.Helper()
	if err := st.UpsertWorkerHeartbeat(ctx, worker); err != nil {
		t.Fatalf("register worker %s: %v", worker.ID, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := st.Pool().Exec(cleanupCtx, `DELETE FROM workers WHERE id = $1`, worker.ID); err != nil {
			t.Errorf("delete resource test worker %s: %v", worker.ID, err)
		}
	})
}

func createResourceTestJob(t *testing.T, ctx context.Context, st *store.PostgresStore, input model.NewJobInput) *model.Job {
	t.Helper()
	if input.Payload == nil {
		input.Payload = map[string]any{}
	}
	job, err := st.CreateJob(ctx, input)
	if err != nil {
		t.Fatalf("create resource job: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := st.Pool().Exec(cleanupCtx, `DELETE FROM jobs WHERE id = $1`, job.ID); err != nil {
			t.Errorf("delete resource test job %s: %v", job.ID, err)
		}
	})
	return job
}

func createResourceTestRun(t *testing.T, ctx context.Context, st *store.PostgresStore, jobID string, priority int16) *model.JobRun {
	t.Helper()
	run, err := st.CreateRun(ctx, jobID, priority, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("create resource run: %v", err)
	}
	return run
}

func assertWorkerResources(t *testing.T, workers any, workerID string, cpu, memory, gpu, gpuMemory int64) {
	t.Helper()
	worker := findWorker(t, workers, workerID)
	if worker.AvailableCPUMillis != cpu || worker.AvailableMemoryMB != memory || worker.AvailableGPUCount != gpu || worker.AvailableGPUMemoryMB != gpuMemory {
		t.Fatalf("worker %s resource snapshot mismatch: available cpu=%d memory=%d gpu=%d gpu_memory=%d; want %d/%d/%d/%d; reserved cpu=%d memory=%d gpu=%d gpu_memory=%d",
			workerID, worker.AvailableCPUMillis, worker.AvailableMemoryMB, worker.AvailableGPUCount,
			worker.AvailableGPUMemoryMB, cpu, memory, gpu, gpuMemory,
			worker.CurrentCPUReserved, worker.CurrentMemoryReserved, worker.CurrentGPUReserved,
			worker.CurrentGPUMemoryReserved)
	}
}

func findWorker(t *testing.T, workers any, workerID string) *model.Worker {
	t.Helper()
	var worker *model.Worker
	switch values := workers.(type) {
	case []*model.Worker:
		for _, candidate := range values {
			if candidate.ID == workerID {
				worker = candidate
				break
			}
		}
	case []model.Worker:
		for i := range values {
			if values[i].ID == workerID {
				worker = &values[i]
				break
			}
		}
	}
	if worker == nil {
		t.Fatalf("worker %s missing from resource snapshot", workerID)
	}
	return worker
}

func createJob(t *testing.T, baseURL, token string, body map[string]any) *model.Job {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/jobs: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v1/jobs: expected 201, got %d", resp.StatusCode)
	}

	var job model.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatalf("decode job response: %v", err)
	}
	return &job
}

func waitForTerminalRun(t *testing.T, ctx context.Context, st store.Store, jobID string, timeout time.Duration) model.RunStatus {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		runs, err := st.ListJobRuns(ctx, jobID, 1)
		if err == nil && len(runs) == 1 {
			switch runs[0].Status {
			case model.RunStatusSucceeded, model.RunStatusFailed, model.RunStatusDead:
				return runs[0].Status
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run for job %s did not reach a terminal state within %s", jobID, timeout)
	return ""
}
