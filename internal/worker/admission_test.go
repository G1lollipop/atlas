package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

type janitorTrackingStore struct {
	*fakeStore
	reclaimCalls  atomic.Int32
	reclaimNotify chan struct{}
}

func (s *janitorTrackingStore) ReclaimExpiredLeases(context.Context) (int, error) {
	s.reclaimCalls.Add(1)
	select {
	case s.reclaimNotify <- struct{}{}:
	default:
	}
	return 0, nil
}

func TestResourceAdmissionAllowsMultipleCPUJobsUpToCapacity(t *testing.T) {
	admission := newResourceAdmission(model.Worker{
		CPUCapacity:      4000,
		MemoryCapacityMB: 8192,
	})
	job := &model.Job{RequiredCPUMillis: 2000, RequiredMemoryMB: 2048}

	releaseFirst, err := admission.tryAcquire(job)
	if err != nil {
		t.Fatalf("first CPU reservation failed: %v", err)
	}
	releaseSecond, err := admission.tryAcquire(job)
	if err != nil {
		t.Fatalf("second CPU reservation failed: %v", err)
	}
	assertReservations(t, admission, 4000, 4096, 0, 0)
	if _, err := admission.tryAcquire(job); err == nil || !strings.Contains(err.Error(), "CPU request") {
		t.Fatalf("third CPU reservation error = %v, want capacity rejection", err)
	}

	releaseSecond()
	releaseSecond() // A permit release is safe to repeat.
	releaseFirst()
	releaseAll, err := admission.tryAcquire(&model.Job{RequiredCPUMillis: 4000})
	if err != nil {
		t.Fatalf("reservation after release failed: %v", err)
	}
	releaseAll()

	assertReservations(t, admission, 0, 0, 0, 0)
}

func TestResourceAdmissionBoundsGPUCountAndVRAM(t *testing.T) {
	admission := newResourceAdmission(model.Worker{
		CPUCapacity:      8000,
		MemoryCapacityMB: 32768,
		GPUCount:         2,
		GPUType:          "H100",
		GPUMemoryMB:      81920,
	})
	job := &model.Job{
		RequiredGPUCount:    2,
		RequiredGPUMemoryMB: 40960,
		RequiredAccelerator: "h100",
	}
	release, err := admission.tryAcquire(job)
	if err != nil {
		t.Fatalf("two-GPU reservation failed: %v", err)
	}
	assertReservations(t, admission, 0, 0, 2, 81920)
	if _, err := admission.tryAcquire(&model.Job{RequiredGPUCount: 1, RequiredGPUMemoryMB: 1}); err == nil {
		t.Fatal("GPU reservation exceeded the worker's two GPU slots")
	}
	release()

	if _, err := admission.tryAcquire(&model.Job{RequiredGPUCount: 1, RequiredGPUMemoryMB: 81921}); err == nil || !strings.Contains(err.Error(), "per GPU") {
		t.Fatalf("over-capacity VRAM request error = %v, want per-GPU rejection", err)
	}
	if _, err := admission.tryAcquire(&model.Job{RequiredGPUCount: 1, RequiredAccelerator: "A10"}); err == nil || !strings.Contains(err.Error(), "accelerator") {
		t.Fatalf("wrong accelerator error = %v, want accelerator rejection", err)
	}

	assertReservations(t, admission, 0, 0, 0, 0)
}

func TestLeaseLoopCapsConcurrentHandlersAndReleasesOnCancellation(t *testing.T) {
	fs := newFakeStore()
	fs.leaseNotify = make(chan struct{}, 8)
	workerID := "bounded-worker"
	expiresAt := time.Now().Add(time.Minute)
	for i := 0; i < 4; i++ {
		run := &model.JobRun{
			ID:                  "run-" + string(rune('1'+i)),
			Status:              model.RunStatusAssigned,
			AssignedWorkerID:    &workerID,
			AssignmentExpiresAt: &expiresAt,
		}
		job := &model.Job{
			ID:                "job-" + run.ID,
			Name:              "blocking",
			RequiredCPUMillis: 500,
			RequiredMemoryMB:  256,
			MaxAttempts:       3,
			TimeoutSeconds:    60,
		}
		fs.leaseCandidates = append(fs.leaseCandidates, leaseCandidate{run: run, job: job})
	}

	p := NewPool(fs, workerID, 2, time.Minute, 5*time.Millisecond, testLogger())
	p.SetCapabilities(model.Worker{CPUCapacity: 8000, MemoryCapacityMB: 8192})
	var active atomic.Int32
	var maximum atomic.Int32
	started := make(chan struct{}, 4)
	p.RegisterHandler("blocking", func(ctx context.Context, _ *model.Job, _ *model.JobRun) (map[string]any, error) {
		current := active.Add(1)
		for previous := maximum.Load(); current > previous && !maximum.CompareAndSwap(previous, current); previous = maximum.Load() {
		}
		defer active.Add(-1)
		started <- struct{}{}
		<-ctx.Done()
		return nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("worker did not start both available execution slots")
		}
	}
	select {
	case <-started:
		cancel()
		t.Fatal("worker started a handler beyond configured concurrency")
	case <-time.After(40 * time.Millisecond):
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Pool.Run() error = %v, want context.Canceled", err)
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum active handlers = %d, want 2", got)
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active handlers after cancellation = %d, want 0", got)
	}
	if got := len(p.executionSlots); got != 0 {
		t.Fatalf("execution slots retained after cancellation = %d, want 0", got)
	}
	assertReservations(t, p.resources, 0, 0, 0, 0)
}

func TestResourceAdmissionReleaseIsSafeAcrossCancellationRace(t *testing.T) {
	admission := newResourceAdmission(model.Worker{CPUCapacity: 1000, MemoryCapacityMB: 1024})
	release, err := admission.tryAcquire(&model.Job{RequiredCPUMillis: 1000, RequiredMemoryMB: 1024})
	if err != nil {
		t.Fatalf("resource reservation failed: %v", err)
	}

	var releases atomic.Int32
	done := make(chan struct{}, 16)
	for i := 0; i < cap(done); i++ {
		go func() {
			release()
			releases.Add(1)
			done <- struct{}{}
		}()
	}
	for i := 0; i < cap(done); i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent permit releases did not finish")
		}
	}
	if got := releases.Load(); got != int32(cap(done)) {
		t.Fatalf("release calls = %d, want %d", got, cap(done))
	}
	assertReservations(t, admission, 0, 0, 0, 0)
}

func TestResourceAdmissionMismatchHonorsMaxAttempts(t *testing.T) {
	fs := newFakeStore()
	p := newTestPool(fs)
	p.RegisterHandler("oversized", func(context.Context, *model.Job, *model.JobRun) (map[string]any, error) {
		return nil, nil
	})
	job := &model.Job{
		ID:                "oversized-job",
		Name:              "oversized",
		RequiredCPUMillis: 1,
		MaxAttempts:       2,
		TimeoutSeconds:    5,
	}

	p.executeOne(context.Background(), &model.JobRun{ID: "oversized-run", Attempt: 1}, job)
	if len(fs.failRunCalls) != 1 || !fs.failRunCalls[0].requeue {
		t.Fatalf("first admission rejection FailRun calls = %#v, want one requeue", fs.failRunCalls)
	}
	if len(fs.markDeadCalls) != 0 {
		t.Fatalf("MarkDead calls after first admission rejection = %d, want 0", len(fs.markDeadCalls))
	}

	p.executeOne(context.Background(), &model.JobRun{ID: "oversized-run", Attempt: 2}, job)
	if len(fs.failRunCalls) != 2 || fs.failRunCalls[1].requeue {
		t.Fatalf("final admission rejection FailRun calls = %#v, want terminal failure", fs.failRunCalls)
	}
	if len(fs.markDeadCalls) != 1 {
		t.Fatalf("MarkDead calls after max attempts = %d, want 1", len(fs.markDeadCalls))
	}
	assertReservations(t, p.resources, 0, 0, 0, 0)
}

func assertReservations(t *testing.T, admission *resourceAdmission, cpu, memory, gpu, gpuMemory int64) {
	t.Helper()
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if admission.cpuReservedMillis != cpu || admission.memoryReservedMB != memory ||
		admission.gpuReserved != gpu || admission.gpuMemoryReserved != gpuMemory {
		t.Fatalf("reserved resources = CPU %d, memory %d, GPU %d, GPU memory %d; want %d/%d/%d/%d",
			admission.cpuReservedMillis, admission.memoryReservedMB, admission.gpuReserved,
			admission.gpuMemoryReserved, cpu, memory, gpu, gpuMemory)
	}
}

func TestPoolRunValidatesConcurrencyAndPreservesJanitorOnlyMode(t *testing.T) {
	negativeStore := newFakeStore()
	negativePool := NewPool(negativeStore, "negative-worker", -1, time.Second, time.Millisecond, testLogger())
	if err := negativePool.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "concurrency") {
		t.Fatalf("negative concurrency error = %v, want validation error", err)
	}
	if len(negativeStore.heartbeatCalls) != 0 {
		t.Fatal("worker registered despite invalid negative concurrency")
	}

	janitorStore := &janitorTrackingStore{
		fakeStore:     newFakeStore(),
		reclaimNotify: make(chan struct{}, 1),
	}
	janitorPool := NewPool(janitorStore, "janitor-worker", 0, time.Second, 5*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- janitorPool.Run(ctx) }()
	select {
	case <-janitorStore.reclaimNotify:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("janitor-only mode did not reclaim expired leases")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("janitor-only Pool.Run() error = %v, want context.Canceled", err)
	}
	if len(janitorStore.heartbeatCalls) != 0 {
		t.Fatalf("janitor-only mode registered or heartbeated %d times, want none", len(janitorStore.heartbeatCalls))
	}
	if len(janitorStore.leaseWorkerIDs) != 0 {
		t.Fatalf("janitor-only mode made %d lease calls, want none", len(janitorStore.leaseWorkerIDs))
	}
	if got := janitorStore.reclaimCalls.Load(); got == 0 {
		t.Fatal("janitor-only mode made no reclaim calls")
	}
}
