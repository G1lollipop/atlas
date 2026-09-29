package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/scheduler"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/G1lollipop/atlas/internal/worker"
	"github.com/google/uuid"
)

type leaseObservationStore struct {
	store.Store
	mu              sync.Mutex
	renewals        int
	heartbeats      int
	janitorCalls    int
	reclaimedLeases int
}

func (s *leaseObservationStore) ExtendLease(ctx context.Context, runID, workerID string, attempt int16, extend time.Duration) error {
	err := s.Store.ExtendLease(ctx, runID, workerID, attempt, extend)
	if err == nil {
		s.mu.Lock()
		s.renewals++
		s.mu.Unlock()
	}
	return err
}

func (s *leaseObservationStore) UpsertWorkerHeartbeat(ctx context.Context, worker model.Worker) error {
	err := s.Store.UpsertWorkerHeartbeat(ctx, worker)
	if err == nil {
		s.mu.Lock()
		s.heartbeats++
		s.mu.Unlock()
	}
	return err
}

func (s *leaseObservationStore) ReclaimExpiredLeases(ctx context.Context) (int, error) {
	n, err := s.Store.ReclaimExpiredLeases(ctx)
	s.mu.Lock()
	s.janitorCalls++
	s.reclaimedLeases += n
	s.mu.Unlock()
	return n, err
}

func TestLongRunningHandlerRenewsLeaseIndependentlyOfHeartbeat(t *testing.T) {
	st, ctx := openResourceTestStore(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	workerID := "lease-renewal-" + uuid.NewString()
	job, err := st.CreateJob(ctx, model.NewJobInput{
		Name: "lease-renewal-test", MaxAttempts: 1, TimeoutSeconds: 5,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	registerTestWorker(t, ctx, st, model.Worker{
		ID: workerID, Hostname: "lease-renewal-test", CPUCapacity: 1000, MemoryCapacityMB: 1024,
	})
	promoter := &scheduler.Promoter{Store: st, Logger: log}
	if promoted, err := promoter.PromoteOnce(ctx); err != nil || promoted != 1 {
		t.Fatalf("PromoteOnce() = %d, %v; want 1, nil", promoted, err)
	}
	if assigned, err := promoter.DispatchOnce(ctx); err != nil || assigned != 1 {
		t.Fatalf("DispatchOnce() = %d, %v; want 1, nil", assigned, err)
	}

	observed := &leaseObservationStore{Store: st}
	pool := worker.NewPool(observed, workerID, 1, 300*time.Millisecond, 20*time.Millisecond, log)
	pool.SetCapabilities(model.Worker{CPUCapacity: 1000, MemoryCapacityMB: 1024})
	var handlerCalls atomic.Int32
	handlerStarted := make(chan struct{}, 1)
	pool.RegisterHandler(job.Name, func(ctx context.Context, _ *model.Job, _ *model.JobRun) (map[string]any, error) {
		handlerCalls.Add(1)
		select {
		case handlerStarted <- struct{}{}:
		default:
		}
		select {
		case <-time.After(900 * time.Millisecond):
			return map[string]any{"completed": true}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- pool.Run(workerCtx) }()
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("worker did not start the assigned run")
	}

	status := waitForTerminalRun(t, ctx, st, job.ID, 3*time.Second)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Pool.Run() error = %v, want context.Canceled", err)
	}
	if status != model.RunStatusSucceeded {
		t.Fatalf("long-running job status = %q, want succeeded", status)
	}
	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want one execution despite janitor polling", got)
	}

	observed.mu.Lock()
	defer observed.mu.Unlock()
	if observed.renewals < 2 {
		t.Fatalf("successful lease renewals = %d, want repeated renewal during 900ms handler", observed.renewals)
	}
	if observed.janitorCalls == 0 || observed.reclaimedLeases != 0 {
		t.Fatalf("janitor calls/reclaimed leases = %d/%d, want janitor activity with no reclaim", observed.janitorCalls, observed.reclaimedLeases)
	}
	if observed.heartbeats < observed.renewals*3 {
		t.Fatalf("worker heartbeats=%d, lease renewals=%d; expected independent faster heartbeat cadence", observed.heartbeats, observed.renewals)
	}
}
