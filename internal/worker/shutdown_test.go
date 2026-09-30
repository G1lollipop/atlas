package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

type blockedHeartbeatStore struct {
	*fakeStore
	aliveCalls atomic.Int32
	started    chan struct{}
	release    chan struct{}
}

func (s *blockedHeartbeatStore) UpsertWorkerHeartbeat(ctx context.Context, heartbeat model.Worker) error {
	if heartbeat.Status == model.WorkerStatusAlive && s.aliveCalls.Add(1) == 2 {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.fakeStore.UpsertWorkerHeartbeat(ctx, heartbeat)
}

func TestPoolDrainHeartbeatFollowsInFlightAliveHeartbeat(t *testing.T) {
	fs := &blockedHeartbeatStore{
		fakeStore: newFakeStore(),
		started:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	p := NewPool(fs, "heartbeat-order-worker", 1, time.Second, 10*time.Millisecond, testLogger())
	if err := p.SetShutdownGracePeriod(500 * time.Millisecond); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	var releaseOnce sync.Once
	releaseHeartbeat := func() { releaseOnce.Do(func() { close(fs.release) }) }
	finished := false
	defer func() {
		cancel()
		releaseHeartbeat()
		if !finished {
			select {
			case <-done:
			case <-time.After(time.Second):
			}
		}
	}()
	select {
	case <-fs.started:
	case <-time.After(time.Second):
		t.Fatal("periodic alive heartbeat did not become blocked")
	}

	cancel()
	drainDeadline := time.Now().Add(time.Second)
	for p.workerRecord().Status != model.WorkerStatusDraining && time.Now().Before(drainDeadline) {
		time.Sleep(time.Millisecond)
	}
	if p.workerRecord().Status != model.WorkerStatusDraining {
		t.Fatal("worker did not enter draining state")
	}
	releaseHeartbeat()
	select {
	case err := <-done:
		finished = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Pool.Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Pool.Run() did not finish after the blocked heartbeat was released")
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.heartbeatCalls) < 3 {
		t.Fatalf("heartbeat calls = %d, want initial alive, in-flight alive, and final draining", len(fs.heartbeatCalls))
	}
	if got := fs.heartbeatCalls[len(fs.heartbeatCalls)-1].Status; got != model.WorkerStatusDraining {
		t.Fatalf("last persisted worker status = %q, want draining", got)
	}
}

func TestPoolDrainsActiveHandlerAndKeepsHeartbeatAndLeaseRenewal(t *testing.T) {
	fs := newFakeStore()
	workerID := "draining-worker"
	expires := time.Now().Add(time.Minute)
	fs.leaseCandidates = []leaseCandidate{{
		run: &model.JobRun{
			ID: "drain-run", Status: model.RunStatusAssigned,
			AssignedWorkerID: &workerID, AssignmentExpiresAt: &expires,
		},
		job: &model.Job{ID: "drain-job", Name: "slow", MaxAttempts: 1, TimeoutSeconds: 5},
	}}

	p := NewPool(fs, workerID, 1, 150*time.Millisecond, 10*time.Millisecond, testLogger())
	if err := p.SetShutdownGracePeriod(500 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	p.RegisterHandler("slow", func(context.Context, *model.Job, *model.JobRun) (map[string]any, error) {
		close(started)
		<-release
		return map[string]any{"drained": true}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("worker did not start the assigned handler")
	}
	cancel()

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		fs.mu.Lock()
		drainingHeartbeats := 0
		for _, heartbeat := range fs.heartbeatCalls {
			if heartbeat.Status == model.WorkerStatusDraining {
				drainingHeartbeats++
			}
		}
		renewals := len(fs.extendLeaseCalls)
		fs.mu.Unlock()
		if drainingHeartbeats >= 2 && renewals > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	fs.mu.Lock()
	drainingHeartbeats, renewals := 0, len(fs.extendLeaseCalls)
	for _, heartbeat := range fs.heartbeatCalls {
		if heartbeat.Status == model.WorkerStatusDraining {
			drainingHeartbeats++
		}
	}
	fs.mu.Unlock()
	if drainingHeartbeats < 2 {
		t.Errorf("draining heartbeats = %d, want an immediate update plus a heartbeat during the drain", drainingHeartbeats)
	}
	if renewals == 0 {
		t.Fatal("worker did not renew the active lease during drain")
	}

	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Pool.Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not finish within its shutdown grace period")
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.leaseWorkerIDs) != 1 {
		t.Errorf("lease calls = %d, want exactly the initial claim and no new claim during drain", len(fs.leaseWorkerIDs))
	}
	if len(fs.completeRunCalls) != 1 || fs.completeRunCalls[0].runID != "drain-run" {
		t.Errorf("completed runs = %#v, want the in-flight run to complete", fs.completeRunCalls)
	}
	if len(fs.failRunCalls) != 0 {
		t.Errorf("failure calls during successful drain = %d, want 0", len(fs.failRunCalls))
	}
}

func TestPoolReturnsAfterGraceWhenHandlerIgnoresCancellation(t *testing.T) {
	fs := newFakeStore()
	workerID := "stubborn-worker"
	expires := time.Now().Add(time.Minute)
	fs.leaseCandidates = []leaseCandidate{{
		run: &model.JobRun{
			ID: "stubborn-run", Status: model.RunStatusAssigned,
			AssignedWorkerID: &workerID, AssignmentExpiresAt: &expires,
		},
		job: &model.Job{ID: "stubborn-job", Name: "stubborn", MaxAttempts: 1, TimeoutSeconds: 5},
	}}

	p := NewPool(fs, workerID, 1, 150*time.Millisecond, 10*time.Millisecond, testLogger())
	if err := p.SetShutdownGracePeriod(200 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	handlerReturned := make(chan struct{})
	p.RegisterHandler("stubborn", func(context.Context, *model.Job, *model.JobRun) (map[string]any, error) {
		close(started)
		<-release
		close(handlerReturned)
		return map[string]any{"too_late": true}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("worker did not start the stubborn handler")
	}
	cancel()
	shutdownStarted := time.Now()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Pool.Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Pool.Run() did not return after the configured grace period")
	}
	if elapsed := time.Since(shutdownStarted); elapsed > 350*time.Millisecond {
		t.Fatalf("shutdown took %s, want it bounded near the 200ms grace period", elapsed)
	}

	fs.mu.Lock()
	renewalsAtShutdown := len(fs.extendLeaseCalls)
	if len(fs.completeRunCalls) != 0 || len(fs.failRunCalls) != 0 || len(fs.markDeadCalls) != 0 {
		fs.mu.Unlock()
		t.Fatal("worker wrote run lifecycle state after grace expired")
	}
	fs.mu.Unlock()
	if renewalsAtShutdown == 0 {
		t.Fatal("worker did not renew the active lease during grace")
	}
	time.Sleep(100 * time.Millisecond)
	fs.mu.Lock()
	if got := len(fs.extendLeaseCalls); got != renewalsAtShutdown {
		fs.mu.Unlock()
		t.Fatalf("lease renewals after grace expired = %d, want unchanged count %d", got, renewalsAtShutdown)
	}
	fs.mu.Unlock()

	close(release)
	select {
	case <-handlerReturned:
	case <-time.After(time.Second):
		t.Fatal("stubborn handler did not return after release")
	}
	deadline := time.Now().Add(time.Second)
	for len(p.executionSlots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(p.executionSlots) != 0 {
		t.Fatal("late handler did not release its execution slot")
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.completeRunCalls) != 0 || len(fs.failRunCalls) != 0 || len(fs.markDeadCalls) != 0 {
		t.Fatal("late handler wrote lifecycle state after shutdown returned")
	}
}
