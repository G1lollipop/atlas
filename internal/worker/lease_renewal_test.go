package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
)

type leaseRenewalFailureStore struct {
	*fakeStore
	failure  error
	observed chan struct{}
}

type timeoutRenewalStore struct {
	*fakeStore
	started chan struct{}
}

func (s *timeoutRenewalStore) ExtendLease(ctx context.Context, runID, workerID string, attempt int16, extend time.Duration) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *leaseRenewalFailureStore) ExtendLease(ctx context.Context, runID, workerID string, attempt int16, extend time.Duration) error {
	_ = s.fakeStore.ExtendLease(ctx, runID, workerID, attempt, extend)
	select {
	case s.observed <- struct{}{}:
	default:
	}
	return s.failure
}

func TestLeaseRenewalFailureCancelsHandlerAndSkipsLifecycleWrite(t *testing.T) {
	fs := &leaseRenewalFailureStore{
		fakeStore: newFakeStore(),
		failure:   store.ErrNotFound,
		observed:  make(chan struct{}, 1),
	}
	p := NewPool(fs, "test-worker", 1, 30*time.Millisecond, 0, testLogger())

	handlerResult := make(chan error, 1)
	p.RegisterHandler("long-job", func(ctx context.Context, _ *model.Job, _ *model.JobRun) (map[string]any, error) {
		select {
		case <-time.After(time.Second):
			return map[string]any{"unexpected": true}, nil
		case <-ctx.Done():
			handlerResult <- ctx.Err()
			return nil, ctx.Err()
		}
	})

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		p.executeOne(context.Background(), &model.JobRun{ID: "run-1", Attempt: 1}, &model.Job{
			ID: "job-1", Name: "long-job", MaxAttempts: 3, TimeoutSeconds: 5,
		})
	}()

	select {
	case <-fs.observed:
	case <-time.After(time.Second):
		t.Fatal("lease renewal was not attempted")
	}
	select {
	case err := <-handlerResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not stop after lease ownership was lost")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("executeOne did not return after handler cancellation")
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.completeRunCalls) != 0 || len(fs.failRunCalls) != 0 || len(fs.markDeadCalls) != 0 {
		t.Fatalf("lifecycle writes after lease loss: complete=%d fail=%d dead=%d; want none",
			len(fs.completeRunCalls), len(fs.failRunCalls), len(fs.markDeadCalls))
	}
}

func TestHandlerTimeoutStillRecordsFailureWhenRenewalIsCanceled(t *testing.T) {
	fs := &timeoutRenewalStore{fakeStore: newFakeStore(), started: make(chan struct{}, 1)}
	p := NewPool(fs, "test-worker", 1, 1500*time.Millisecond, 0, testLogger())
	p.RegisterHandler("slow-job", func(ctx context.Context, _ *model.Job, _ *model.JobRun) (map[string]any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	p.executeOne(context.Background(), &model.JobRun{ID: "run-1", Attempt: 1}, &model.Job{
		ID: "job-1", Name: "slow-job", MaxAttempts: 2, TimeoutSeconds: 1,
	})
	select {
	case <-fs.started:
	default:
		t.Fatal("renewal was not in progress at handler timeout")
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.failRunCalls) != 1 || !fs.failRunCalls[0].requeue {
		t.Fatalf("timeout FailRun calls = %#v, want one scheduled retry", fs.failRunCalls)
	}
}
