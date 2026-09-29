package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

func TestDurableCancellationCancelsHandlerContextAndAcknowledgesRun(t *testing.T) {
	fs := newFakeStore()
	fs.cancelObserved = make(chan struct{}, 1)
	p := NewPool(fs, "worker-1", 1, 30*time.Millisecond, 0, testLogger())

	started := make(chan struct{})
	handlerDone := make(chan error, 1)
	p.RegisterHandler("long-job", func(ctx context.Context, _ *model.Job, _ *model.JobRun) (map[string]any, error) {
		close(started)
		<-ctx.Done()
		handlerDone <- ctx.Err()
		return nil, ctx.Err()
	})

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		p.executeOne(context.Background(), &model.JobRun{ID: "run-1", Attempt: 1}, &model.Job{
			ID: "job-1", Name: "long-job", MaxAttempts: 3, TimeoutSeconds: 5,
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	fs.mu.Lock()
	fs.cancellationRequested = true
	fs.mu.Unlock()

	select {
	case <-fs.cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("worker did not poll the durable cancellation request")
	}
	select {
	case err := <-handlerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not cancel the handler context")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("executeOne did not return after cancellation")
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.markCanceledCalls) != 1 || fs.markCanceledCalls[0] != "run-1" {
		t.Fatalf("MarkCanceled calls = %#v, want one ack for run-1", fs.markCanceledCalls)
	}
	if len(fs.completeRunCalls) != 0 || len(fs.failRunCalls) != 0 || len(fs.markDeadCalls) != 0 {
		t.Fatalf("lifecycle writes after cancellation: complete=%d fail=%d dead=%d; want none",
			len(fs.completeRunCalls), len(fs.failRunCalls), len(fs.markDeadCalls))
	}
}

func TestHandlerIgnoringContextCannotCompleteAfterDurableCancellation(t *testing.T) {
	fs := newFakeStore()
	fs.cancelObserved = make(chan struct{}, 1)
	p := NewPool(fs, "worker-1", 1, 30*time.Millisecond, 0, testLogger())

	started := make(chan struct{})
	release := make(chan struct{})
	p.RegisterHandler("stubborn-job", func(context.Context, *model.Job, *model.JobRun) (map[string]any, error) {
		close(started)
		<-release
		return map[string]any{"should_not_complete": true}, nil
	})

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		p.executeOne(context.Background(), &model.JobRun{ID: "run-2", Attempt: 1}, &model.Job{
			ID: "job-2", Name: "stubborn-job", MaxAttempts: 3, TimeoutSeconds: 5,
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	fs.mu.Lock()
	fs.cancellationRequested = true
	fs.mu.Unlock()
	select {
	case <-fs.cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("worker did not poll the durable cancellation request")
	}

	select {
	case <-finished:
		t.Fatal("handler that ignores context should remain bounded by its own return or lease expiry")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("executeOne did not return after handler returned")
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.completeRunCalls) != 0 || len(fs.failRunCalls) != 0 {
		t.Fatalf("handler result wrote lifecycle state after cancellation: complete=%d fail=%d",
			len(fs.completeRunCalls), len(fs.failRunCalls))
	}
	if len(fs.markCanceledCalls) != 1 || fs.markCanceledCalls[0] != "run-2" {
		t.Fatalf("MarkCanceled calls = %#v, want one ack for run-2", fs.markCanceledCalls)
	}
}
