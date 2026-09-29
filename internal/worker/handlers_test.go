package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
)

func TestEchoHandler(t *testing.T) {
	payload := map[string]any{"hello": "world", "n": float64(42)}
	job := &model.Job{Name: "echo", Payload: payload}
	run := &model.JobRun{ID: "run-1", JobID: "job-1"}

	result, err := EchoHandler(context.Background(), job, run)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != len(payload) {
		t.Fatalf("expected result to have %d keys, got %d", len(payload), len(result))
	}
	for k, v := range payload {
		if result[k] != v {
			t.Errorf("result[%q] = %v, want %v", k, result[k], v)
		}
	}
}

func TestSleepHandler_RespectsContextCancellation(t *testing.T) {
	job := &model.Job{Name: "sleep", Payload: map[string]any{"seconds": float64(60)}}
	run := &model.JobRun{ID: "run-1", JobID: "job-1"}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := SleepHandler(ctx, job, run)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from a cancelled sleep, got nil")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("expected SleepHandler to return promptly on ctx cancellation, took %v", elapsed)
	}
}

func TestHTTPCallHandler_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("hello from server"))
	}))
	defer ts.Close()

	job := &model.Job{Name: "http_call", Payload: map[string]any{"url": ts.URL}}
	run := &model.JobRun{ID: "run-1", JobID: "job-1"}

	result, err := HTTPCallHandler(context.Background(), job, run)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["status_code"] != http.StatusCreated {
		t.Errorf("status_code = %v, want %d", result["status_code"], http.StatusCreated)
	}
	if result["body"] != "hello from server" {
		t.Errorf("body = %q, want %q", result["body"], "hello from server")
	}
}

func TestHTTPCallHandler_MissingURL(t *testing.T) {
	job := &model.Job{Name: "http_call", Payload: map[string]any{}}
	run := &model.JobRun{ID: "run-1", JobID: "job-1"}

	_, err := HTTPCallHandler(context.Background(), job, run)
	if err == nil {
		t.Fatal("expected error when payload is missing \"url\", got nil")
	}
}

func TestHTTPCallHandler_SendsExecutionKeyForMutation(t *testing.T) {
	const executionKey = "c47f5b2c-9b8f-4f6f-b6db-4c8562c1c8f4"
	var gotKey string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	job := &model.Job{Name: "http_call", Payload: map[string]any{"url": ts.URL, "method": http.MethodPost}}
	run := &model.JobRun{ID: "run-1", ExecutionKey: executionKey}
	if _, err := HTTPCallHandler(context.Background(), job, run); err != nil {
		t.Fatalf("HTTPCallHandler returned an error: %v", err)
	}
	if gotKey != executionKey {
		t.Fatalf("Idempotency-Key = %q, want %q", gotKey, executionKey)
	}
}

func TestHTTPCallHandler_OmitsExecutionKeyForSafeMethod(t *testing.T) {
	var gotKey string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	job := &model.Job{Name: "http_call", Payload: map[string]any{"url": ts.URL, "method": http.MethodGet}}
	run := &model.JobRun{ID: "run-1", ExecutionKey: "c47f5b2c-9b8f-4f6f-b6db-4c8562c1c8f4"}
	if _, err := HTTPCallHandler(context.Background(), job, run); err != nil {
		t.Fatalf("HTTPCallHandler returned an error: %v", err)
	}
	if gotKey != "" {
		t.Fatalf("GET Idempotency-Key = %q, want empty", gotKey)
	}
}

func TestIdempotentRecordHandlerUsesRunExecutionKey(t *testing.T) {
	const executionKey = "c47f5b2c-9b8f-4f6f-b6db-4c8562c1c8f4"
	idem := &recordingIdempotencyStore{result: map[string]any{"recorded": true}}
	handler := NewIdempotentRecordHandler(idem)

	result, err := handler(context.Background(), &model.Job{Payload: map[string]any{"value": "saved"}},
		&model.JobRun{ExecutionKey: executionKey})
	if err != nil {
		t.Fatalf("handler returned an error: %v", err)
	}
	if idem.executionKey != executionKey {
		t.Fatalf("execution key = %q, want %q", idem.executionKey, executionKey)
	}
	if result["recorded"] != true {
		t.Fatalf("result = %v, want recorded=true", result)
	}
}

type recordingIdempotencyStore struct {
	executionKey string
	result       map[string]any
}

func (s *recordingIdempotencyStore) RunOnce(_ context.Context, executionKey string, _ store.TransactionalExecution) (map[string]any, bool, error) {
	s.executionKey = executionKey
	return s.result, true, nil
}
