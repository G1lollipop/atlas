package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
)

// Handler executes a Job for a given JobRun attempt and returns the result to persist.
type Handler func(ctx context.Context, job *model.Job, run *model.JobRun) (result map[string]any, err error)

// EchoHandler returns the job's payload unchanged, useful for exercising the
// lease/execute/complete pipeline end-to-end without any real side effects.
func EchoHandler(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
	return job.Payload, nil
}

// SleepHandler sleeps for job.Payload["seconds"] (default 1), honoring ctx
// cancellation. It exists to let someone demo the timeout/retry path by
// setting seconds longer than the job's timeout_seconds.
func SleepHandler(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
	seconds := 1.0
	if v, ok := job.Payload["seconds"]; ok {
		if f, ok := v.(float64); ok {
			seconds = f
		}
	}

	timer := time.NewTimer(time.Duration(seconds * float64(time.Second)))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}

	return map[string]any{"slept_seconds": seconds}, nil
}

// httpCallClient is a safety-net timeout in addition to ctx cancellation (e.g. if a
// caller passes a context.Background() by mistake).
var httpCallClient = &http.Client{Timeout: 30 * time.Second}

// HTTPCallHandler makes an HTTP request described by job.Payload["url"] (required)
// and job.Payload["method"] (default GET), returning the status code and up to the
// first 4096 bytes of the response body.
func HTTPCallHandler(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
	rawURL, ok := job.Payload["url"]
	if !ok {
		return nil, errors.New("http_call: payload missing required \"url\"")
	}
	urlStr, ok := rawURL.(string)
	if !ok || urlStr == "" {
		return nil, errors.New("http_call: payload \"url\" must be a non-empty string")
	}

	method := "GET"
	if rawMethod, ok := job.Payload["method"]; ok {
		if m, ok := rawMethod.(string); ok && m != "" {
			method = m
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("http_call: building request: %w", err)
	}
	// The receiving service must honor this key to suppress duplicate remote
	// effects after a lease expires or a worker loses its response.
	if run != nil && run.ExecutionKey != "" && !isSafeHTTPMethod(method) {
		req.Header.Set("Idempotency-Key", run.ExecutionKey)
	}

	resp, err := httpCallClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http_call: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	const maxBody = 4096
	buf := make([]byte, maxBody)
	n, err := io.ReadFull(resp.Body, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("http_call: reading response body: %w", err)
	}

	return map[string]any{
		"status_code": resp.StatusCode,
		"body":        string(buf[:n]),
	}, nil
}

func isSafeHTTPMethod(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}

// NewIdempotentRecordHandler demonstrates database-backed deduplication for a
// handler with a transactional side effect. It writes the submitted payload to
// idempotent_handler_effects and stores the result in the same transaction, so
// reclaimed or manually retried executions return the saved result. Register it
// as "idempotent_record" when wiring a Postgres-backed worker.
func NewIdempotentRecordHandler(idempotencyStore store.IdempotencyStore) Handler {
	return func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		if idempotencyStore == nil {
			return nil, errors.New("idempotent_record: idempotency store is nil")
		}
		if job == nil {
			return nil, errors.New("idempotent_record: job is nil")
		}
		if run == nil || run.ExecutionKey == "" {
			return nil, errors.New("idempotent_record: run has no execution key")
		}

		payloadRaw, err := json.Marshal(job.Payload)
		if err != nil {
			return nil, fmt.Errorf("idempotent_record: marshal payload: %w", err)
		}
		result, _, err := idempotencyStore.RunOnce(ctx, run.ExecutionKey, func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			if _, err := tx.Exec(ctx, `
				INSERT INTO idempotent_handler_effects (execution_key, payload)
				VALUES ($1, $2)
			`, run.ExecutionKey, payloadRaw); err != nil {
				return nil, fmt.Errorf("idempotent_record: persist effect: %w", err)
			}
			return map[string]any{
				"execution_key": run.ExecutionKey,
				"recorded":      true,
			}, nil
		})
		return result, err
	}
}
