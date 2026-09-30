package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/api"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/scheduler"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/G1lollipop/atlas/internal/tracing"
	"github.com/G1lollipop/atlas/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

var (
	integrationTracerOnce sync.Once
	integrationRecorder   *tracetest.SpanRecorder
	integrationProvider   *sdktrace.TracerProvider
	integrationSetupErr   error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if integrationProvider != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := integrationProvider.Shutdown(shutdownCtx); err != nil && code == 0 {
			fmt.Fprintln(os.Stderr, "shut down test tracer provider:", err)
			code = 1
		}
		cancel()
	}
	os.Exit(code)
}

func TestHTTPJobTraceFlowsThroughSchedulerAndWorker(t *testing.T) {
	databaseURL := os.Getenv("ATLAS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set ATLAS_TEST_DATABASE_URL to run the Postgres job tracing integration test")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	tracer, recorder := setupIntegrationTracer(t)
	st := newIsolatedStore(t, ctx, databaseURL)

	requestCtx, requestRoot := tracer.Start(ctx, "test.request.root")
	requestRootSC := requestRoot.SpanContext()
	requestCarrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(requestCtx, requestCarrier)

	const jwtSecret = "job-trace-integration-secret"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := otelhttp.NewHandler(api.NewRouter(st, logger, jwtSecret, 10000, 10000), "atlas-api")
	token, err := api.MintToken(jwtSecret, "trace-integration", time.Hour)
	if err != nil {
		t.Fatalf("mint test token: %v", err)
	}
	jobName := "trace-flow-" + uuid.NewString()
	body, err := json.Marshal(map[string]any{
		"name":         jobName,
		"max_attempts": 2,
		// These body fields are deliberately forged. The Store must use only the
		// trusted active OpenTelemetry context supplied by otelhttp.
		"traceparent": "00-11111111111111111111111111111111-2222222222222222-01",
		"tracestate":  "forged=value",
	})
	if err != nil {
		t.Fatalf("marshal create request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(string(body)))
	req = req.WithContext(requestCtx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("traceparent", requestCarrier.Get("traceparent"))
	req.Header.Set("tracestate", "tracevendor=trusted")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	requestRoot.End()
	if response.Code != http.StatusCreated {
		t.Fatalf("POST /v1/jobs status = %d, body: %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), `"traceparent"`) || strings.Contains(response.Body.String(), `"tracestate"`) {
		t.Fatalf("API response exposed internal trace context: %s", response.Body.String())
	}
	var created model.Job
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode created job: %v", err)
	}
	job, err := st.GetJob(ctx, created.ID)
	if err != nil {
		t.Fatalf("read created job: %v", err)
	}
	jobSC := storedSpanContext(job.TraceParent, job.TraceState)
	if !jobSC.IsValid() {
		t.Fatal("job did not persist a valid request SpanContext")
	}
	if jobSC.TraceID() != requestRootSC.TraceID() {
		t.Fatalf("job trace ID = %s, want HTTP request trace %s", jobSC.TraceID(), requestRootSC.TraceID())
	}
	if jobSC.SpanID() == requestRootSC.SpanID() {
		t.Fatal("job persisted the external request parent instead of the active API server span")
	}
	if !strings.Contains(job.TraceState, "tracevendor=trusted") || strings.Contains(job.TraceState, "forged") {
		t.Fatalf("job tracestate = %q, want trusted header and no forged body value", job.TraceState)
	}

	schedulerPollCtx, schedulerPollSpan := tracer.Start(ctx, "test.scheduler.poll")
	promoter := scheduler.NewPromoter(st, nil, logger, time.Second)
	promoted, err := promoter.PromoteOnce(schedulerPollCtx)
	schedulerPollSpan.End()
	if err != nil {
		t.Fatalf("promote job: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("PromoteOnce promoted %d jobs, want 1", promoted)
	}
	runs, err := st.ListJobRuns(ctx, created.ID, 5)
	if err != nil || len(runs) != 1 {
		t.Fatalf("read promoted run: len=%d err=%v", len(runs), err)
	}
	runID := runs[0].ID

	if err := st.UpsertWorkerHeartbeat(ctx, model.Worker{
		ID: "trace-worker-" + uuid.NewString(), Hostname: "trace-test", Status: model.WorkerStatusAlive,
		CPUCapacity: 2, MemoryCapacityMB: 2048, StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	workers, err := st.ListWorkers(ctx)
	if err != nil || len(workers) != 1 {
		t.Fatalf("list integration worker: len=%d err=%v", len(workers), err)
	}
	workerID := workers[0].ID

	dispatchCtx, dispatchPoll := tracer.Start(ctx, "test.dispatch.poll")
	assigned, err := promoter.DispatchOnce(dispatchCtx)
	dispatchPoll.End()
	if err != nil {
		t.Fatalf("dispatch promoted run: %v", err)
	}
	if assigned != 1 {
		t.Fatalf("DispatchOnce assigned %d runs, want 1", assigned)
	}

	pool := worker.NewPool(st, workerID, 1, 20*time.Second, 10*time.Millisecond, logger)
	pool.SetCapabilities(model.Worker{CPUCapacity: 2, MemoryCapacityMB: 2048})
	handlerFinished := make(chan struct{})
	pool.RegisterHandler(jobName, func(handlerCtx context.Context, _ *model.Job, _ *model.JobRun) (map[string]any, error) {
		tp, ts := tracing.TraceContext(handlerCtx)
		spanContext := trace.SpanContextFromContext(handlerCtx)
		close(handlerFinished)
		return map[string]any{
			"handler_traceparent": tp,
			"handler_tracestate":  ts,
			"handler_trace_id":    spanContext.TraceID().String(),
		}, nil
	})

	workerPollCtx, workerPollSpan := tracer.Start(ctx, "test.worker.poll")
	workerCtx, stopWorker := context.WithCancel(workerPollCtx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- pool.Run(workerCtx) }()
	select {
	case <-handlerFinished:
	case <-ctx.Done():
		stopWorker()
		workerPollSpan.End()
		t.Fatalf("handler did not start: %v", ctx.Err())
	}

	// Handler completion precedes the durable result write, so wait for the
	// database terminal state before checking persisted trace context.
	var completed *model.JobRun
	for {
		completed, err = st.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("read run after handler: %v", err)
		}
		if completed.Status == model.RunStatusSucceeded {
			break
		}
		select {
		case <-ctx.Done():
			stopWorker()
			workerPollSpan.End()
			t.Fatalf("run did not complete: status=%s, error=%v", completed.Status, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	stopWorker()
	if err := <-workerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("worker pool returned %v, want context.Canceled", err)
	}
	workerPollSpan.End()

	workers, err = st.ListWorkers(ctx)
	if err != nil || len(workers) != 1 || workers[0].ID != workerID {
		t.Fatalf("list stopped integration worker: workers=%v err=%v", workers, err)
	}
	if workers[0].Status != model.WorkerStatusDraining {
		t.Fatalf("stopped worker status = %q, want draining", workers[0].Status)
	}

	if completed.Result["handler_trace_id"] != requestRootSC.TraceID().String() {
		t.Fatalf("handler trace ID = %v, want %s", completed.Result["handler_trace_id"], requestRootSC.TraceID())
	}
	resultParent := storedSpanContext(completed.TraceParent, completed.TraceState)
	if !resultParent.IsValid() || resultParent.TraceID() != requestRootSC.TraceID() {
		t.Fatalf("completed run trace context = %v, want a valid child of request trace %s", resultParent, requestRootSC.TraceID())
	}
	if completed.Result["handler_traceparent"] == nil {
		t.Fatal("handler did not receive and return its active trace context")
	}

	spans := makeSpanIndex(recorder.Ended(), requestRootSC.TraceID())
	createJob := spans[jobSC.SpanID()]
	if createJob == nil || createJob.Name != "api.CreateJob" {
		t.Fatalf("recorded job creation span %s not found", jobSC.SpanID())
	}
	apiSpan := spans[createJob.Parent.SpanID()]
	if apiSpan == nil {
		t.Fatalf("API server span %s was not recorded", createJob.Parent.SpanID())
	}
	if apiSpan.Parent.SpanID() != requestRootSC.SpanID() {
		t.Fatalf("API server span parent = %s, want request root %s", apiSpan.Parent.SpanID(), requestRootSC.SpanID())
	}
	requireDatabaseInsert(t, spans, createJob.SpanContext)
	promoteJob := requireChild(t, spans, "scheduler.PromoteJob", jobSC)
	createRun := requireChild(t, spans, "scheduler.CreateRun", promoteJob.SpanContext)
	scheduleRun := requireChild(t, spans, "scheduler.ScheduleRun", createRun.SpanContext)
	assignRun := requireChild(t, spans, "scheduler.AssignRun", scheduleRun.SpanContext)
	leaseRun := requireChild(t, spans, "worker.LeaseRun", assignRun.SpanContext)
	execution := requireChild(t, spans, "worker.executeOne", leaseRun.SpanContext)
	handler := requireChild(t, spans, "worker.handler", execution.SpanContext)
	resultPersist := requireChild(t, spans, "worker.result.persist", execution.SpanContext)
	if resultParent.SpanID() != resultPersist.SpanContext.SpanID() {
		t.Fatalf("persisted run parent span = %s, want result persist span %s", resultParent.SpanID(), resultPersist.SpanContext.SpanID())
	}
	if got := storedSpanContext(completed.Result["handler_traceparent"].(string), completed.Result["handler_tracestate"].(string)); got.SpanID() != handler.SpanContext.SpanID() {
		t.Fatalf("handler received span %s, recorded handler span %s", got.SpanID(), handler.SpanContext.SpanID())
	}
	for _, span := range []*tracetest.SpanStub{promoteJob, createRun, scheduleRun, assignRun, leaseRun, execution, handler, resultPersist} {
		if span.SpanContext.TraceID() != requestRootSC.TraceID() {
			t.Fatalf("span %q trace ID = %s, want %s", span.Name, span.SpanContext.TraceID(), requestRootSC.TraceID())
		}
	}

	// A stopped worker remains draining so active leases can finish and the
	// scheduler cannot assign it new work. Use a fresh worker identity for the
	// retry lifecycle below rather than reactivating the stopped identity.
	retryWorkerID := "trace-retry-worker-" + uuid.NewString()
	if err := st.UpsertWorkerHeartbeat(ctx, model.Worker{
		ID: retryWorkerID, Hostname: "trace-retry-test", Status: model.WorkerStatusAlive,
		CPUCapacity: 2, MemoryCapacityMB: 2048, StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("register retry worker: %v", err)
	}
	verifyRetryReclaimAndDeadLetterTrace(t, ctx, st, tracer, recorder, retryWorkerID)
}

func setupIntegrationTracer(t *testing.T) (trace.Tracer, *tracetest.SpanRecorder) {
	t.Helper()
	integrationTracerOnce.Do(func() {
		integrationRecorder = tracetest.NewSpanRecorder()
		providerOptions := []sdktrace.TracerProviderOption{
			sdktrace.WithSpanProcessor(integrationRecorder),
			sdktrace.WithResource(resource.NewSchemaless()),
		}
		if endpoint := os.Getenv("ATLAS_TEST_OTLP_ENDPOINT"); endpoint != "" {
			setupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			exporter, err := otlptracehttp.New(setupCtx,
				otlptracehttp.WithEndpoint(endpoint),
				otlptracehttp.WithInsecure(),
			)
			if err != nil {
				integrationSetupErr = fmt.Errorf("create optional OTLP exporter: %w", err)
				return
			}
			providerOptions = append(providerOptions, sdktrace.WithBatcher(exporter))
		}
		integrationProvider = sdktrace.NewTracerProvider(providerOptions...)
		otel.SetTracerProvider(integrationProvider)
		otel.SetTextMapPropagator(propagation.TraceContext{})
	})
	if integrationSetupErr != nil {
		t.Fatal(integrationSetupErr)
	}
	return otel.Tracer("atlas/tracing-integration"), integrationRecorder
}

func newIsolatedStore(t *testing.T, ctx context.Context, databaseURL string) *store.PostgresStore {
	t.Helper()
	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	adminPool, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	schema := "atlas_trace_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		adminPool.Close()
		t.Fatalf("create isolated test schema: %v", err)
	}

	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
		t.Fatalf("parse isolated database URL: %v", err)
	}
	query := parsedURL.Query()
	query.Set("search_path", schema+",public")
	parsedURL.RawQuery = query.Encode()
	st, err := store.New(ctx, parsedURL.String())
	if err != nil {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
		t.Fatalf("open isolated store: %v", err)
	}
	if err := store.RunMigrations(ctx, st.Pool(), filepath.Join("..", "..", "..", "migrations")); err != nil {
		st.Close()
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
		t.Fatalf("run migrations in isolated schema: %v", err)
	}
	t.Cleanup(func() {
		st.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adminPool.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
	})
	return st
}

func storedSpanContext(traceparent, tracestate string) trace.SpanContext {
	ctx := tracing.ContextWithTraceContext(context.Background(), traceparent, tracestate)
	return trace.SpanContextFromContext(ctx)
}

func makeSpanIndex(ended []sdktrace.ReadOnlySpan, traceID trace.TraceID) map[trace.SpanID]*tracetest.SpanStub {
	spans := make(map[trace.SpanID]*tracetest.SpanStub, len(ended))
	for _, span := range ended {
		stub := tracetest.SpanStubFromReadOnlySpan(span)
		if stub.SpanContext.TraceID() != traceID {
			continue
		}
		spans[stub.SpanContext.SpanID()] = &stub
	}
	return spans
}

func requireChild(t *testing.T, spans map[trace.SpanID]*tracetest.SpanStub, name string, parent trace.SpanContext) *tracetest.SpanStub {
	t.Helper()
	var child *tracetest.SpanStub
	for _, span := range spans {
		if span.Name == name {
			if child != nil {
				t.Fatalf("found multiple %q spans", name)
			}
			child = span
		}
	}
	if child == nil {
		t.Fatalf("span %q was not recorded", name)
	}
	if child.Parent.SpanID() != parent.SpanID() {
		t.Fatalf("span %q parent = %s, want %s", name, child.Parent.SpanID(), parent.SpanID())
	}
	return child
}

func requireDatabaseInsert(t *testing.T, spans map[trace.SpanID]*tracetest.SpanStub, parent trace.SpanContext) {
	t.Helper()
	for _, span := range spans {
		if span.Name != "pg.query" || span.Parent.SpanID() != parent.SpanID() {
			continue
		}
		for _, attr := range span.Attributes {
			if attr.Key == "db.statement" && strings.Contains(strings.ToUpper(attr.Value.AsString()), "INSERT INTO JOBS") {
				return
			}
		}
	}
	t.Fatalf("no database INSERT INTO jobs span was recorded under %s", parent.SpanID())
}

func verifyRetryReclaimAndDeadLetterTrace(t *testing.T, ctx context.Context, st *store.PostgresStore, tracer trace.Tracer, recorder *tracetest.SpanRecorder, workerID string) {
	t.Helper()
	rootCtx, rootSpan := tracer.Start(ctx, "test.retry.root")
	rootSC := rootSpan.SpanContext()
	job, err := st.CreateJob(rootCtx, model.NewJobInput{
		Name: "trace-retry-" + uuid.NewString(), MaxAttempts: 5, TimeoutSeconds: 60,
	})
	rootSpan.End()
	if err != nil {
		t.Fatalf("create retry fixture job: %v", err)
	}
	run, err := st.CreateRun(ctx, job.ID, job.Priority, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("create retry fixture run: %v", err)
	}

	lease := func() *model.JobRun {
		t.Helper()
		if _, err := st.ScheduleDueRuns(ctx); err != nil {
			t.Fatalf("schedule retry fixture run: %v", err)
		}
		assigned, err := st.AssignRun(ctx, run.ID, workerID, 30*time.Second, 30*time.Second)
		if err != nil || !assigned {
			t.Fatalf("assign retry fixture run: assigned=%v err=%v", assigned, err)
		}
		leased, _, err := st.LeaseNextRun(ctx, workerID, 30*time.Second)
		if err != nil || leased == nil {
			t.Fatalf("lease retry fixture run: run=%v err=%v", leased, err)
		}
		return leased
	}

	firstLease := lease()
	firstLeaseSC := storedSpanContext(firstLease.TraceParent, firstLease.TraceState)
	if firstLeaseSC.TraceID() != rootSC.TraceID() {
		t.Fatalf("first retry lease trace ID = %s, want %s", firstLeaseSC.TraceID(), rootSC.TraceID())
	}
	if err := st.MarkRunning(tracing.ContextWithTraceContext(ctx, firstLease.TraceParent, firstLease.TraceState), run.ID, workerID, firstLease.Attempt); err != nil {
		t.Fatalf("mark first retry lease running: %v", err)
	}
	failureParent := tracing.ContextWithTraceContext(ctx, firstLease.TraceParent, firstLease.TraceState)
	retryPersistCtx, retryPersistSpan := tracer.Start(failureParent, "test.retry.persist")
	if err := st.FailRun(retryPersistCtx, run.ID, workerID, firstLease.Attempt, "retry test", true, 0); err != nil {
		t.Fatalf("requeue first attempt: %v", err)
	}
	retryPersistSC := retryPersistSpan.SpanContext()
	retryPersistSpan.End()
	afterRetry, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("read retried run: %v", err)
	}
	if got := storedSpanContext(afterRetry.TraceParent, afterRetry.TraceState); got.TraceID() != rootSC.TraceID() || got.SpanID() != retryPersistSC.SpanID() {
		t.Fatalf("retry persisted context = %v, want child %s in trace %s", got, retryPersistSC.SpanID(), rootSC.TraceID())
	}

	secondLease := lease()
	if secondLease.Attempt != 2 {
		t.Fatalf("second lease attempt = %d, want 2", secondLease.Attempt)
	}
	secondLeaseSC := storedSpanContext(secondLease.TraceParent, secondLease.TraceState)
	if err := st.MarkRunning(tracing.ContextWithTraceContext(ctx, secondLease.TraceParent, secondLease.TraceState), run.ID, workerID, secondLease.Attempt); err != nil {
		t.Fatalf("mark second retry lease running: %v", err)
	}
	// Direct store callers may have no active span. That must preserve the run's
	// durable context instead of clearing it during a retry transition.
	if err := st.FailRun(context.Background(), run.ID, workerID, secondLease.Attempt, "untraced retry test", true, 0); err != nil {
		t.Fatalf("requeue second attempt without active span: %v", err)
	}
	afterUntracedRetry, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("read run after untraced retry: %v", err)
	}
	untracedRetrySC := storedSpanContext(afterUntracedRetry.TraceParent, afterUntracedRetry.TraceState)
	if afterUntracedRetry.Status != model.RunStatusQueued || untracedRetrySC.TraceID() != rootSC.TraceID() || untracedRetrySC.SpanID() != secondLeaseSC.SpanID() {
		t.Fatalf("untraced retry status/context = %s/%v, want queued with prior lease parent %s", afterUntracedRetry.Status, untracedRetrySC, secondLeaseSC.SpanID())
	}

	thirdLease := lease()
	if thirdLease.Attempt != 3 {
		t.Fatalf("third lease attempt = %d, want 3", thirdLease.Attempt)
	}
	thirdLeaseSC := storedSpanContext(thirdLease.TraceParent, thirdLease.TraceState)
	if err := st.MarkRunning(tracing.ContextWithTraceContext(ctx, thirdLease.TraceParent, thirdLease.TraceState), run.ID, workerID, thirdLease.Attempt); err != nil {
		t.Fatalf("mark third retry lease running: %v", err)
	}
	if _, err := st.Pool().Exec(ctx, `UPDATE job_runs SET lease_expires_at = now() - INTERVAL '1 second' WHERE id = $1`, run.ID); err != nil {
		t.Fatalf("expire third lease for reclaim: %v", err)
	}
	reclaimed, err := st.ReclaimExpiredLeases(context.Background())
	if err != nil || reclaimed != 1 {
		t.Fatalf("reclaim third lease without active span: count=%d err=%v", reclaimed, err)
	}
	afterReclaim, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("read reclaimed run: %v", err)
	}
	reclaimSC := storedSpanContext(afterReclaim.TraceParent, afterReclaim.TraceState)
	if afterReclaim.Status != model.RunStatusQueued || reclaimSC.TraceID() != rootSC.TraceID() || reclaimSC.SpanID() == thirdLeaseSC.SpanID() {
		t.Fatalf("reclaimed status/context = %s/%v, want queued in trace %s", afterReclaim.Status, reclaimSC, rootSC.TraceID())
	}
	if reclaimSpan := findSpan(recorder.Ended(), "worker.ReclaimExpiredLease", reclaimSC.SpanID()); reclaimSpan == nil || reclaimSpan.Parent.SpanID() != thirdLeaseSC.SpanID() {
		t.Fatalf("reclaim span was not parented by the third lease span %s", thirdLeaseSC.SpanID())
	}

	fourthLease := lease()
	if fourthLease.Attempt != 4 {
		t.Fatalf("fourth lease attempt = %d, want 4", fourthLease.Attempt)
	}
	fourthCtx := tracing.ContextWithTraceContext(ctx, fourthLease.TraceParent, fourthLease.TraceState)
	if err := st.MarkRunning(fourthCtx, run.ID, workerID, fourthLease.Attempt); err != nil {
		t.Fatalf("mark fourth lease running: %v", err)
	}
	deadFailureCtx, deadFailureSpan := tracer.Start(fourthCtx, "test.dead_letter.failure.persist")
	if err := st.FailRun(deadFailureCtx, run.ID, workerID, fourthLease.Attempt, "dead-letter test", false, 0); err != nil {
		deadFailureSpan.End()
		t.Fatalf("fail fourth attempt: %v", err)
	}
	deadFailureSpan.End()
	if err := st.MarkDead(context.Background(), run.ID, "trace test"); err != nil {
		t.Fatalf("mark run dead: %v", err)
	}
	afterDead, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("read run after dead-lettering: %v", err)
	}
	deadSC := storedSpanContext(afterDead.TraceParent, afterDead.TraceState)
	if afterDead.Status != model.RunStatusDead || deadSC.TraceID() != rootSC.TraceID() {
		t.Fatalf("dead-letter status/context = %s/%v, want dead in trace %s", afterDead.Status, deadSC, rootSC.TraceID())
	}
	deadSpan := findSpan(recorder.Ended(), "worker.MarkDead", deadSC.SpanID())
	if deadSpan == nil || deadSpan.Parent.SpanID() != deadFailureSpan.SpanContext().SpanID() {
		t.Fatalf("dead-letter span was not parented by failure persistence span %s", deadFailureSpan.SpanContext().SpanID())
	}
	letters, err := st.ListDeadLetters(ctx, 10, 0)
	if err != nil || len(letters) != 1 {
		t.Fatalf("list trace fixture dead letters: len=%d err=%v", len(letters), err)
	}
	retried, err := st.RetryDeadLetter(ctx, letters[0].ID)
	if err != nil {
		t.Fatalf("retry trace fixture dead letter: %v", err)
	}
	finalSC := storedSpanContext(retried.TraceParent, retried.TraceState)
	if finalSC.TraceID() != rootSC.TraceID() {
		t.Fatalf("dead-letter retry trace ID = %s, want %s", finalSC.TraceID(), rootSC.TraceID())
	}
	if retrySpan := findSpan(recorder.Ended(), "scheduler.RetryDeadLetter", finalSC.SpanID()); retrySpan == nil {
		t.Fatalf("dead-letter retry span %s was not recorded", finalSC.SpanID())
	} else if retrySpan.Parent.SpanID() != deadSC.SpanID() {
		t.Fatalf("dead-letter retry parent = %s, want MarkDead span %s", retrySpan.Parent.SpanID(), deadSC.SpanID())
	}
}

func findSpan(spans []sdktrace.ReadOnlySpan, name string, spanID trace.SpanID) *tracetest.SpanStub {
	for _, span := range spans {
		stub := tracetest.SpanStubFromReadOnlySpan(span)
		if stub.Name == name && stub.SpanContext.SpanID() == spanID {
			return &stub
		}
	}
	return nil
}
