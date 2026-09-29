package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/G1lollipop/atlas/internal/model"
)

func TestConcurrentQueueAdmissionsRespectGlobalLimitAcrossPools(t *testing.T) {
	first, ctx := openQueueAdmissionTestStore(t)
	second := openQueueAdmissionPeer(t, ctx, first)
	limits := QueueLimits{MaxQueueDepth: 5, PerQueueLimit: 5, PerTenantLimit: 5}

	const submissions = 24
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	errs := make([]error, 0)
	for i := 0; i < submissions; i++ {
		st := first
		if i%2 == 1 {
			st = second
		}
		wg.Add(1)
		go func(index int, st *PostgresStore) {
			defer wg.Done()
			err := submitQueueReservation(ctx, st, limits, "cpu-default", "tenant-shared", fmt.Sprintf("concurrent-%d", index))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				accepted++
			} else if !errors.Is(err, ErrQueueCapacityExceeded) {
				errs = append(errs, err)
			}
		}(i, st)
	}
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("unexpected submission errors: %v", errs)
	}
	if accepted != limits.MaxQueueDepth {
		t.Fatalf("concurrent accepted reservations = %d, want exactly %d", accepted, limits.MaxQueueDepth)
	}
	backlog := readQueueBacklog(t, ctx, first, "cpu-default", "tenant-shared")
	if backlog.Total != 5 || backlog.Queue != 5 || backlog.Tenant != 5 {
		t.Fatalf("backlog after concurrent submissions = %+v, want 5/5/5", backlog)
	}
}

func TestQueueAndTenantIdentifiersHaveBoundedIndexedLength(t *testing.T) {
	if err := ValidateQueue(strings.Repeat("q", MaxQueueIdentifierBytes)); err != nil {
		t.Fatalf("queue at byte limit rejected: %v", err)
	}
	if err := ValidateTenant(strings.Repeat("t", MaxQueueIdentifierBytes)); err != nil {
		t.Fatalf("tenant at byte limit rejected: %v", err)
	}
	if err := ValidateQueue(strings.Repeat("q", MaxQueueIdentifierBytes+1)); !errors.Is(err, ErrInvalidQueue) {
		t.Fatalf("oversized queue validation error = %v, want ErrInvalidQueue", err)
	}
	if err := ValidateTenant(strings.Repeat("t", MaxQueueIdentifierBytes+1)); !errors.Is(err, ErrInvalidTenant) {
		t.Fatalf("oversized tenant validation error = %v, want ErrInvalidTenant", err)
	}
	st := &PostgresStore{}
	if _, err := st.CreateJob(context.Background(), model.NewJobInput{
		Name: "too-long-queue", Queue: strings.Repeat("q", MaxQueueIdentifierBytes+1),
	}); !errors.Is(err, ErrInvalidQueue) {
		t.Fatalf("direct CreateJob oversized queue error = %v, want ErrInvalidQueue", err)
	}
	if _, err := st.CreateJob(context.Background(), model.NewJobInput{
		Name: "too-long-tenant", TenantID: strings.Repeat("t", MaxQueueIdentifierBytes+1),
	}); !errors.Is(err, ErrInvalidTenant) {
		t.Fatalf("direct CreateJob oversized tenant error = %v, want ErrInvalidTenant", err)
	}
}

func TestQueueBackpressureSeparatesQueueAndTenantLimits(t *testing.T) {
	st, ctx := openQueueAdmissionTestStore(t)
	limits := QueueLimits{MaxQueueDepth: 20, PerQueueLimit: 2, PerTenantLimit: 2}

	for _, item := range []struct {
		name   string
		queue  string
		tenant string
		want   bool
	}{
		{name: "queue-a tenant-a first", queue: "queue-a", tenant: "tenant-a", want: true},
		{name: "queue-b tenant-a second", queue: "queue-b", tenant: "tenant-a", want: true},
		{name: "tenant-a reaches its limit across queues", queue: "queue-c", tenant: "tenant-a", want: false},
		{name: "queue-a accepts a second tenant", queue: "queue-a", tenant: "tenant-b", want: true},
		{name: "queue-a reaches its limit", queue: "queue-a", tenant: "tenant-c", want: false},
		{name: "queue-b accepts tenant-b", queue: "queue-b", tenant: "tenant-b", want: true},
		{name: "tenant-b reaches its limit across queues", queue: "queue-c", tenant: "tenant-b", want: false},
	} {
		t.Run(item.name, func(t *testing.T) {
			err := submitQueueReservation(ctx, st, limits, item.queue, item.tenant, item.name)
			if item.want && err != nil {
				t.Fatalf("submission rejected: %v", err)
			}
			if !item.want && !errors.Is(err, ErrQueueCapacityExceeded) {
				t.Fatalf("submission error = %v, want queue capacity exceeded", err)
			}
		})
	}
}

func TestQueueBackpressureReleasesCapacityWhenRunTerminates(t *testing.T) {
	limits := QueueLimits{MaxQueueDepth: 1, PerQueueLimit: 1, PerTenantLimit: 1}

	for _, terminalStatus := range []string{"canceled", "succeeded"} {
		t.Run(terminalStatus, func(t *testing.T) {
			st, ctx := openQueueAdmissionTestStore(t)
			tenant := "tenant-release-" + terminalStatus
			jobID := insertQueueRun(t, ctx, st, "release-"+terminalStatus, "cpu-default", tenant, "running")
			if err := submitQueueReservation(ctx, st, limits, "cpu-default", tenant, "must-be-full"); !errors.Is(err, ErrQueueCapacityExceeded) {
				t.Fatalf("submission while %s run is active returned %v, want capacity exceeded", terminalStatus, err)
			}
			if _, err := st.Pool().Exec(ctx, `UPDATE job_runs SET status = $1, finished_at = now() WHERE job_id = $2`, terminalStatus, jobID); err != nil {
				t.Fatalf("finish run as %s: %v", terminalStatus, err)
			}
			if err := submitQueueReservation(ctx, st, limits, "cpu-default", tenant, "after-"+terminalStatus); err != nil {
				t.Fatalf("capacity was not released after %s: %v", terminalStatus, err)
			}
		})
	}
}

func submitQueueReservation(ctx context.Context, st *PostgresStore, limits QueueLimits, queue, tenant, name string) error {
	tx, err := st.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := admitQueueWork(ctx, tx, limits, queue, tenant, false); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (name, queue, tenant_id) VALUES ($1, $2, $3)
	`, name, NormalizeQueue(queue), NormalizeTenant(tenant))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func insertQueueRun(t *testing.T, ctx context.Context, st *PostgresStore, name, queue, tenant, status string) string {
	t.Helper()
	var jobID string
	if err := st.Pool().QueryRow(ctx, `
		INSERT INTO jobs (name, queue, tenant_id) VALUES ($1, $2, $3) RETURNING id
	`, name, queue, tenant).Scan(&jobID); err != nil {
		t.Fatalf("insert queue test job: %v", err)
	}
	if _, err := st.Pool().Exec(ctx, `
		INSERT INTO job_runs (job_id, status, priority, scheduled_at)
		VALUES ($1, $2, 0, now())
	`, jobID, status); err != nil {
		t.Fatalf("insert queue test run: %v", err)
	}
	return jobID
}

func readQueueBacklog(t *testing.T, ctx context.Context, st *PostgresStore, queue, tenant string) QueueBacklog {
	t.Helper()
	tx, err := st.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	backlog, err := queueBacklog(ctx, tx, queue, tenant)
	if err != nil {
		t.Fatal(err)
	}
	return backlog
}

func openQueueAdmissionTestStore(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL not set; skipping queue admission integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	admin, err := New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	schema := "queue_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Pool().Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatalf("create isolated schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Pool().Exec(cleanupCtx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`); err != nil {
			t.Errorf("drop isolated schema: %v", err)
		}
		admin.Close()
	})

	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	query := parsedURL.Query()
	query.Set("search_path", schema)
	parsedURL.RawQuery = query.Encode()
	st, err := New(ctx, parsedURL.String())
	if err != nil {
		t.Fatalf("connect to isolated schema: %v", err)
	}
	t.Cleanup(st.Close)
	if err := RunMigrations(ctx, st.Pool(), "../../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return st, ctx
}

func openQueueAdmissionPeer(t *testing.T, ctx context.Context, primary *PostgresStore) *PostgresStore {
	t.Helper()
	var schema string
	if err := primary.Pool().QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("read isolated schema: %v", err)
	}
	databaseURL, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	query := databaseURL.Query()
	query.Set("search_path", schema)
	databaseURL.RawQuery = query.Encode()
	peer, err := New(ctx, databaseURL.String())
	if err != nil {
		t.Fatalf("connect peer store: %v", err)
	}
	t.Cleanup(peer.Close)
	return peer
}
