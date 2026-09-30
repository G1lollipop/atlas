package lock

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresElectorCampaignContentionReleaseAndSessionLoss(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres elector integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open Postgres pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping Postgres: %v", err)
	}

	key := time.Now().UnixNano()
	first := newPostgresElectorWithOptions(pool, key, PostgresElectorOptions{
		RetryInterval: 10 * time.Millisecond, OperationTimeout: time.Second,
	})
	second := newPostgresElectorWithOptions(pool, key, PostgresElectorOptions{
		RetryInterval: 20 * time.Millisecond, OperationTimeout: time.Second,
	})
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		_ = first.Release(cleanupCtx)
		_ = second.Release(cleanupCtx)
	})

	if acquired, err := first.TryAcquire(ctx); err != nil || !acquired {
		t.Fatalf("first elector acquisition = %v, %v; want leadership", acquired, err)
	}
	if acquired, err := second.TryAcquire(ctx); err != nil || acquired {
		t.Fatalf("second elector acquisition = %v, %v; want contention", acquired, err)
	}
	second.mu.Lock()
	probeConnLeaked := second.conn != nil
	second.mu.Unlock()
	if probeConnLeaked {
		t.Fatal("nonleader retained its probe connection")
	}

	// Release must clear the session lock even after the caller's context expires.
	canceledReleaseCtx, cancelRelease := context.WithCancel(context.Background())
	cancelRelease()
	if err := first.Release(canceledReleaseCtx); err != nil {
		t.Fatalf("release with canceled context: %v", err)
	}
	if acquired, err := second.TryAcquire(ctx); err != nil || !acquired {
		t.Fatalf("second elector after release = %v, %v; want leadership", acquired, err)
	}

	// The contender's campaign should take over as soon as PostgreSQL drops the
	// leader's session and its advisory lock.
	campaignCtx, cancelCampaign := context.WithCancel(ctx)
	campaignDone := make(chan error, 1)
	go func() { campaignDone <- first.Campaign(campaignCtx) }()
	if !waitForElectorState(t, ctx, func() bool {
		first.mu.Lock()
		defer first.mu.Unlock()
		return first.campaignActive
	}) {
		t.Fatal("first campaign did not start")
	}
	time.Sleep(30 * time.Millisecond)
	second.mu.Lock()
	if second.conn == nil || !second.isLeader {
		second.mu.Unlock()
		t.Fatal("second elector lost its held connection before session-loss test")
	}
	pid := int32(second.conn.Conn().PgConn().PID())
	second.mu.Unlock()
	var terminated bool
	if err := pool.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate leader session pid %d = %v, %v", pid, terminated, err)
	}
	if !waitForElectorState(t, ctx, first.IsLeader) {
		t.Fatal("contender did not take leadership after the held PostgreSQL session died")
	}
	if err := second.Release(ctx); err != nil {
		// The old session is already gone; Release still must clear its local state
		// and destroy its connection rather than returning it to the pool.
		t.Logf("release reported the terminated session: %v", err)
	}
	if second.IsLeader() {
		t.Fatal("session loss left the former leader marked as leader")
	}

	cancelCampaign()
	select {
	case err := <-campaignDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("campaign result after cancel = %v, want context.Canceled", err)
		}
	case <-ctx.Done():
		t.Fatal("campaign did not stop after its context was canceled")
	}
	if first.IsLeader() {
		t.Fatal("campaign cancellation did not release leadership")
	}
}

func waitForElectorState(t *testing.T, ctx context.Context, state func() bool) bool {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if state() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}
