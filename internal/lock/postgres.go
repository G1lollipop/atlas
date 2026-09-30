package lock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultRetryInterval    = 500 * time.Millisecond
	defaultOperationTimeout = 3 * time.Second
)

var errCampaignAlreadyRunning = errors.New("lock: campaign is already running")

// postgresElector holds a dedicated connection only while it owns the session
// advisory lock. A nonleader releases its probe connection immediately, so
// contenders do not consume pool capacity while waiting.
type postgresElector struct {
	pool             *pgxpool.Pool
	lockKey          int64
	retryInterval    time.Duration
	operationTimeout time.Duration

	mu             sync.Mutex
	conn           *pgxpool.Conn
	isLeader       bool
	campaignActive bool
	campaignCancel context.CancelFunc
}

func newPostgresElectorWithOptions(pool *pgxpool.Pool, lockKey int64, options PostgresElectorOptions) *postgresElector {
	if options.RetryInterval <= 0 {
		options.RetryInterval = defaultRetryInterval
	}
	if options.OperationTimeout <= 0 {
		options.OperationTimeout = defaultOperationTimeout
	}
	return &postgresElector{
		pool:             pool,
		lockKey:          lockKey,
		retryInterval:    options.RetryInterval,
		operationTimeout: options.OperationTimeout,
	}
}

// TryAcquire performs one bounded attempt. The returned connection stays pinned
// only when the advisory lock was acquired; losing or probing the lock never
// strands a pool connection.
func (e *postgresElector) TryAcquire(ctx context.Context) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.pool == nil {
		return false, errors.New("lock: Postgres pool is nil")
	}
	if e.conn != nil && e.isLeader {
		if err := e.conn.Ping(ctx); err == nil {
			return true, nil
		} else {
			// Destroy a suspect session instead of returning it to the pool: it may
			// still hold the advisory lock if the ping failed during a network split.
			discardConn(e.conn)
			e.conn = nil
			e.isLeader = false
		}
	}
	if e.conn != nil {
		e.conn.Release()
		e.conn = nil
		e.isLeader = false
	}

	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", e.lockKey).Scan(&acquired); err != nil {
		discardConn(conn)
		return false, err
	}
	if !acquired {
		conn.Release()
		return false, nil
	}
	e.conn = conn
	e.isLeader = true
	return true, nil
}

// Campaign retries ordinary contention until ctx ends. Each database operation
// has its own timeout, and the held session is pinged on each retry interval so a
// dropped database connection clears leadership promptly.
func (e *postgresElector) Campaign(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	campaignCtx, cancelCampaign := context.WithCancel(ctx)
	e.mu.Lock()
	if e.campaignActive {
		e.mu.Unlock()
		cancelCampaign()
		return errCampaignAlreadyRunning
	}
	e.campaignActive = true
	e.campaignCancel = cancelCampaign
	e.mu.Unlock()
	defer func() {
		cancelCampaign()
		e.mu.Lock()
		e.campaignActive = false
		e.campaignCancel = nil
		e.mu.Unlock()
	}()

	ticker := time.NewTicker(e.retryInterval)
	defer ticker.Stop()
	for {
		if err := campaignCtx.Err(); err != nil {
			return e.releaseAfterCampaign(campaignCtx, err)
		}
		attemptCtx, cancel := context.WithTimeout(campaignCtx, e.operationTimeout)
		_, err := e.TryAcquire(attemptCtx)
		cancel()
		if err != nil {
			if campaignCtx.Err() != nil {
				return e.releaseAfterCampaign(campaignCtx, campaignCtx.Err())
			}
			return e.releaseAfterCampaign(campaignCtx, fmt.Errorf("lock: Postgres leadership campaign: %w", err))
		}
		select {
		case <-campaignCtx.Done():
			return e.releaseAfterCampaign(campaignCtx, campaignCtx.Err())
		case <-ticker.C:
		}
	}
}

func (e *postgresElector) releaseAfterCampaign(ctx context.Context, campaignErr error) error {
	if err := e.Release(ctx); err != nil {
		return errors.Join(campaignErr, fmt.Errorf("lock: release Postgres leadership campaign: %w", err))
	}
	return campaignErr
}

func (e *postgresElector) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.isLeader
}

// Release gives up leadership and always disposes of the held session. If the
// caller's context has already expired, it makes a short independent unlock
// attempt; if unlock cannot be confirmed, it closes the physical connection so
// PostgreSQL drops the session lock rather than returning it to the pool.
func (e *postgresElector) Release(ctx context.Context) error {
	e.mu.Lock()
	if e.campaignCancel != nil {
		e.campaignCancel()
	}
	conn := e.conn
	leader := e.isLeader
	e.conn = nil
	e.isLeader = false
	e.mu.Unlock()

	if conn == nil {
		return nil
	}
	if !leader {
		conn.Release()
		return nil
	}

	unlockCtx, cancel := e.releaseContext(ctx)
	defer cancel()
	var unlocked bool
	err := conn.QueryRow(unlockCtx, "SELECT pg_advisory_unlock($1)", e.lockKey).Scan(&unlocked)
	if err != nil {
		discardConn(conn)
		return fmt.Errorf("lock: unlock Postgres advisory lock: %w", err)
	}
	if !unlocked {
		discardConn(conn)
		return errors.New("lock: Postgres session did not confirm advisory unlock")
	}
	conn.Release()
	return nil
}

func (e *postgresElector) releaseContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil || ctx.Err() != nil {
		return context.WithTimeout(context.Background(), e.operationTimeout)
	}
	return context.WithTimeout(ctx, e.operationTimeout)
}

func discardConn(conn *pgxpool.Conn) {
	if conn == nil {
		return
	}
	physical := conn.Hijack()
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = physical.Close(closeCtx)
}

var _ LeaderElector = (*postgresElector)(nil)
