// Package lock provides leader election so exactly one scheduler replica promotes
// DAG-ready jobs into runs at a time (multiple replicas may run for availability,
// but promotion must not double-fire). Backed by Postgres advisory locks rather than
// a separate consensus system (etcd/ZooKeeper): the scheduler already depends on
// Postgres as its system of record, so this avoids introducing a second failure domain
// purely for leader election. An etcd lease alternative would also need fencing tokens
// checked by store writes; lease expiry alone cannot prevent a paused former leader
// from writing after another replica takes over. Trade-off discussed in docs/ARCHITECTURE.md.
package lock

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Elector is the compatibility contract for callers that make one election attempt
// at a time. New scheduler code should use LeaderElector.Campaign.
type Elector interface {
	// TryAcquire attempts to become (or remain) leader. It is safe to call repeatedly
	// on a poll loop; a session-scoped advisory lock is held for as long as the
	// underlying connection is alive, and released automatically if the process dies.
	TryAcquire(ctx context.Context) (bool, error)
	// Release gives up leadership immediately (graceful shutdown).
	Release(ctx context.Context) error
}

// LeaderElector runs a bounded, context-owned leadership campaign and exposes the
// current campaign state. Campaign returns when its context is canceled or when a
// backend operation fails; it retries ordinary contention internally.
type LeaderElector interface {
	Elector
	Campaign(ctx context.Context) error
	IsLeader() bool
}

// PostgresElectorOptions bounds database calls and controls how often a campaign
// retries contention or verifies its held session.
type PostgresElectorOptions struct {
	RetryInterval    time.Duration
	OperationTimeout time.Duration
}

// NewPostgresElector returns a LeaderElector using pg_try_advisory_lock(lockKey) on a
// dedicated held connection from pool.
func NewPostgresElector(pool *pgxpool.Pool, lockKey int64) LeaderElector {
	return NewPostgresElectorWithOptions(pool, lockKey, PostgresElectorOptions{})
}

// NewPostgresElectorWithOptions returns a Postgres advisory-lock elector with
// explicit campaign polling and database-call bounds. Nonpositive options use defaults.
func NewPostgresElectorWithOptions(pool *pgxpool.Pool, lockKey int64, options PostgresElectorOptions) LeaderElector {
	return newPostgresElectorWithOptions(pool, lockKey, options)
}
