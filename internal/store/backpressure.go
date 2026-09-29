package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const (
	DefaultQueue                  = "cpu-default"
	DefaultTenant                 = "legacy"
	MaxQueueIdentifierBytes       = 128
	DefaultMaxQueueDepth          = 10000
	DefaultPerQueueLimit          = 2500
	DefaultPerTenantLimit         = 1000
	queueAdmissionLockKey   int64 = 727433003
)

// ErrQueueCapacityExceeded means accepting one more unit of work would exceed one
// of the configured scheduler backlog limits. The API maps this to HTTP 429.
var ErrQueueCapacityExceeded = errors.New("store: queue capacity exceeded")
var ErrInvalidQueue = errors.New("store: invalid queue name")
var ErrInvalidTenant = errors.New("store: invalid tenant id")

// QueueLimits apply to accepted work across every API and scheduler replica. A zero
// value disables that individual limit; the defaults are nonzero in config.Load.
type QueueLimits struct {
	MaxQueueDepth  int
	PerQueueLimit  int
	PerTenantLimit int
}

// DefaultQueueLimits matches config.Load's defaults. Direct-store callers therefore
// receive backpressure even when they do not explicitly configure the API/scheduler.
func DefaultQueueLimits() QueueLimits {
	return QueueLimits{
		MaxQueueDepth:  DefaultMaxQueueDepth,
		PerQueueLimit:  DefaultPerQueueLimit,
		PerTenantLimit: DefaultPerTenantLimit,
	}
}

// Validate rejects invalid negative limits. Zero intentionally disables a dimension.
func (limits QueueLimits) Validate() error {
	if limits.MaxQueueDepth < 0 || limits.PerQueueLimit < 0 || limits.PerTenantLimit < 0 {
		return fmt.Errorf("queue limits must not be negative")
	}
	return nil
}

// QueueCapacityError identifies the limit that rejected an admission. Callers may
// use errors.Is(err, ErrQueueCapacityExceeded) without depending on this detail.
type QueueCapacityError struct {
	Dimension string
	Current   int64
	Limit     int64
}

func (e *QueueCapacityError) Error() string {
	return fmt.Sprintf("%v: %s has %d outstanding units (limit %d)", ErrQueueCapacityExceeded, e.Dimension, e.Current, e.Limit)
}

func (e *QueueCapacityError) Is(target error) bool {
	return target == ErrQueueCapacityExceeded
}

// QueueBacklog is the global, queue-local, and tenant-local view used for one
// atomic admission decision. It includes running/assigned work and one-shot jobs
// accepted by the API but not yet promoted to a job_run.
type QueueBacklog struct {
	Total  int64
	Queue  int64
	Tenant int64
}

// NormalizeQueue returns the default class for omitted queue names and trims input.
func NormalizeQueue(queue string) string {
	if queue = strings.TrimSpace(queue); queue != "" {
		return queue
	}
	return DefaultQueue
}

// NormalizeTenant returns the legacy tenant for direct-store and pre-tenant callers.
func NormalizeTenant(tenant string) string {
	if tenant = strings.TrimSpace(tenant); tenant != "" {
		return tenant
	}
	return DefaultTenant
}

// ValidateQueue and ValidateTenant bound indexed identifiers by UTF-8 byte length
// so caller-supplied names stay within PostgreSQL's B-tree index tuple limit.
func ValidateQueue(queue string) error {
	if len(NormalizeQueue(queue)) > MaxQueueIdentifierBytes {
		return fmt.Errorf("%w: maximum length is %d bytes", ErrInvalidQueue, MaxQueueIdentifierBytes)
	}
	return nil
}

func ValidateTenant(tenant string) error {
	if len(NormalizeTenant(tenant)) > MaxQueueIdentifierBytes {
		return fmt.Errorf("%w: maximum length is %d bytes", ErrInvalidTenant, MaxQueueIdentifierBytes)
	}
	return nil
}

// lockQueueAdmissions serializes only the small admission transaction across API
// and scheduler processes. It is intentionally distinct from migration/leader locks.
func lockQueueAdmissions(ctx context.Context, tx querier) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, queueAdmissionLockKey)
	if err != nil {
		return fmt.Errorf("lock queue admission: %w", err)
	}
	return nil
}

// queueBacklog reads the same accounting used by admission. Its caller must hold
// queueAdmissionLockKey when using the result to make a write decision.
func queueBacklog(ctx context.Context, tx querier, queue, tenant string) (QueueBacklog, error) {
	var backlog QueueBacklog
	err := tx.QueryRow(ctx, `
		WITH outstanding AS (
			SELECT j.queue, j.tenant_id, count(*)::bigint AS units
			FROM job_runs r
			JOIN jobs j ON j.id = r.job_id
			WHERE r.status IN ('queued', 'scheduled', 'assigned', 'leased', 'running')
			GROUP BY j.queue, j.tenant_id
			UNION ALL
			SELECT j.queue, j.tenant_id, 1::bigint AS units
			FROM jobs j
			WHERE j.status IN ('active', 'paused')
			  AND j.cron_expr IS NULL
			  AND NOT EXISTS (SELECT 1 FROM job_runs r WHERE r.job_id = j.id)
		)
		SELECT COALESCE(sum(units), 0)::bigint,
		       COALESCE(sum(units) FILTER (WHERE queue = $1), 0)::bigint,
		       COALESCE(sum(units) FILTER (WHERE tenant_id = $2), 0)::bigint
		FROM outstanding
	`, NormalizeQueue(queue), NormalizeTenant(tenant)).Scan(&backlog.Total, &backlog.Queue, &backlog.Tenant)
	if err != nil {
		return QueueBacklog{}, fmt.Errorf("count queue backlog: %w", err)
	}
	return backlog, nil
}

// admitQueueWork must run before the transaction inserts a new job or run. If
// reservationHeld is true, a one-shot job already occupies one slot and promotion
// only replaces that reservation with its first active run, so it consumes no more
// capacity. Every counter write that can add active work must take the same lock.
func admitQueueWork(
	ctx context.Context,
	tx querier,
	limits QueueLimits,
	queue, tenant string,
	reservationHeld bool,
) error {
	if err := lockQueueAdmissions(ctx, tx); err != nil {
		return err
	}
	return checkQueueAdmissionCapacity(ctx, tx, limits, queue, tenant, reservationHeld)
}

// checkQueueAdmissionCapacity must be called with queueAdmissionLockKey already
// held by the transaction. It is split out for CreateJob's idempotency check.
func checkQueueAdmissionCapacity(
	ctx context.Context,
	tx querier,
	limits QueueLimits,
	queue, tenant string,
	reservationHeld bool,
) error {
	if reservationHeld {
		return nil
	}

	backlog, err := queueBacklog(ctx, tx, queue, tenant)
	if err != nil {
		return err
	}
	if limits.MaxQueueDepth > 0 && backlog.Total+1 > int64(limits.MaxQueueDepth) {
		return &QueueCapacityError{Dimension: "global backlog", Current: backlog.Total, Limit: int64(limits.MaxQueueDepth)}
	}
	if limits.PerQueueLimit > 0 && backlog.Queue+1 > int64(limits.PerQueueLimit) {
		return &QueueCapacityError{Dimension: fmt.Sprintf("queue %q", NormalizeQueue(queue)), Current: backlog.Queue, Limit: int64(limits.PerQueueLimit)}
	}
	if limits.PerTenantLimit > 0 && backlog.Tenant+1 > int64(limits.PerTenantLimit) {
		return &QueueCapacityError{Dimension: fmt.Sprintf("tenant %q", NormalizeTenant(tenant)), Current: backlog.Tenant, Limit: int64(limits.PerTenantLimit)}
	}
	return nil
}
