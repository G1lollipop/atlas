package store

import (
	"context"
	"fmt"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

// ObservabilitySnapshot reads the current queue backlog and worker reservations
// from the primary database. It runs two read-only queries per metrics scrape:
// one queue aggregation and one worker/reservation aggregation. The queue query
// emits queue rows only for current backlog, with a queue-empty cpu/gpu zero
// sentinel when a class has no backlog; the worker query emits seven series per
// worker seen within ten heartbeat TTLs so old worker IDs age out.
func (s *PostgresStore) ObservabilitySnapshot(ctx context.Context, heartbeatTTL time.Duration) (model.ObservabilitySnapshot, error) {
	if heartbeatTTL <= 0 {
		return model.ObservabilitySnapshot{}, fmt.Errorf("heartbeat TTL must be positive")
	}

	snapshot := model.ObservabilitySnapshot{
		QueueDepths: make([]model.QueueDepthSnapshot, 0),
		Workers:     make([]model.WorkerReservationSnapshot, 0),
	}

	queueRows, err := s.pool.Query(ctx, `
		WITH backlog AS (
			-- Materialized work remains in the backlog while queued, scheduled, or
			-- assigned, even if its parent job is paused or archived. Cancellation
			-- updates the run state or sets cancel_requested_at.
			SELECT j.queue,
			       CASE WHEN j.required_gpu_count > 0 THEN 'gpu' ELSE 'cpu' END AS resource_class,
			       count(*)::bigint AS depth
			FROM job_runs r
			JOIN jobs j ON j.id = r.job_id
			WHERE r.status IN ('queued', 'scheduled', 'assigned')
			  AND r.cancel_requested_at IS NULL
			  AND r.scheduled_at <= now()
			GROUP BY j.queue, CASE WHEN j.required_gpu_count > 0 THEN 'gpu' ELSE 'cpu' END

			UNION ALL

			SELECT j.queue,
			       CASE WHEN j.required_gpu_count > 0 THEN 'gpu' ELSE 'cpu' END AS resource_class,
			       count(*)::bigint AS depth
			FROM jobs j
			WHERE j.status = 'active'
			  AND j.cron_expr IS NULL
			  AND NOT EXISTS (SELECT 1 FROM job_runs r WHERE r.job_id = j.id)
			GROUP BY j.queue, CASE WHEN j.required_gpu_count > 0 THEN 'gpu' ELSE 'cpu' END
		),
		backlog_by_class AS (
			SELECT queue, resource_class, sum(depth)::bigint AS depth
			FROM backlog
			GROUP BY queue, resource_class
		),
		queue_classes AS (
			SELECT queue, resource_class FROM backlog_by_class
			UNION ALL
			SELECT ''::text AS queue, c.resource_class
			FROM (VALUES ('cpu'::text), ('gpu'::text)) AS c(resource_class)
			WHERE NOT EXISTS (
				SELECT 1 FROM backlog_by_class b WHERE b.resource_class = c.resource_class
			)
		)
		SELECT q.queue, q.resource_class, COALESCE(b.depth, 0)::bigint
		FROM queue_classes q
		LEFT JOIN backlog_by_class b
		  ON b.queue = q.queue AND b.resource_class = q.resource_class
		ORDER BY q.queue, q.resource_class
	`)
	if err != nil {
		return model.ObservabilitySnapshot{}, fmt.Errorf("query observability queue depth: %w", err)
	}
	for queueRows.Next() {
		var depth model.QueueDepthSnapshot
		if err := queueRows.Scan(&depth.Queue, &depth.ResourceClass, &depth.Depth); err != nil {
			queueRows.Close()
			return model.ObservabilitySnapshot{}, fmt.Errorf("scan observability queue depth: %w", err)
		}
		snapshot.QueueDepths = append(snapshot.QueueDepths, depth)
	}
	if err := queueRows.Err(); err != nil {
		queueRows.Close()
		return model.ObservabilitySnapshot{}, fmt.Errorf("read observability queue depth: %w", err)
	}
	queueRows.Close()

	heartbeatTTLMillis := durationMilliseconds(heartbeatTTL)
	workerRetentionMillis := heartbeatTTLMillis * 10
	workerRows, err := s.pool.Query(ctx, `
		SELECT w.id,
		       (w.status = 'alive' AND
		       w.last_heartbeat_at > now() - ($1::bigint * INTERVAL '1 millisecond')) AS alive,
		       w.cpu_capacity::bigint,
		       w.memory_capacity_mb::bigint,
		       w.gpu_count::bigint,
		       COALESCE(res.cpu_reserved, 0)::bigint,
		       COALESCE(res.memory_reserved, 0)::bigint,
		       COALESCE(res.gpu_reserved, 0)::bigint
		FROM workers w
		LEFT JOIN LATERAL (
			SELECT SUM(j.required_cpu_millis)::bigint AS cpu_reserved,
			       SUM(j.required_memory_mb)::bigint AS memory_reserved,
			       SUM(j.required_gpu_count)::bigint AS gpu_reserved
			FROM job_runs r
			JOIN jobs j ON j.id = r.job_id
			WHERE w.status = 'alive'
			AND w.last_heartbeat_at > now() - ($1::bigint * INTERVAL '1 millisecond')
			  -- These are scheduler reservations until a reclaimer changes the row's status.
			  AND ((r.status = 'assigned' AND r.assigned_worker_id = w.id)
			    OR (r.status IN ('leased', 'running') AND r.leased_by = w.id))
		) res ON TRUE
		WHERE w.last_heartbeat_at > now() - ($2::bigint * INTERVAL '1 millisecond')
		ORDER BY w.id
	`, heartbeatTTLMillis, workerRetentionMillis)
	if err != nil {
		return model.ObservabilitySnapshot{}, fmt.Errorf("query observability worker reservations: %w", err)
	}
	for workerRows.Next() {
		var worker model.WorkerReservationSnapshot
		if err := workerRows.Scan(&worker.WorkerID, &worker.Alive,
			&worker.CPUCapacityMillis, &worker.MemoryCapacityMB, &worker.GPUCapacity,
			&worker.CPUReservedMillis, &worker.MemoryReservedMB, &worker.GPUReservedCount); err != nil {
			workerRows.Close()
			return model.ObservabilitySnapshot{}, fmt.Errorf("scan observability worker reservation: %w", err)
		}
		if !worker.Alive {
			worker.CPUReservedMillis = 0
			worker.MemoryReservedMB = 0
			worker.GPUReservedCount = 0
		}
		snapshot.Workers = append(snapshot.Workers, worker)
	}
	if err := workerRows.Err(); err != nil {
		workerRows.Close()
		return model.ObservabilitySnapshot{}, fmt.Errorf("read observability worker reservations: %w", err)
	}
	workerRows.Close()

	return snapshot, nil
}
