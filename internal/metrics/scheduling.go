package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsCompleted counts successful job runs.
	JobsCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "atlas_jobs_completed_total",
		Help: "Total number of job runs completed successfully.",
	})

	// JobsFailed counts failed execution attempts, including attempts that are retried.
	JobsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "atlas_jobs_failed_total",
		Help: "Total number of job execution attempts that failed.",
	})

	JobQueueWaitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "atlas_job_queue_wait_seconds",
		Help:    "Time from a one-shot job's acceptance or a run's due time until a worker starts it, by resource class; retry backoff is excluded.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
	}, []string{"resource_class"})

	JobExecutionSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "atlas_job_execution_seconds",
		Help:    "Time spent executing a job handler.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900},
	})

	SchedulerDecisionSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "atlas_scheduler_decision_seconds",
		Help:    "Time spent promoting and dispatching work in one scheduler decision cycle.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})

	RetryTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "atlas_retry_total",
		Help: "Total number of failed attempts successfully returned to the retry queue.",
	})

	DeadLetterTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "atlas_dead_letter_total",
		Help: "Total number of runs successfully moved to the dead-letter state.",
	})

	LeaseExpiredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "atlas_lease_expired_total",
		Help: "Total number of expired worker leases successfully reclaimed.",
	})
)

// ResourceClass maps a workload's declared GPU requirement to the bounded queue
// metric label shared by the store snapshot and worker instrumentation.
func ResourceClass(requiredGPUCount int32) string {
	if requiredGPUCount > 0 {
		return "gpu"
	}
	return "cpu"
}

// QueueWaitDurationSeconds measures first-attempt one-shot work from API
// acceptance. Recurring runs and retries start at runScheduledAt so retry
// backoff does not inflate queue wait. Negative durations are clamped to zero.
func QueueWaitDurationSeconds(jobCreatedAt, runScheduledAt, startedAt time.Time, firstOneShotAttempt bool) float64 {
	queuedAt := runScheduledAt
	if firstOneShotAttempt {
		queuedAt = jobCreatedAt
	}
	wait := startedAt.Sub(queuedAt).Seconds()
	if wait < 0 {
		return 0
	}
	return wait
}

// DefaultObservabilityTimeout is the maximum time a single /metrics scrape may
// spend waiting for the scheduler's database snapshot.
const DefaultObservabilityTimeout = 3 * time.Second
