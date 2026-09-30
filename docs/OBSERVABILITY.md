# Job traces, scheduling metrics, and dashboards

See [OBSERVABILITY-VERIFICATION.md](OBSERVABILITY-VERIFICATION.md) for the current
test evidence and deployment verification boundary.

Atlas persists W3C `traceparent` and `tracestate` with jobs and runs. An accepted
submission carries its HTTP trace through promotion, assignment, leasing, handler
execution, and the final database write. These fields are internal metadata;
clients cannot supply them in the JSON create body. No payload or baggage is
copied into the trace context.

Scheduling and execution happen after the HTTP response, so their child spans
can outlive the submission span. Each run advances its stored parent context
transactionally at the scheduling and lease boundaries. Retries, lease recovery,
and dead-letter retry retain the trace identity, while each execution attempt
gets a new span. The handler receives that context and can create child spans or
inject it into an outbound request.

Set `OTEL_EXPORTER_OTLP_ENDPOINT=jaeger:4318` on all three services to export via
OTLP/HTTP. The endpoint is a `host:port`, without a URL scheme. Compose includes
a pinned Jaeger v2 instance and an explicit in-memory configuration. Open
`http://localhost:16686`, choose service `api`, and search for the submission;
attributes such as `job.id`, `run.id`, and `run.attempt` identify the work. The
trace should also include spans from services `scheduler` and `worker`.

Without an exporter endpoint, Atlas still creates propagation IDs but records
no spans and sends no telemetry. Invalid or missing persisted context starts a
fresh trace for the selected work, rather than inheriting an unrelated polling
trace. Persisting context does not persist the collector's spans: delayed or
recurring jobs can outlive collector retention. Production needs a retention and
sampling policy appropriate to those lifetimes.

## Metric meanings

Every service exposes `/metrics` on `METRICS_ADDR` (default `:9090`). The scheduler
also publishes a database-backed fleet snapshot on scrape; both leader and
standby replicas can serve the same global values. Use `max`, not `sum`, across
replica copies of those gauges. Event counters and histograms are process-local
and should be summed across producers. Existing `atlas_runs_*`,
`atlas_queue_depth`, and cache metrics remain available for compatibility.

| Metric | Meaning |
| --- | --- |
| `atlas_jobs_submitted_total` | New job definitions accepted by the API; idempotent replays do not increment it. |
| `atlas_jobs_completed_total` | Successful persisted run completions. A recurring job can complete many runs. |
| `atlas_jobs_failed_total` | Failed attempts whose failure transition was persisted, including retriable failures. |
| `atlas_job_queue_depth{queue,resource_class}` | Ready work awaiting execution: active unpromoted one-shot jobs and due queued, scheduled, or assigned runs. Excludes executing work, future retry delays, and canceled work. |
| `atlas_job_queue_wait_seconds` | Ready-to-lease wait per attempt; first one-shot attempts include submission-to-promotion time, retries exclude backoff. |
| `atlas_job_execution_seconds` | Handler wall-clock duration, including failed or canceled executions. |
| `atlas_scheduler_decision_seconds` | Wall time for a leader's promotion and dispatch cycle. |
| `atlas_worker_utilization{worker_id,resource}` | Reserved capacity divided by advertised capacity; this measures scheduler reservations, not physical CPU/GPU activity. |
| `atlas_worker_cpu_reserved` | CPU millicores reserved by current assignments or leases. |
| `atlas_worker_memory_reserved` | Host memory MB reserved by current assignments or leases. |
| `atlas_worker_gpu_reserved` | GPU devices reserved by current assignments or leases. |
| `atlas_retry_total` | Automatic retries successfully persisted. |
| `atlas_dead_letter_total` | Successful transitions into dead letter. |
| `atlas_lease_expired_total` | Expired leases actually reclaimed; competing janitors count only the rows each reclaimed. |
| `atlas_observability_up` | Whether the database snapshot succeeded. A failed snapshot omits queue gauges instead of reporting false zero backlog. |

The resource class is computed from the request: `required_gpu_count > 0` is
`gpu`, otherwise `cpu`. It is independent of queue names and worker labels.
Histograms and event counters avoid job IDs, tenant IDs, and arbitrary queue
names as labels. Queue names and worker IDs appear only on snapshot gauges;
their cardinality follows current backlog queues and fleet inventory. Completed
queue series disappear from the next successful scrape.
Worker snapshots retain recent heartbeats for ten heartbeat TTLs (five minutes
with the default 30-second TTL). Stale workers inside that window report
`atlas_worker_alive=0` and zero reservations; older worker series disappear.
Fresh draining workers remain alive in this snapshot and retain their active
reservations. Alive means reachable, not accepting new work: assignment and
claim paths explicitly exclude workers whose status is `draining`.

## Grafana and scaling

Compose provisions three additional dashboards in the Atlas folder:
**Scheduler Overview**, **Worker Fleet**, and **Failures**. The existing
**Atlas Overview** remains available. They show ready backlog, queue and
execution latency, scheduling duration, reservations, retries, dead letters,
and lease recovery.

CPU queue depth across scheduler replicas is:

```promql
sum(max by (queue, resource_class) (
  atlas_job_queue_depth{resource_class="cpu"}
    and on (job, instance) (up == 1)
    and on (job, instance) (atlas_observability_up == 1)
))
```

Use `resource_class="gpu"` for GPU work. Empty, healthy resource classes expose zero;
unavailable snapshots expose no depth series. The optional KEDA manifests use
these queries, reject missing data, and configure fallback replica behavior.
See [KUBERNETES.md](KUBERNETES.md) for heterogeneous worker deployment and
scaling prerequisites. Capability matching permits a GPU worker to run CPU work,
so independent scaler classes do not imply exclusive queue-to-machine routing.

References: [OpenTelemetry trace API](https://opentelemetry.io/docs/specs/otel/trace/api/),
[Prometheus instrumentation](https://prometheus.io/docs/practices/instrumentation/),
[KEDA Prometheus scaler](https://keda.sh/docs/2.18/scalers/prometheus/), and
[Jaeger downloads and versions](https://www.jaegertracing.io/download/).
