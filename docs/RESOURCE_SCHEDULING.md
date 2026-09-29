# Atlas assignment and scheduling policy

Atlas separates deciding **when** a run is ready from deciding **where** it runs.
The elected scheduler creates a `queued` run for a due, dependency-satisfied job.
It moves due queued runs to `scheduled`, scores each feasible run–worker pair, and
atomically records `assigned_worker_id`. A worker only leases an unexpired run
assigned to its own ID. Execution then follows `leased` → `running` →
`succeeded`; exhausted failures enter the dead letter state.

```text
queued → scheduled → assigned → leased → running → succeeded
assigned → queued     (assignment expires before lease)
leased/running → queued (lease expires)
running → queued      (retry with backoff)
running → failed → dead (attempts exhausted)
```

The scheduler is leader-elected with a PostgreSQL advisory lock. Every leader pass
reclaims expired worker leases, returns expired assignments to the queue, schedules
due runs, and assigns feasible runs. The assignment TTL is 30 seconds. Workers
must have sent a heartbeat within 30 seconds to receive a new assignment. An
expired assignment or lease releases its reservation so another worker can be
chosen. The database validates worker capacity again in the assignment
transaction; the scheduler's read of available resources is a planning snapshot.

Jobs declare CPU in millicores, host memory in MB, GPU count, memory per GPU in
MB, and an optional accelerator type. Workers advertise capacity, GPU type, and
labels on every heartbeat. `GET /v1/workers` reports available resources after
subtracting the requirements of `assigned`, `leased`, and `running` work. Worker
heartbeats cannot overwrite those server-derived reservations. A GPU request must
fit both the per-device GPU memory and the aggregate free GPU memory.

`SCHEDULING_POLICY` selects a placement strategy at scheduler startup. The
default is `priority-aware`; the other values are `first-fit`, `least-loaded`,
and `best-fit`. Each strategy checks worker liveness and resource fit. All four
consider scheduled runs by priority plus waiting age, then differ in how they
choose a worker:

| Policy | Worker choice | Trade-off |
| --- | --- | --- |
| `first-fit` | First feasible worker in stable worker-ID order | Simple, but can concentrate work and fragment capacity. |
| `least-loaded` | Lowest normalized utilization of the resources this job requests | Spreads load, but may leave small unusable gaps. |
| `best-fit` | Least normalized capacity left after this job | Packs work tightly, but can concentrate load. |
| `priority-aware` | Highest run/worker score, with a GPU memory fragmentation penalty | Keeps urgency and aging in the placement decision while preferring a closer GPU fit. |

The default strategy scores each feasible run–worker pair as:

```text
score = run.priority × 3600
      + seconds_waiting_since_scheduled_at
      − GPU_fragmentation_penalty
```

The GPU penalty is the fraction of per-device memory left unused by a GPU job,
bounded between 0 and 1. With the same job and sufficient free resources, a 4GB
request therefore prefers an 8GB GPU over a 48GB GPU. One priority point equals
one hour of waiting. Aging is unbounded: a long-waiting low-priority run can
outrank newly arriving higher-priority runs. Equal scores use the earlier
`scheduled_at` first, then stable run and worker IDs. The database makes the
final fit and assignment decision under row locks, so concurrent scheduler
attempts cannot reserve the same run twice or overbook a worker.

The worker runs at most `WORKER_CONCURRENCY` handlers at once (default 4). A
buffered execution-slot channel bounds the local goroutines that may lease work.
The worker also tracks CPU, host memory, GPU count, and GPU memory used by its
currently executing handlers and checks local capacity before starting another.
`WORKER_CONCURRENCY=0` intentionally starts lease cleanup without registering
or leasing work; negative values are rejected. PostgreSQL reservations remain the
shared source of truth across worker processes: local admission is a second
guard within one process, not another database reservation.

`AssignRun` locks the worker row before it reads active reservations and writes
the new assignment. That serializes competing scheduler transactions for the
same worker. For example, if two 12GB jobs race for a 16GB worker, only one can
reserve memory; the other remains scheduled until capacity is released.

For example, an embedding job can request `required_cpu_millis: 2000`,
`required_memory_mb: 4096`, and no GPU. An inference job can request
`required_gpu_count: 1` and `required_gpu_memory_mb: 16384`. `workload_type`
labels the work but does not silently add resource requirements. Existing jobs
with zero requirements remain eligible on any registered worker.

Capacity is a scheduling declaration, not operating-system enforcement. Labels
are inventory metadata, not placement constraints in this version. GPU devices
within a worker are modeled as identical, with aggregate free VRAM rather than
per-device live reservations; heterogeneous GPUs or fragmented per-device
allocations need a richer inventory model. Each scheduler pass currently scans
all scheduled runs in pages and scores feasible run–worker pairs in memory, so
very large backlogs will need a more selective candidate index or bounded
incremental dispatch.

## Execution, leases, and recovery

A worker renews the lease of a running job at a fraction of the configured lease
duration. Renewal is fenced by the worker ID, run attempt, active status, and
unexpired lease. A long-running handler can therefore keep its lease while it is
healthy; if the worker loses ownership, it cancels the handler context and does
not write a completion for that attempt. Handlers must respect context
cancellation before making further effects. The janitor requeues runs whose lease has
expired. A worker heartbeat serves a different purpose: it advertises capacity
and liveness to the scheduler so the scheduler can assign new work. It does not
extend any individual run's lease.

Execution is **at least once**. A handler can finish an external side effect and
crash before the run is marked succeeded; recovery may execute the same run
again. Every run has a stable `execution_key` that survives lease expiry, normal
retry, and a manual dead-letter retry. Handlers can pass that key to a downstream
system that supports idempotency, or use the transactional `IdempotencyStore` for
database effects. Neither approach makes arbitrary external effects exactly once:
the downstream system must actually honor the key, and a database transaction
cannot atomically cover an unrelated external service.

Transient failures are retried with a bounded, configurable exponential delay
plus jitter so jobs that fail together do not all retry at the same instant.
The run's `max_attempts` remains the upper bound. Once attempts are exhausted,
the run enters `dead` and a dead-letter record is available through the API.
An operator can retry that same run from attempt zero, keeping its
`execution_key`, or discard the dead-letter record while leaving the run
terminal for audit. A retry of a run that already applied a side effect therefore
still needs the handler's idempotency contract.

The built-in `idempotent_record` handler is a small database example. It writes
the job payload to `idempotent_handler_effects` and the result to
`execution_idempotency` in one transaction; a repeat call with the same
`execution_key` reads that stored result. The `http_call` example sends the
same key as `Idempotency-Key` on mutation requests, but the receiving service
must implement deduplication for that header to have an effect.

The worker reads these retry settings at startup:

| Setting | Default | Meaning |
| --- | --- | --- |
| `WORKER_RETRY_BASE_DELAY` | `1s` | First retry delay. |
| `WORKER_RETRY_MULTIPLIER` | `2` | Exponential growth factor, at least one. |
| `WORKER_RETRY_MAX_DELAY` | `5m` | Cap on the exponential part before jitter. |
| `WORKER_RETRY_JITTER` | `0.2` | Uniform extra delay from zero through 20% of the capped delay. |
| `WORKER_RETRY_MAX_ATTEMPTS` | `0` | Optional fleet-wide ceiling; zero uses the job's `max_attempts`. |

For example, with the defaults, the second attempt starts after `1s` plus up
to `0.2s` of jitter following the first failure. The next delay starts at
`2s` plus up to `0.4s`; later exponential delays stop growing at `5m`, then
receive up to `1m` of jitter. The worker's optional attempt ceiling can only
reduce the maximum declared by a job.

## Queues, admission, and cancellation

Jobs carry a free-form `queue` workload class and a `tenant_id` derived from the
verified JWT subject on API submission. Queue names group backlog for quotas;
worker placement still checks the job's resource requirements against live
worker capability. `workload_type` is descriptive and does not choose a worker
or queue automatically. This is quota accounting for an admin API, not
multi-tenant authorization: a valid admin token can still inspect any job.

Admission has three independently configurable ceilings:
`MAX_QUEUE_DEPTH` (default 10000), `PER_QUEUE_LIMIT` (2500), and
`PER_TENANT_LIMIT` (1000). Zero disables that particular ceiling. The backlog
counts active queued, scheduled, assigned, leased, and running runs, plus
accepted one-shot jobs that have not yet been promoted to a run. A terminal
run releases its slot. Recurring job definitions use capacity when each due
run is created, rather than holding a permanent slot between executions.
Submission, due-run creation, and manual dead-letter retry all check capacity
under the same PostgreSQL transaction-scoped advisory lock before adding work.
All API and scheduler replicas must use the same configured limits. An API
request that exceeds a limit returns `429`; a recurring due run waits for a
later scheduler pass when capacity is full.

`POST /v1/jobs/{id}/cancel` makes the job terminal and prevents future
promotion. Unstarted runs move straight to `canceled` and release assignments.
For a leased or running run, the database records a durable cancellation
request; the worker checks it during lease renewal, cancels the handler
context, then acknowledges the run as canceled. Completion and retry writes
are fenced against that request. If the worker dies first, lease expiry lets
the janitor finalize cancellation. A handler must honor context cancellation
to stop its own external work promptly; arbitrary code that ignores context
cannot be forcibly stopped by Go.
