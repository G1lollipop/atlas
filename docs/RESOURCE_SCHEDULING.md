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
