# Atlas resource scheduling

Atlas keeps the existing job lifecycle (`pending` → `leased` → `running` →
`succeeded`, with retries and dead letters) and adds resource-aware placement when a
worker leases a pending run. The scheduler promoter still determines *when* a job
becomes pending; the lease query determines *which worker* can run it.

Create a job with its resource requirements. CPU is measured in millicores, and
memory and GPU memory are measured in MB. GPU memory is the minimum memory **per
GPU** requested; `required_accelerator` matches the worker's GPU type without case
sensitivity. `workload_type` classifies the work and does not imply hidden resource
requirements.

```json
{
  "name": "echo",
  "workload_type": "inference",
  "priority": 20,
  "required_cpu_millis": 2000,
  "required_memory_mb": 4096,
  "required_gpu_count": 1,
  "required_gpu_memory_mb": 16384,
  "required_accelerator": "NVIDIA-A100",
  "timeout_seconds": 300,
  "max_attempts": 5
}
```

Workers advertise capacity and labels at startup and on every heartbeat through
`WORKER_CPU_CAPACITY_MILLIS`, `WORKER_MEMORY_CAPACITY_MB`, `WORKER_GPU_COUNT`,
`WORKER_GPU_TYPE`, `WORKER_GPU_MEMORY_MB`, and `WORKER_LABELS` (a JSON string map).
`GET /v1/workers` shows those capabilities, the resources reserved by the worker's
active `leased` and `running` runs, and the remaining available resources. The
server derives reservations from active leases rather than trusting a worker's
self-reported free capacity. A heartbeat updates capacity and labels without
resetting reservations.

Within a transaction, a worker locks its registration row, sums its current
reservations, then selects the highest-priority due `pending` run whose CPU,
memory, GPU count, per-GPU memory, total GPU memory, and accelerator type fit.
`FOR UPDATE SKIP LOCKED` keeps concurrent claims from taking the same run. The
worker-row lock also prevents its own concurrent pollers from exceeding capacity.
An incompatible run stays `pending` and does not block lower-priority work that
fits. Completing, retrying, dead-lettering, or reclaiming a run releases its
reservation through the run's status change.

Capacity is a scheduling declaration, not operating-system enforcement. A handler
can use more resources than requested, and jobs with zero requirements remain
eligible on any registered worker for compatibility. Labels are exposed for
inventory but are not placement constraints in this version. GPU memory is modeled
as equal capacity per GPU; heterogeneous devices within one worker are not yet
represented.
