# API reference

Base URL: `http://<host>:8080` (the `HTTP_ADDR` the `api` binary listens on; `:8080` by
default). All routes are defined in `internal/api/router.go`.

A machine-readable version of this reference is at [openapi.yaml](openapi.yaml)
(OpenAPI 3.0) — load it into Swagger UI, Postman, or a
client generator instead of hand-transcribing the tables below.

The placement rules for resource-aware jobs are described in
[RESOURCE_SCHEDULING.md](RESOURCE_SCHEDULING.md).

## Auth

Every route under `/v1` requires `Authorization: Bearer <token>`, where `<token>` is an
HS256 JWT signed with the API's `JWT_SECRET`. There is no login endpoint — see the
[`MintToken` helper](../internal/api/auth.go) for how to mint one locally. Requests
without a well-formed bearer token get `401`; requests over the per-IP rate limit get
`429` (see [ARCHITECTURE.md](ARCHITECTURE.md) for why the limiter is per-process, not
shared). `GET /healthz` and `GET /metrics` are unauthenticated and unrated.

All responses are `application/json`. Error responses are `{"error": "<message>"}`.
Handler-side (`5xx`) errors are logged server-side with full detail and returned to the
client as a generic `"internal server error"` — no internal error text is leaked.

---

### `GET /healthz`

No auth. Liveness probe.

- **200** `{"status": "ok"}`

### `GET /metrics`

No auth. Prometheus exposition format (all registered collectors — HTTP handled by
`internal/metrics`).

---

### `POST /v1/jobs`

Create a job (one-shot, or recurring if `cron_expr` is set). Idempotent when
`idempotency_key` is supplied.

**Request body** (`model.NewJobInput`, max 64KB):

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | yes | Non-blank after trimming whitespace; also selects the worker handler. |
| `payload` | object | no | Arbitrary JSON passed to the handler. |
| `cron_expr` | string | no | Standard 5-field cron expression (`robfig/cron` `ParseStandard`); omit for one-shot jobs. |
| `priority` | int16 | no | Higher runs first within the pending queue. |
| `workload_type` | string | no | Defaults to `generic` when omitted or blank; trimmed before storage. A descriptive workload class such as `embedding` or `inference`. |
| `required_cpu_millis` | int32 | no | CPU request in millicores; defaults to `0`, must be non-negative. `2000` means two CPU cores. |
| `required_memory_mb` | int32 | no | Host RAM request in MB; defaults to `0`, must be non-negative. |
| `required_gpu_count` | int32 | no | Number of GPU devices; defaults to `0`, must be non-negative. |
| `required_gpu_memory_mb` | int32 | no | GPU memory per requested device in MB; defaults to `0`, must be non-negative, and requires `required_gpu_count > 0`. |
| `required_accelerator` | string | no | Optional GPU type selector, trimmed before storage; requires `required_gpu_count > 0`. Empty means any GPU type. |
| `max_attempts` | int16 | no | Defaults to `5` if `0`; must be `>= 1` otherwise. |
| `timeout_seconds` | int32 | no | Defaults to `300` if `0`; must be `>= 1` otherwise. |
| `idempotency_key` | string | no | If reused, returns the original job instead of creating a new one. |
| `depends_on` | array of job IDs | no | Job IDs whose latest run must have succeeded before this job is promoted. |

**Responses:**

| Status | When |
|---|---|
| `201 Created` | Job created; body is the full `model.Job` |
| `200 OK` | `idempotency_key` matches an existing job; body is that existing `model.Job` (no new job created) |
| `400 Bad Request` | Body isn't valid JSON (`"invalid request body"`); `name` blank (`"name is required"`); `cron_expr` doesn't parse (`"invalid cron_expr"`); any resource request is negative; GPU memory or accelerator is set while `required_gpu_count` is `0`; `max_attempts < 1`; or `timeout_seconds < 1`. |
| `409 Conflict` | `idempotency_key` collided with a different job concurrently (`"idempotency key already used"`) |
| `500` | Store failure |

### `GET /v1/jobs`

List jobs, most recently created first.

**Query params:**

| Param | Notes |
|---|---|
| `status` | One of `active`, `paused`, `archived`; omit for all statuses. Anything else → `400 "invalid status"` |
| `limit` | Default `50`, capped at `200`. Must parse as a non-negative int or → `400 "invalid limit"` |
| `offset` | Default `0`. Must parse as a non-negative int or → `400 "invalid offset"` |

- **200** `[]model.Job`
- **500** on store failure

### `GET /v1/jobs/{id}`

- **200** `model.Job`
- **404** `"job not found"` if no job with that ID exists
- **500** on store failure

### `POST /v1/jobs/{id}/pause`

Sets the job's status to `paused` (the promoter skips non-`active` jobs).

- **200** the updated `model.Job`
- **404** `"job not found"`
- **500** on store failure

### `POST /v1/jobs/{id}/resume`

Sets the job's status to `active`.

- **200** the updated `model.Job`
- **404** `"job not found"`
- **500** on store failure

### `GET /v1/jobs/{id}/runs`

List runs for a job, most recently created first. Does not validate that `{id}`
corresponds to a real job — a nonexistent job ID simply returns an empty list, not
`404`.

**Query params:** `limit` — default `50`, capped at `200`, same validation as above.

- **200** `[]model.JobRun`
- **400** `"invalid limit"`
- **500** on store failure

### `GET /v1/runs/{id}`

- **200** `model.JobRun`
- **404** `"run not found"`
- **500** on store failure

### `GET /v1/workers`

Lists workers that have sent a heartbeat, with their last-known status, capability, and
resource availability. Resource capacity and labels come from the latest worker
heartbeat; reservations are calculated from the worker's active leased and running jobs.

- **200** `[]model.Worker`
- **500** on store failure

---

## Response shapes

**`model.Job`**

```json
{
  "id": "uuid",
  "name": "echo",
  "payload": {},
  "cron_expr": null,
  "priority": 0,
  "workload_type": "generic",
  "required_cpu_millis": 0,
  "required_memory_mb": 0,
  "required_gpu_count": 0,
  "required_gpu_memory_mb": 0,
  "required_accelerator": "",
  "max_attempts": 5,
  "timeout_seconds": 300,
  "status": "active",
  "idempotency_key": null,
  "depends_on": [],
  "created_at": "2026-07-21T00:00:00Z",
  "updated_at": "2026-07-21T00:00:00Z"
}
```

**`model.JobRun`**

```json
{
  "id": "uuid",
  "job_id": "uuid",
  "status": "pending",
  "attempt": 0,
  "priority": 0,
  "scheduled_at": "2026-07-21T00:00:00Z",
  "leased_by": null,
  "leased_at": null,
  "lease_expires_at": null,
  "started_at": null,
  "finished_at": null,
  "result": null,
  "error": null,
  "created_at": "2026-07-21T00:00:00Z"
}
```

`status` is one of `pending`, `leased`, `running`, `succeeded`, `failed`, `dead` — see
the lifecycle diagram in [ARCHITECTURE.md](ARCHITECTURE.md).

**`model.Worker`**

```json
{
  "id": "host-1234",
  "hostname": "host",
  "status": "alive",
  "last_heartbeat_at": "2026-07-21T00:00:00Z",
  "started_at": "2026-07-21T00:00:00Z",
  "cpu_capacity": 4000,
  "memory_capacity_mb": 8192,
  "gpu_count": 1,
  "gpu_type": "nvidia-h100",
  "gpu_memory_mb": 24576,
  "labels": {"zone": "us-central1-a"},
  "current_cpu_reserved": 2000,
  "current_memory_reserved": 4096,
  "current_gpu_reserved": 0,
  "current_gpu_memory_reserved": 0,
  "available_cpu_millis": 2000,
  "available_memory_mb": 4096,
  "available_gpu_count": 1,
  "available_gpu_memory_mb": 24576
}
```

Worker CPU capacity and reservations use millicores (`4000` is four CPU cores); host
memory and GPU memory values use MB. GPU memory capacity and availability are aggregate
across that worker's GPUs, while a job's `required_gpu_memory_mb` must also fit on each
requested GPU. Available values are capacity minus active reservations, floored at zero.
The scheduler checks available CPU, RAM, GPU count, and aggregate GPU memory, and checks
that the requested per-GPU memory and accelerator type fit the worker. Accelerator type
matching ignores surrounding whitespace and letter case. A job requiring a GPU cannot
be claimed by a CPU-only worker.
