# Observability and heterogeneous workers: verification

Verified on 2026-09-30 UTC using the final Atlas binaries, Go 1.26.5,
PostgreSQL 18.6, local Redis, and the official Jaeger 2.21.0 binary. Database
fixtures used isolated schemas and were dropped after each run. This record
describes local process verification and manifest validation, not a deployed
Kubernetes environment.

## Code and database checks

| Check | Result |
| --- | --- |
| Full `go test -count=1 ./...` with both database test variables set | Passed |
| Final `go test -race -count=1 ./...` after lint fixes | Passed |
| `go vet ./...` | Passed |
| CI-matching golangci-lint 2.12.2 | Zero issues |
| Durable tracing integration test with `-count=2` | Passed twice |
| Core, NVIDIA overlay, and KEDA Kustomize rendering | Passed |
| `k8s/validate.ps1`, including JSON worker labels | Passed |
| Compose configuration, Jaeger configuration validation, YAML parsing | Passed |
| Four provisioned Grafana dashboard JSON files | Parsed; panel IDs unique |

The durable tracing test sends an actual instrumented HTTP request through the
router and uses PostgreSQL, the promoter, dispatcher, and worker pool. It checks
the database insertion span and parent IDs through handler execution and result
persist. It also checks retry, lease recovery, and dead-letter retry; an untraced
failure call preserves the stored trace, and forged JSON trace fields cannot
replace the request context.

The snapshot tests cover paused jobs with existing runs, future retry delays,
canceled work, reservations before and after reclaim, stale worker retirement,
healthy empty queues, and database failures that must not emit false zero depth.

## Actual processes and exported trace

A local HTTP API, scheduler, CPU worker, and simulated GPU worker were run as
separate processes against an isolated schema. They exported OTLP/HTTP to Jaeger
using the committed development configuration with only its ports changed.

The success trace was retrieved from Jaeger's v3 API and contained 37 spans from
services `api`, `scheduler`, and `worker`, all with trace ID
`391f84bb5d0e4710a95125ded469c88f`. The job ID was
`d0f8900c-b64d-4742-b815-b138936965bf`; the run ID was
`cf516f9f-a6b3-44b2-b29e-3bbf64e8bab5`.

Parent IDs were asserted along this chain:

```text
POST
  api.CreateJob
    scheduler.PromoteJob
      scheduler.CreateRun
        scheduler.ScheduleRun
          scheduler.AssignRun
            worker.LeaseRun
              worker.executeOne
                worker.handler
                worker.result.persist
```

The live checks also confirmed:

- A running CPU job appeared in the database snapshot with 1,000 CPU millicores
  and 1,024 MB reserved.
- A GPU-required job remained in backlog while only a CPU worker was available;
  a newly started compatible simulated GPU worker completed it.
- A timed-out job retried once and then entered dead letter after its second
  failed attempt.
- The two worker endpoints together reported two successful completions, two
  failed attempts, one retry, one dead letter, and four measured executions.

Local raw trace, metric scrape, and report artifacts were retained under the
ignored `.baseline/observability` directory. They are verification outputs, not
repository fixtures. The development collector uses volatile in-memory storage.

## Deployment boundary and repeatable checks

Docker Engine and a Kubernetes cluster were unavailable in this environment.
Grafana browser rendering, pod startup on real NVIDIA devices, and actual KEDA
scale-out/scale-in were not exercised. The committed manifests and dashboards
were validated statically; the GPU workload check used simulated capability.

Repeat the committed database trace test with a disposable PostgreSQL instance:

```sh
export ATLAS_TEST_DATABASE_URL='postgres://USER:PASSWORD@localhost:5432/atlas?sslmode=disable'
go test -count=2 -v ./internal/tracing/integration
```

Set `DATABASE_URL` to the same test instance for the existing integration suite,
then run `go test -race -count=1 ./...`. Set `REDIS_ADDR` to a disposable Redis
instance to include its distributed limiter checks. The standalone Kubernetes
validation command is `./k8s/validate.ps1`.

See [OBSERVABILITY.md](OBSERVABILITY.md) for metric semantics and tracing setup,
and [KUBERNETES.md](KUBERNETES.md) for the prerequisites for live deployment and
autoscaling verification.
