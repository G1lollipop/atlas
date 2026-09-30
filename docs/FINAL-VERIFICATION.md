# Acceptance: coordination, recovery, and measured scaling

Date: 2026-09-30. Local runtime: WSL2, Go 1.26.5, PostgreSQL 18.6,
Redis 8.0.5. Database/Redis tests used actual services. New fixtures create and
drop separate schemas; experiments cut only owned proxy connections or owned
processes. Existing unrelated local artifacts were preserved.

| Requested area | Implementation / evidence | Boundary |
| --- | --- | --- |
| Graceful shutdown | Configured drain window; claims stop; heartbeat becomes draining; active leases renew; grace expiry prevents late lifecycle writes. Real SIGTERM checks and race tests passed. | Custom handlers must propagate context and release checked-out connections. |
| Leadership | `LeaderElector`, dedicated-session PostgreSQL campaign, bounded probing/release, session-loss and leader SIGKILL tests. | Etcd is a documented migration decision, not an implemented backend. |
| Scheduler scans | Job UUID keyset pages and scored-run keyset cursor; PostgreSQL tests cross more than 1,000 candidates. | Per-pass work still grows with backlog; public list APIs can retain offset pagination. |
| Test system | Unit, PostgreSQL/Redis integration, 32-worker execution, 32-way assignment/lease races, stale-attempt fencing, process faults. | Concurrent valid claims are exclusive; expired execution can overlap an uncooperative old handler. |
| Chaos | Seven groups and 41 retained assertions; side-effect crash, worker loss, leader kill, timeout, PG/Redis transport loss, and SIGTERM. | Transport loss is not a server restart or disk failure. |
| Load and bottleneck | 9,000 primary jobs / 30 trials plus 900 polling-probe jobs; raw JSON/CSV, PNG/SVG plots, independent percentile recomputation. | Simulated service time, shared local host, three repetitions; no production capacity claim. |
| Policies | Seeded 70/20/10 mixed workload; FirstFit versus BestFit throughput, class p95, reservation/slack, placement, and bounded waiting. | No universal policy winner or continuous-demand starvation proof. |
| Design documentation | New architecture narrative/diagram, five ADRs, recruiter README, runbook, test and failure guides. | Cron/DAG remain supporting features. |
| History | Separate scheduler, worker, testing, startup fix, performance, and documentation commits. | Baseline attribution remains in provenance/history. |

## Commands actually run

With both database variables and `REDIS_ADDR` set to the disposable services:

```sh
go test -race -count=1 ./...
go vet ./...
golangci-lint run ./...
```

The complete real-service race suite and vet passed. CI-matching
golangci-lint 2.12.2 reported zero issues. The experiment helper tests verify
schema ownership, unordered percentile inputs, process-stat parsing, CSV
integrity, and TCP isolation/recovery. A separate race-built runner completed
two live batches, checking telemetry synchronization and per-trial reset.

The process runner completed `--mode chaos --scenario all`; the raw
[chaos report](benchmarks/2026-09-30/chaos/report.json) retains every assertion.
After review found a narrow registration-to-cancellation race, a startup drain
regression was added and passed, along with the full race suite and a fresh
process SIGTERM run. The final
[SIGTERM report](benchmarks/2026-09-30/grace-final/report.json) records this check.

`k8s/validate.ps1` rendered and checked core, NVIDIA, and KEDA configurations.
The new drain configuration is 20 seconds with a 45-second Kubernetes outer
termination budget. This is static validation: Docker Engine and a live
Kubernetes/GPU cluster were unavailable; live KEDA, real GPU execution, and
pod-scale termination were not claimed.

## Measurement integrity

Each primary configuration ran three 300-job batches. Every primary batch
accepted and succeeded all jobs with no unfinished work; raw job timestamps
independently reproduce recorded p95 assignment, start, and ready-wait values.
The per-trial reset guard checks the exact owned schema before truncation.
Reports confirm schema cleanup. Failed/partial trials would be retained and
produce a nonzero exit; the plotter excludes them from complete aggregates.

The earlier retained-history diagnostic is explicitly separate from the
controlled result. Full results, definitions, causal limits, source hashes,
and reproduction steps are in [BENCHMARKS.md](BENCHMARKS.md).
