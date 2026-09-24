# Atlas baseline results

中文摘要：原始 Taskflow 与 Atlas 各自通过全部 14 项运行时断言；Atlas 全栈冒烟通过 17 项。`go vet`、常规测试及 race 测试均通过。

## Source comparison

The checked out source baseline is commit `64bb4d9d50bc61045f49abf2558a151c7e5973d8`, matching [PROVENANCE.md](PROVENANCE.md). Comparing `.baseline/original` with the working source after normalizing only the module path and brand substitutions (`github.com/manavsingla/taskflow` → `github.com/G1lollipop/atlas`, `taskflow` → `atlas`, `Taskflow` → `Atlas`) produced exact matches for all **36 Go files and the initial SQL migration**. No non-brand source or migration changes were found.

Static review also confirmed that renamed Compose service DNS names match Prometheus scrape targets, Grafana queries use the Atlas metrics, Kubernetes references agree, and Terraform references resolve consistently.

## Runtime baseline

The original source and renamed Atlas each passed all **14 runtime assertions** in the baseline harness. The original run took about **59 seconds** and Atlas about **58 seconds**; these are harness durations, not throughput measurements.

| Source | Result | Evidence |
| --- | --- | --- |
| Original source at `64bb4d9d50bc61045f49abf2558a151c7e5973d8` | 14/14 passed | `.baseline/reports/original-verified/atlas-baseline-original.json` |
| Renamed Atlas | 14/14 passed | `.baseline/reports/atlas-baseline-renamed.json` |

Both runs proved scheduler promotion before workers started, successful `SKIP LOCKED` progress past a locked queue head, dead-lettering after retry exhaustion, and advisory-lock leader failover. In each eight-run worker batch, worker-one and worker-two completed four runs apiece; the audit recorded one lease and one owner per run.

The crash audit recorded `pending → leased → running → pending → leased → running → succeeded`. The first lease-to-reclaim interval was about six seconds, matching the harness’s six-second lease. A concurrency-zero janitor observer intentionally kept the reclaimed row pending for inspection; after that observer stopped, worker-two re-executed the run successfully on attempt two. This measures lease-to-reclaim behavior and does not claim immediate recovery from the kill itself.

Leader failover evidence recorded original scheduler PID 92 (`scheduler-two`) handing off to PID 94 (`scheduler-one`), and Atlas PID 91 (`scheduler-one`) handing off to PID 93 (`scheduler-two`). The killed PID was inactive in each report, and the new leader promoted a subsequent job that worker-two completed successfully.

`go vet ./...` and `go test -count=1 -v ./...` passed. The integration tests connected to the dedicated PostgreSQL database and were not skipped; output is in `.baseline/go-validation.log`. The race-enabled run also passed with integration tests connected to dedicated PostgreSQL database `atlas_race`; output is in `.baseline/go-race.log`.

The root Compose stack smoke passed all **17 assertions** at 2026-09-24 00:14 UTC under project `atlas-stack-verified`. It verified API health and authenticated echo-job completion, PostgreSQL streaming replication and replica recovery state, Redis PING, all three Prometheus targets and Atlas metrics, Grafana health plus datasource/dashboard provisioning, and the Jaeger query endpoint. The tested host endpoints were API `:8080`, Grafana `:3000`, Prometheus `:9093`, and Jaeger `:16686`. Evidence is in `.baseline/reports/stack-smoke.json`.

An earlier root-stack startup failed because Windows line endings made `init-replication.sh`’s shebang resolve as `/bin/sh^M`. Docker shell scripts and Dockerfiles are now normalized to LF, with `.gitattributes` preserving LF on future checkouts. The successful smoke used a fresh Compose project, `atlas-stack-verified`; the earlier failed project’s stopped resources were left untouched.

## Deferred source finding

Code inspection only: `store.Store` defines `ExtendLease` as a periodic worker operation and the Postgres store implements it, but the worker pool never calls it while a handler runs. The janitor reclaims expired `leased` or `running` rows, so a healthy handler that outlives its lease can be reclaimed while still active. This issue was not isolated or reproduced by these runtime checks and remains deferred.
