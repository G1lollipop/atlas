# Empirical scheduler and recovery experiments

The experiment command starts Atlas API, scheduler, and worker processes against a fresh PostgreSQL schema. Benchmark submissions use the real HTTP API; workers use Atlas's lease pool with experiment-only deterministic handlers. Each report includes the exact fixture settings, trial and per-job timings, sampled PostgreSQL observations, child-process logs, and any failed or partial run. The runner drops only the schema it created.

Run this from the repository's WSL checkout with the cached Go toolchain and the isolated verification database. The binaries must be built from the same checkout being measured:

```bash
export GOBIN=/home/herbert/.cache/atlas-verification/go/bin
export GOMODCACHE=/home/herbert/.cache/atlas-verification/mod
export GOCACHE=/home/herbert/.cache/atlas-verification/build
GO=/home/herbert/.cache/atlas-verification/go/bin/go
# With another Go 1.26.5 installation, use: GO="$(command -v go)"

mkdir -p /tmp/atlas-experiment-bin
"$GO" build -o /tmp/atlas-experiment-bin/api ./cmd/api
"$GO" build -o /tmp/atlas-experiment-bin/scheduler ./cmd/scheduler
"$GO" build -o /tmp/atlas-experiment-bin/experiment-worker ./scripts/experiments/worker
```

Use a unique output directory for every invocation. A quick end-to-end measurement can use the CPU-only axis:

```bash
DATABASE_URL='postgres://atlas_test@127.0.0.1:55432/postgres?sslmode=disable' \
"$GO" run ./scripts/experiments \
  --mode bench --workload-profile cpu --jobs 100 --trials 3 \
  --worker-counts 1,2,4,8,16 --policies first-fit,best-fit \
  --submission-rates 25,100,0 --job-duration 50ms \
  --api-bin /tmp/atlas-experiment-bin/api \
  --scheduler-bin /tmp/atlas-experiment-bin/scheduler \
  --worker-bin /tmp/atlas-experiment-bin/experiment-worker \
  --pg-pid-file /home/herbert/.cache/atlas-verification/pg/data/postmaster.pid \
  --out artifacts/experiments/cpu-$(date -u +%Y%m%dT%H%M%SZ)
```

Run the mixed placement batch separately to compare policies on simulated GPU workloads:

```bash
DATABASE_URL='postgres://atlas_test@127.0.0.1:55432/postgres?sslmode=disable' \
"$GO" run ./scripts/experiments \
  --mode bench --workload-profile mixed --jobs 300 --trials 3 \
  --worker-counts 1,2,4,8,16 --policies first-fit,best-fit \
  --submission-rates 25,100,0 --job-duration 50ms \
  --api-bin /tmp/atlas-experiment-bin/api \
  --scheduler-bin /tmp/atlas-experiment-bin/scheduler \
  --worker-bin /tmp/atlas-experiment-bin/experiment-worker \
  --pg-pid-file /home/herbert/.cache/atlas-verification/pg/data/postmaster.pid \
  --out artifacts/experiments/mixed-$(date -u +%Y%m%dT%H%M%SZ)
```

The defaults are bounded (100 jobs, three trials per combination, ten-minute overall deadline). Increase job count or duration only when the host can finish within the selected deadline. Before each individual trial, the runner verifies that no run is active, verifies that PostgreSQL selected the exact generated `atlas_exp_...` schema, truncates that schema's `jobs` table with `CASCADE`, and verifies that job history is empty. This prevents prior batches from adding read and index work to later trials; worker processes remain active between policy comparisons. The report records `history_reset_between_trials` and the configured policy order. Machine time and background load can still drift with policy order, so reverse or repeat policy order when checking small differences. Seeds are recorded in each trial and keep the 70/20/10 CPU/small-GPU/large-GPU batch mix consistent across policies. Submission rates are offered HTTP job rates; `0` means unpaced submissions. The CPU profile removes GPU topology from the worker-count scaling axis. The mixed profile changes its CPU/large-GPU/small-GPU worker topology as worker count changes, so its throughput curve is not a pure CPU scaling curve.

## What the measurements mean

`whole_batch_execution_throughput_jobs_per_second` is succeeded jobs divided by wall time from the first submission until terminal completion of the batch. `completion_drain_ms` is only the time after submission ends. Scheduler latency is measured from database acceptance (`jobs.created_at`) to assignment; acceptance-to-start and `scheduled_at`-to-`started_at` ready-queue waits are separate fields. P95 values use observed terminal runs. The oldest-ready value is sampled, so a short-lived peak can fall between samples. The bounded batch and deadline report oldest wait and unfinished jobs but cannot prove starvation is impossible.

Database CPU is sampled as postmaster plus descendant process CPU using per-PID `/proc` tick deltas and the host's `getconf CLK_TCK`. A process that exits between samples can be undercounted. Lock-wait fraction is sampled lock-waiting app connections divided by sampled active app connections; it is not an exact query-level contention total. Connection pool counts come from sampled `pg_stat_activity` by application name. Each child service pool is capped at four connections and controller pools at two; reported connection counts are what PostgreSQL exposed during samples.

GPU tests are simulations. The report measures reserved VRAM, device reservations, and memory slack proxies; it does not measure physical GPU utilization. A device counts as available only if it has no GPU reservation. Memory on a fully reserved device is recorded as whole-device stranded VRAM, even if some bytes remain free, because this fixture reserves GPU count exclusively. Subthreshold available slack sums free devices below the 16 GB large-job requirement; it is a policy-specific memory slack proxy, not a claim that GPUs are divisible. Small jobs placed on workers with at least 16 GB are counted explicitly. Worker IDs are intentionally ordered CPU-only first, then 32 GB large-GPU workers, then 8 GB small-GPU workers so first-fit and best-fit see this fixture ordering. Results describe this ordering and topology only; they do not imply a universal policy winner.

For plots, install Python `matplotlib` and `numpy` in the analysis environment, then pass the measured report directly:

```bash
python3 scripts/experiments/plot.py artifacts/experiments/mixed-<run-id>/report.json
```

The plotter writes standalone PNG files in the report's `plots/` directory. It reads report values only and lists failed or partial trials before plotting their available measurements. Keep the report JSON and CSV files alongside plots as the source data.

## Process-based recovery scenarios

Build the same three executables above, then run all recovery cases or one case at a time with `--mode chaos`. `--scenario` accepts `side-effect-crash`, `worker-loss`, `scheduler-failover`, `timeout`, `postgres-outage`, `redis-outage`, `graceful-shutdown`, or `all`. Redis cases require `--redis-addr host:port` and use a runner-owned proxy. The PostgreSQL case uses a runner-owned proxy for child processes; disabling it cuts existing child connections temporarily while the shared database remains running. These are transport-isolation experiments, not database restart tests. The report states recovery assertions and retains process logs. Only runner-owned child processes and the unique schema are cleaned up.

## Concurrent database claim check

The integration case uses a separate schema and can be run against the same disposable Postgres instance by setting:

```bash
ATLAS_TEST_DATABASE_URL='postgres://atlas_test@127.0.0.1:55432/postgres?sslmode=disable' \
"$GO" test ./integration/concurrencytests -run TestConcurrentAssignmentLeaseAndFencedCompletion -count=1
```

It races 32 workers for one assignment and one lease, expires and reclaims the lease, then confirms that an older attempt cannot complete after attempt two owns the live lease. Without `ATLAS_TEST_DATABASE_URL`, Go skips the database integration case.
