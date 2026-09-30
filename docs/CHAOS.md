# Process failure verification

On 2026-09-30, the experiment runner launched real API, scheduler, and worker
processes against PostgreSQL 18.6 and Redis. All seven scenario groups passed.
Workers execute Atlas's production pool with explicit experiment handlers.
The [raw report](benchmarks/2026-09-30/chaos/report.json) retains 41 assertions,
observations, timing, and the isolated-schema cleanup result.

| Injected fault | Expected behavior | Observed |
| --- | --- | --- |
| SIGKILL after a committed side effect, before run completion | Lease recovery replays the same execution key; an atomic idempotency boundary suppresses duplicate effects. | Attempt 2 succeeded; exactly one protected effect row remained. |
| Worker loss during its lease | A fresh worker reclaims expired ownership and finishes another attempt. | Attempt 2 succeeded after heartbeat resumed. |
| Scheduler leader SIGKILL | The PostgreSQL session lock releases; the standby wins and schedules new work. | A different scheduler owned the lock; a new HTTP-submitted job succeeded at attempt 1. |
| First handler exceeds its timeout | Cancel execution, apply retry policy, and accept completion only from the current attempt. | Attempt 2 succeeded; the stored result identified attempt 2. |
| PostgreSQL transport loss | Reject creation without inserting partial work; resume after connections recover. | HTTP 500 with no job inserted, followed by HTTP 201 and successful execution. |
| Redis transport loss | Distributed quota fails closed without creating work; recover when Redis returns. | HTTP 503 with no job inserted, followed by HTTP 201 and successful execution. |
| Worker SIGTERM: cooperative and context-ignoring handlers | Stop new claims, drain within grace, then leave ownership to expire if necessary. | Cooperative work finished at attempt 1; an assigned second run stayed at attempt 0. The ignoring handler's process exited in 2.006 seconds, and a fresh worker recovered attempt 2 with the same key. |

The cooperative worker exited in 0.767 seconds after SIGTERM. The second case
used a 2-second application grace and a 3-second lease. The old run remained
running under a live lease immediately after exit, was not reclaimed before its
persisted expiry, and retained attempt 2's result after the old handler's nominal
completion time. Unit tests separately keep a context-ignoring handler alive
inside the process and check that its late return cannot finalize after grace.

The side-effect barrier is written only after the protected effect transaction
commits. This establishes the crash window directly. Its guarantee covers a
PostgreSQL effect and idempotency record in the same transaction; arbitrary
external HTTP effects still require destination-side idempotency. Atlas's
delivery guarantee remains at-least-once.

Dependency faults cut existing connections through runner-owned TCP proxies.
PostgreSQL and Redis themselves remained running. These checks establish
transport isolation and recovery, not service restart, disk loss, or network
partition safety. SIGTERM ran on Linux under WSL; Kubernetes pod termination
and real GPU execution were not tested in a live cluster.

The runner tracks and stops only its child processes and drops only its unique
schema. The report confirms schema cleanup. Reproduce the scenarios with the
[experiment runbook](EXPERIMENT-RUNBOOK.md); pass `--mode chaos --scenario all`
and a Redis endpoint, or select one scenario. Failed assertions produce a
nonzero exit and remain in the report.
