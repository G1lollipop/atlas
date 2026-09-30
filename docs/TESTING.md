# Atlas test system

The test suite separates algorithm decisions, transactional invariants,
concurrent execution, and process failures. Passing a mock test is not evidence
that a database lock, signal, or recovery loop works in a live process.

## Unit tests

`internal/scheduler` tests resource fit, strategy decisions, priority and aging,
and deterministic queue order. `internal/worker` tests resource admission,
bounded slots, retry caps/jitter, lease renewal, cancellation, and shutdown.
`internal/tracing` checks W3C propagation boundaries. API tests check validation,
authorization, cancellation, dead letters, and quota responses.

```sh
go test -count=1 ./internal/...
```

## Database and Redis integration

Use disposable local services. The database role needs permission to create and
drop test schemas. Both variables are intentional: the original integration
package uses `DATABASE_URL`, while newer isolated store tests use
`ATLAS_TEST_DATABASE_URL`. Redis limiter tests use `REDIS_ADDR`.

```sh
export DATABASE_URL='postgres://USER:PASSWORD@localhost:5432/atlas?sslmode=disable'
export ATLAS_TEST_DATABASE_URL="$DATABASE_URL"
export REDIS_ADDR='localhost:6379'
go test -race -count=1 ./...
go vet ./...
```

Tests without these services may skip integration checks. A short green test
run with no service variables is not the full verification boundary. The CI
services supply both PostgreSQL variables and Redis; process experiments run
separately because they start and kill executables and inject transport faults.

## Concurrency and draining

Real-database tests run many worker pools against the same scheduler/store.
They check unique live claims, attempt accounting, resource limits, draining
workers' exclusion from new placement, and renewal while existing work drains.
Race detection checks memory synchronization in addition to database ownership.

These tests assert exclusivity within valid ownership. They do not assert that
an expired, disconnected handler can never overlap with its successor.

## Process failure tests

The [experiment runbook](EXPERIMENT-RUNBOOK.md) launches isolated API, scheduler,
and worker processes. [CHAOS.md](CHAOS.md) records expected and observed behavior
for SIGKILL, side-effect-before-completion, mid-lease loss, timeout, and scoped
datastore transport failures. Fixtures use explicit barriers instead of hoping
that a sleep catches a particular crash window.

All process IDs and schemas created by the harness are tracked and cleaned up.
Fault injection targets owned proxies or owned processes, not unrelated local
services. Output retains failed scenarios instead of silently discarding them.

## Acceptance rules

Run formatting, unit/integration tests, race checks, vet, and CI-matching lint
before a code commit. Render core, NVIDIA, and KEDA manifests with
`./k8s/validate.ps1`. Load testing additionally requires raw trial data, a stated
machine/configuration, consistent metric definitions, and an honest distinction
between simulation, transport isolation, and actual service restart.
