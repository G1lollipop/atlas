# Runtime baseline runbook

`scripts/baseline.ps1` runs the same runtime checks against an original source tree
and a renamed source tree. It submits real jobs through the JWT-protected HTTP API,
observes their rows in PostgreSQL, captures service logs, writes a JSON report, and
removes only the named Compose project when it finishes.

Use PowerShell 7 or later with Docker Desktop and Compose v2. Run the original and
renamed snapshots sequentially, with different lowercase project names:

```powershell
pwsh -NoProfile -File .\scripts\baseline.ps1 `
  -SourceRoot .\.baseline\original `
  -ProjectName atlas-baseline-original

pwsh -NoProfile -File .\scripts\baseline.ps1 `
  -SourceRoot . `
  -ProjectName atlas-baseline-renamed
```

The runner builds the API, worker, and scheduler images from each `SourceRoot` by
default. Add `-SkipBuild` if the matching `<ProjectName>-api`, `<ProjectName>-worker`,
and `<ProjectName>-scheduler` images are already present locally; a `docker compose
build` with the same project name produces those tags. `-TimeoutSeconds` adjusts the
general wait limit (default 180 seconds).

The standalone Compose file uses a fresh `atlas` PostgreSQL database, the explicitly
configured `atlas` database user and password, distinct worker IDs, and ephemeral
localhost ports for the API and metrics endpoints. Its workers use six-second leases;
normal handlers stay below that duration. The script fails if its requested project
already has containers or a database volume, so a prior run cannot be silently reused.

The checks cover scheduler promotion before workers start, API job completion, a
locked queue head with a lower-priority run advancing under `SKIP LOCKED`, work shared
by both workers with one audited lease per batch run, SIGKILL lease recovery through
the worker janitor, retry exhaustion and dead-letter insertion, and scheduler leader
failover verified by metrics and the PostgreSQL advisory-lock PID.

Each invocation writes `<ProjectName>.json` and `<ProjectName>.log` under
`.baseline/reports/` by default. The JSON contains pass/fail assertions and SQL
snapshots; the log contains the real container logs captured before cleanup. These
files are runtime evidence; this runbook does not claim either source snapshot has
passed until the reports exist and show that result.

## Recreate the original source snapshot

The source revision is recorded in `docs/PROVENANCE.md`. In a fresh checkout that
contains that revision in its Git history, create the ignored snapshot before
running the original-source command above. Use a new destination if one already
exists; do not overwrite captured baseline evidence.

```powershell
New-Item -ItemType Directory -Force .baseline | Out-Null
git archive --format=zip --output=.baseline/original.zip 64bb4d9d50bc61045f49abf2558a151c7e5973d8
Expand-Archive -LiteralPath .baseline/original.zip -DestinationPath .baseline/original
```

Use a different `-ReportDir` for subsequent runs to preserve earlier JSON and logs.
The measured runs and their evidence paths are listed in `docs/BASELINE-RESULTS.md`.

## Full local stack

The verified stack remains running under project `atlas-stack-verified`; the earlier
failed `atlas` project's containers are stopped and its volumes are preserved. Keep
using the explicit project name to avoid starting a second stack on the same ports.

```powershell
docker compose -p atlas-stack-verified up -d --build
$env:COMPOSE_PROJECT_NAME = 'atlas-stack-verified'
pwsh -NoProfile -File .\scripts\stack-smoke.ps1
Remove-Item Env:COMPOSE_PROJECT_NAME
```

The smoke script checks the existing stack and writes
`.baseline/reports/stack-smoke.json`; it does not start or stop services. API health
is at `http://localhost:8080/healthz`, Grafana at `http://localhost:3000`, Prometheus
at `http://localhost:9093`, and Jaeger at `http://localhost:16686`. To stop services
while preserving their data, run `docker compose -p atlas-stack-verified stop`.
