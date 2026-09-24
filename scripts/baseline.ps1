[CmdletBinding()]
param(
    [string] $SourceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string] $ProjectName = "",
    [string] $ReportDir = (Join-Path $PSScriptRoot "..\.baseline\reports"),
    [switch] $SkipBuild,
    [ValidateRange(30, 600)]
    [int] $TimeoutSeconds = 180
)

# Reproducible runtime baseline for the original service and its renamed copy.
# Requires PowerShell 7+, Docker Desktop with Compose v2, and a working Docker engine.
# This script uses only a uniquely named Compose project and removes that project in
# finally. It never deletes containers, images, or volumes by broad name patterns.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($PSVersionTable.PSVersion.Major -lt 7) {
    throw "PowerShell 7 or later is required."
}

$SourceRoot = (Resolve-Path -LiteralPath $SourceRoot).Path
$ComposeFile = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "baseline.compose.yml")).Path
if ([string]::IsNullOrWhiteSpace($ProjectName)) {
    $uniqueSuffix = [Guid]::NewGuid().ToString("N").Substring(0, 8)
    $ProjectName = "atlas-baseline-$([DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss'))-$uniqueSuffix"
}
if ($ProjectName -notmatch "^[a-z0-9][a-z0-9_-]*$") {
    throw "ProjectName must use lowercase letters, digits, hyphens, or underscores and start with a letter or digit."
}

$script:StartedAt = [DateTime]::UtcNow
$script:Checks = [System.Collections.Generic.List[object]]::new()
$script:Snapshots = [System.Collections.Generic.List[object]]::new()
$script:ComposeStarted = $false
$script:SkipLockJob = $null
$script:JanitorContainer = "$ProjectName-janitor-observer"
$script:JanitorCreated = $false
$script:LogPath = $null
$script:SummaryPath = $null
$oldSourceRoot = $null
$oldJwtSecret = $null
$script:EnvironmentChanged = $false
$script:ApiBase = $null
$script:JwtSecret = "baseline-local-secret-change-me"
$script:Token = $null
$script:ApiPort = $null
$script:MetricPorts = @{}
$script:LeaderMetricName = $null
$script:Failure = $null

function Invoke-Docker {
    param(
        [Parameter(Mandatory = $true)] [string[]] $DockerArgs,
        [switch] $AllowFailure
    )

    $output = @(& docker @DockerArgs 2>&1)
    $exitCode = $LASTEXITCODE
    $text = [string]::Join([Environment]::NewLine, @($output | ForEach-Object { $_.ToString() }))
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "docker $($DockerArgs -join ' ') failed with exit code $exitCode`n$text"
    }
    return [pscustomobject]@{ ExitCode = $exitCode; Text = $text }
}

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)] [string[]] $ComposeArgs,
        [switch] $AllowFailure
    )

    $argsForDocker = @("compose", "--project-name", $ProjectName, "--file", $ComposeFile) + $ComposeArgs
    return Invoke-Docker -DockerArgs $argsForDocker -AllowFailure:$AllowFailure
}

function Invoke-DbText {
    param([Parameter(Mandatory = $true)] [string] $Sql)

    $result = Invoke-Compose -ComposeArgs @(
        "exec", "-T", "postgres", "psql", "-U", "atlas", "-d", "atlas",
        "-v", "ON_ERROR_STOP=1", "-At", "-c", $Sql
    )
    $lines = @($result.Text -split "`r?`n" | Where-Object { $_ -notmatch '^WARNING: Error loading config file:' })
    return ([string]::Join([Environment]::NewLine, $lines)).Trim()
}

function Add-Check {
    param(
        [Parameter(Mandatory = $true)] [string] $Name,
        [Parameter(Mandatory = $true)] [bool] $Passed,
        [Parameter(Mandatory = $true)] [AllowNull()] [AllowEmptyCollection()] $Details
    )

    $script:Checks.Add([ordered]@{
        name = $Name
        passed = $Passed
        details = $Details
        atUtc = [DateTime]::UtcNow.ToString("o")
    })
    if (-not $Passed) {
        throw "Baseline assertion failed: $Name. Details: $($Details | ConvertTo-Json -Depth 12 -Compress)"
    }
}

function Save-SqlSnapshot {
    param(
        [Parameter(Mandatory = $true)] [string] $Name,
        [Parameter(Mandatory = $true)] [string] $Sql
    )

    $text = Invoke-DbText -Sql $Sql
    $script:Snapshots.Add([ordered]@{
        name = $Name
        query = $Sql
        output = $text
        atUtc = [DateTime]::UtcNow.ToString("o")
    })
    return $text
}

function Wait-Until {
    param(
        [Parameter(Mandatory = $true)] [scriptblock] $Condition,
        [Parameter(Mandatory = $true)] [string] $Description,
        [int] $Seconds = $TimeoutSeconds,
        [int] $IntervalMilliseconds = 200
    )

    $watch = [Diagnostics.Stopwatch]::StartNew()
    while ($watch.Elapsed.TotalSeconds -lt $Seconds) {
        $value = & $Condition
        if ($value) { return $value }
        Start-Sleep -Milliseconds $IntervalMilliseconds
    }
    throw "Timed out waiting for $Description after $Seconds seconds."
}

function Get-HostPort {
    param([Parameter(Mandatory = $true)] [string] $Service, [int] $ContainerPort = 9090)

    $mapped = Invoke-Compose -ComposeArgs @("port", $Service, "$ContainerPort")
    $match = [regex]::Match($mapped.Text, ":(?<port>[0-9]+)\s*$")
    if (-not $match.Success) {
        throw "Could not read the randomly assigned host port for ${Service}:${ContainerPort}. Compose returned: $($mapped.Text)"
    }
    return [int] $match.Groups["port"].Value
}

function Get-MetricValue {
    param([Parameter(Mandatory = $true)] [string] $Service, [Parameter(Mandatory = $true)] [string] $Metric)

    # Re-read the Compose-assigned port on each scrape, so the harness follows the
    # active endpoint after a service replacement or restart.
    $port = Get-HostPort -Service $Service -ContainerPort 9090
    $script:MetricPorts[$Service] = $port
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$port/metrics" -TimeoutSec 2
        $pattern = "(?m)^$([regex]::Escape($Metric))(?:\{[^}]*\})?\s+(?<value>[-+0-9.eE]+)\s*$"
        $match = [regex]::Match([string] $response.Content, $pattern)
        if ($match.Success) { return [double]::Parse($match.Groups["value"].Value, [Globalization.CultureInfo]::InvariantCulture) }
    }
    catch { return $null }
    return $null
}

function Get-LeaderServices {
    $leaders = [System.Collections.Generic.List[string]]::new()
    foreach ($service in @("scheduler-one", "scheduler-two")) {
        $value = Get-MetricValue -Service $service -Metric $script:LeaderMetricName
        if ($null -ne $value -and $value -eq 1) { $leaders.Add($service) }
    }
    return $leaders.ToArray()
}

function Get-RunRowsForJobs {
    param([Parameter(Mandatory = $true)] [string[]] $JobIds)

    $idList = (@($JobIds | ForEach-Object { "'$_'::uuid" })) -join ","
    $sql = "SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json)::text FROM (SELECT id::text, job_id::text, status, attempt, leased_by, lease_expires_at, result, error FROM job_runs WHERE job_id IN ($idList) ORDER BY created_at, id) r;"
    $raw = Invoke-DbText -Sql $sql
    if ([string]::IsNullOrWhiteSpace($raw)) { return @() }
    return @((ConvertFrom-Json -InputObject $raw))
}

function Wait-RunStatus {
    param(
        [Parameter(Mandatory = $true)] [string] $JobId,
        [Parameter(Mandatory = $true)] [string[]] $Statuses,
        [int] $Seconds = $TimeoutSeconds
    )

    $last = $null
    $watch = [Diagnostics.Stopwatch]::StartNew()
    while ($watch.Elapsed.TotalSeconds -lt $Seconds) {
        $rows = @(Get-RunRowsForJobs -JobIds @($JobId))
        if ($rows.Count -gt 0) {
            $last = $rows[0]
            if ($Statuses -contains [string] $last.status) { return $last }
        }
        Start-Sleep -Milliseconds 150
    }
    $lastJson = if ($null -eq $last) { "no run row" } else { $last | ConvertTo-Json -Depth 10 -Compress }
    throw "Timed out waiting for job $JobId run status [$($Statuses -join ',')]. Last: $lastJson"
}

function Wait-JobsSucceeded {
    param([Parameter(Mandatory = $true)] [string[]] $JobIds, [int] $Seconds = $TimeoutSeconds)

    $watch = [Diagnostics.Stopwatch]::StartNew()
    $last = @()
    while ($watch.Elapsed.TotalSeconds -lt $Seconds) {
        $last = @(Get-RunRowsForJobs -JobIds $JobIds)
        if ($last.Count -eq $JobIds.Count -and @($last | Where-Object { $_.status -ne "succeeded" }).Count -eq 0) {
            return $last
        }
        if (@($last | Where-Object { $_.status -eq "dead" -or $_.status -eq "failed" }).Count -gt 0) {
            throw "One or more jobs failed before succeeding: $($last | ConvertTo-Json -Depth 10 -Compress)"
        }
        Start-Sleep -Milliseconds 200
    }
    throw "Timed out waiting for jobs to succeed. Last: $($last | ConvertTo-Json -Depth 10 -Compress)"
}

function New-ApiToken {
    param([Parameter(Mandatory = $true)] [string] $Secret)

    $utf8 = [Text.UTF8Encoding]::new($false)
    $encode = {
        param([string] $Value)
        [Convert]::ToBase64String($utf8.GetBytes($Value)).TrimEnd("=").Replace("+", "-").Replace("/", "_")
    }
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $header = & $encode '{"alg":"HS256","typ":"JWT"}'
    $claims = & $encode (@{ sub = "baseline-harness"; iat = $now; exp = $now + 3600 } | ConvertTo-Json -Compress)
    $signingInput = "$header.$claims"
    $hmac = [Security.Cryptography.HMACSHA256]::new($utf8.GetBytes($Secret))
    try {
        $signature = [Convert]::ToBase64String($hmac.ComputeHash($utf8.GetBytes($signingInput))).TrimEnd("=").Replace("+", "-").Replace("/", "_")
    }
    finally { $hmac.Dispose() }
    return "$signingInput.$signature"
}

function Submit-ApiJob {
    param([Parameter(Mandatory = $true)] [hashtable] $Body)

    $json = $Body | ConvertTo-Json -Depth 20 -Compress
    return Invoke-RestMethod -Method Post -Uri "$script:ApiBase/v1/jobs" `
        -Headers @{ Authorization = "Bearer $script:Token" } `
        -ContentType "application/json" -Body $json -TimeoutSec 15
}

function Install-TransitionAudit {
    $sql = @'
CREATE SCHEMA IF NOT EXISTS baseline_harness;
CREATE TABLE IF NOT EXISTS baseline_harness.run_transitions (
    event_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    from_owner TEXT,
    to_owner TEXT,
    happened_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE OR REPLACE FUNCTION baseline_harness.capture_run_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO baseline_harness.run_transitions(run_id, from_status, to_status, from_owner, to_owner)
        VALUES (NEW.id, NULL, NEW.status, NULL, NEW.leased_by);
    ELSIF OLD.status IS DISTINCT FROM NEW.status OR OLD.leased_by IS DISTINCT FROM NEW.leased_by THEN
        INSERT INTO baseline_harness.run_transitions(run_id, from_status, to_status, from_owner, to_owner)
        VALUES (NEW.id, OLD.status, NEW.status, OLD.leased_by, NEW.leased_by);
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS baseline_capture_run_transition ON public.job_runs;
CREATE TRIGGER baseline_capture_run_transition
AFTER INSERT OR UPDATE ON public.job_runs
FOR EACH ROW EXECUTE FUNCTION baseline_harness.capture_run_transition();
'@
    [void] (Invoke-DbText -Sql $sql)
}

function Capture-Logs {
    if (-not $script:ComposeStarted) { return }
    try {
        $logs = Invoke-Compose -ComposeArgs @("logs", "--no-color", "--timestamps") -AllowFailure
        Set-Content -LiteralPath $script:LogPath -Value $logs.Text -Encoding utf8
    }
    catch {
        Add-Content -LiteralPath $script:LogPath -Value "Failed to capture Compose logs: $_" -Encoding utf8
    }
    if ($script:JanitorCreated) {
        $janitorLogs = Invoke-Docker -DockerArgs @("logs", "--timestamps", $script:JanitorContainer) -AllowFailure
        if (-not [string]::IsNullOrWhiteSpace($janitorLogs.Text)) {
            Add-Content -LiteralPath $script:LogPath -Value "`n===== janitor observer =====`n$($janitorLogs.Text)" -Encoding utf8
        }
    }
}

function Assert-ProjectNameUnused {
    $containers = Invoke-Docker -DockerArgs @("ps", "-aq", "--filter", "label=com.docker.compose.project=$ProjectName") -AllowFailure
    $containerIds = @($containers.Text -split "`r?`n" | Where-Object { $_ -match '^[a-fA-F0-9]{12,64}$' })
    if ($containerIds.Count -gt 0) {
        throw "Compose project '$ProjectName' already has containers. Choose another ProjectName so this run stays isolated."
    }
    $volumeName = "${ProjectName}_baseline-postgres"
    $volume = Invoke-Docker -DockerArgs @("volume", "inspect", $volumeName) -AllowFailure
    if ($volume.ExitCode -eq 0) {
        throw "Compose volume '$volumeName' already exists. Choose another ProjectName so this run uses a fresh database."
    }
    $networkName = "${ProjectName}_default"
    $network = Invoke-Docker -DockerArgs @("network", "inspect", $networkName) -AllowFailure
    if ($network.ExitCode -eq 0) {
        throw "Compose network '$networkName' already exists. Choose another ProjectName so this run stays isolated."
    }
    $janitor = Invoke-Docker -DockerArgs @("container", "inspect", $script:JanitorContainer) -AllowFailure
    if ($janitor.ExitCode -eq 0) {
        throw "Container '$($script:JanitorContainer)' already exists. Choose another ProjectName so this run stays isolated."
    }
}

try {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "Docker CLI was not found on PATH." }
    $version = Invoke-Docker -DockerArgs @("compose", "version")
    if ($version.ExitCode -ne 0) { throw "Docker Compose v2 is required." }
    Assert-ProjectNameUnused

    New-Item -ItemType Directory -Path $ReportDir -Force | Out-Null
    $ReportDir = (Resolve-Path -LiteralPath $ReportDir).Path
    $script:LogPath = Join-Path $ReportDir "$ProjectName.log"
    $script:SummaryPath = Join-Path $ReportDir "$ProjectName.json"
    if ((Test-Path -LiteralPath $script:LogPath) -or (Test-Path -LiteralPath $script:SummaryPath)) {
        throw "A report for '$ProjectName' already exists. Choose another ProjectName to preserve earlier evidence."
    }
    $oldSourceRoot = $env:SOURCE_ROOT
    $oldJwtSecret = $env:JWT_SECRET
    $script:EnvironmentChanged = $true
    $env:SOURCE_ROOT = $SourceRoot
    $env:JWT_SECRET = $script:JwtSecret

    $metricsPath = Join-Path $SourceRoot "internal\metrics\metrics.go"
    $metricsSource = Get-Content -LiteralPath $metricsPath -Raw
    $leaderMetric = [regex]::Match($metricsSource, 'Name:\s*"(?<name>[a-zA-Z_:][a-zA-Z0-9_:]*_scheduler_is_leader)"')
    if (-not $leaderMetric.Success) { throw "Could not find the scheduler leader metric in $metricsPath." }
    $script:LeaderMetricName = $leaderMetric.Groups["name"].Value

    if (-not $SkipBuild) {
        Write-Host "Building API, worker, and scheduler images from $SourceRoot"
        [void] (Invoke-Compose -ComposeArgs @("build", "api", "worker-one", "scheduler-one"))
    }
    $script:ComposeStarted = $true
    [void] (Invoke-Compose -ComposeArgs @("up", "-d", "postgres"))

    Wait-Until -Description "Postgres health" -Condition {
        $result = Invoke-Compose -ComposeArgs @("ps", "--format", "json", "postgres") -AllowFailure
        $result.Text -match '"Health"\s*:\s*"healthy"|healthy'
    } | Out-Null

    [void] (Invoke-Compose -ComposeArgs @("up", "-d", "api"))
    $script:ApiPort = Get-HostPort -Service "api" -ContainerPort 8080
    $script:ApiBase = "http://127.0.0.1:$($script:ApiPort)"
    $script:Token = New-ApiToken -Secret $script:JwtSecret
    Wait-Until -Description "API /healthz" -Condition {
        try { (Invoke-WebRequest -UseBasicParsing -Uri "$script:ApiBase/healthz" -TimeoutSec 2).StatusCode -eq 200 }
        catch { $false }
    } | Out-Null
    Install-TransitionAudit

    [void] (Invoke-Compose -ComposeArgs @("up", "-d", "scheduler-one", "scheduler-two"))
    foreach ($service in @("scheduler-one", "scheduler-two")) {
        $script:MetricPorts[$service] = Get-HostPort -Service $service -ContainerPort 9090
    }
    $initialLeader = Wait-Until -Description "one scheduler leader" -Condition {
        $leaders = @(Get-LeaderServices)
        if ($leaders.Count -eq 1) { return [string] $leaders[0] }
        return $null
    }
    Add-Check -Name "exactly-one-scheduler-leader" -Passed (-not [string]::IsNullOrWhiteSpace([string] $initialLeader)) -Details @{ leader = [string] $initialLeader; metric = $script:LeaderMetricName }
    [void] (Save-SqlSnapshot -Name "initial-advisory-locks" -Sql "SELECT COALESCE(json_agg(row_to_json(l)), '[]'::json)::text FROM (SELECT pid, classid::text, objid::text, objsubid, granted FROM pg_locks WHERE locktype='advisory' ORDER BY pid) l;")

    # Prove promotion occurs while no worker containers exist.
    $preWorkerJob = Submit-ApiJob -Body @{ name = "echo"; payload = @{ phase = "before-workers" }; max_attempts = 2; timeout_seconds = 10 }
    $preWorkerRun = Wait-RunStatus -JobId $preWorkerJob.id -Statuses @("pending")
    Add-Check -Name "scheduler-promotes-before-workers" -Passed ($preWorkerRun.status -eq "pending" -and $null -eq $preWorkerRun.leased_by) -Details $preWorkerRun
    [void] (Save-SqlSnapshot -Name "pending-before-workers" -Sql "SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json)::text FROM (SELECT id::text, job_id::text, status, attempt, leased_by, scheduled_at FROM job_runs WHERE job_id='$($preWorkerJob.id)'::uuid) r;")

    # Lock the highest-priority pending row in one PostgreSQL session. The workers
    # must claim the lower-priority row during the lock window, proving the deployed
    # query skips locked rows instead of stalling behind the queue head.
    $skipHead = Submit-ApiJob -Body @{ name = "sleep"; payload = @{ seconds = 1 }; priority = 32000; max_attempts = 1; timeout_seconds = 20 }
    $skipNext = Submit-ApiJob -Body @{ name = "sleep"; payload = @{ seconds = 1.2 }; priority = 31999; max_attempts = 1; timeout_seconds = 20 }
    $skipHeadRun = Wait-RunStatus -JobId $skipHead.id -Statuses @("pending")
    $skipNextRun = Wait-RunStatus -JobId $skipNext.id -Statuses @("pending")
    $lockSql = "BEGIN; SELECT id FROM job_runs WHERE id='$($skipHeadRun.id)'::uuid FOR UPDATE; SELECT pg_sleep(10); COMMIT;"
    $script:SkipLockJob = Start-Job -Name "$ProjectName-skip-lock" -ScriptBlock {
        param($composeProject, $composePath, $sql)
        $out = & docker compose --project-name $composeProject --file $composePath exec -T -e PGAPPNAME=baseline-skip-lock postgres psql -U atlas -d atlas -v ON_ERROR_STOP=1 -c $sql 2>&1
        if ($LASTEXITCODE -ne 0) { throw "PostgreSQL lock transaction failed: $out" }
        $out
    } -ArgumentList @($ProjectName, $ComposeFile, $lockSql)
    Wait-Until -Description "SQL transaction holding the queue head" -Seconds 12 -Condition {
        (Invoke-DbText -Sql "SELECT count(*) FROM pg_stat_activity WHERE application_name='baseline-skip-lock' AND state='active' AND wait_event='PgSleep';") -eq "1"
    } | Out-Null
    # Start both uniquely identified workers only while the pending head is locked.
    [void] (Invoke-Compose -ComposeArgs @("up", "-d", "worker-one", "worker-two"))
    foreach ($service in @("worker-one", "worker-two")) {
        $script:MetricPorts[$service] = Get-HostPort -Service $service -ContainerPort 9090
    }
    Wait-Until -Description "lower-priority row advances while locked head stays pending" -Seconds 8 -Condition {
        (Invoke-DbText -Sql "SELECT count(*) FROM baseline_harness.run_transitions t JOIN job_runs h ON h.id='$($skipHeadRun.id)'::uuid WHERE t.run_id='$($skipNextRun.id)'::uuid AND t.to_status='running' AND h.status='pending' AND EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name='baseline-skip-lock' AND state='active' AND wait_event='PgSleep');") -eq "1"
    } | Out-Null
    $nextRows = @(Get-RunRowsForJobs -JobIds @($skipNext.id))
    $headRows = @(Get-RunRowsForJobs -JobIds @($skipHead.id))
    $nextDuringLock = $nextRows[0]
    $headWhileLocked = $headRows[0]
    Add-Check -Name "workers-skip-a-locked-queue-head" -Passed ($headWhileLocked.status -eq "pending" -and @("running", "succeeded") -contains $nextDuringLock.status -and $nextDuringLock.id -eq $skipNextRun.id) -Details @{ lockedHead = $headWhileLocked; lowerPriorityRun = $nextDuringLock; auditedRunningTransition = $true }
    [void] (Save-SqlSnapshot -Name "skip-locked-proof-during-head-lock" -Sql "SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json)::text FROM (SELECT id::text, job_id::text, status, priority, leased_by FROM job_runs WHERE job_id IN ('$($skipHead.id)'::uuid, '$($skipNext.id)'::uuid) ORDER BY priority DESC) r;")
    $lockFinished = Wait-Job -Job $script:SkipLockJob -Timeout 18
    if ($null -eq $lockFinished) { throw "Queue-head lock transaction did not finish." }
    $lockOutput = Receive-Job -Job $script:SkipLockJob -ErrorAction Stop | Out-String
    Remove-Job -Job $script:SkipLockJob -Force
    $script:SkipLockJob = $null
    [void] (Wait-JobsSucceeded -JobIds @($preWorkerJob.id, $skipHead.id, $skipNext.id) -Seconds 30)

    # Exercise actual work sharing with two workers and long enough handlers that
    # both processes get opportunities to lease jobs.
    $batchJobs = [System.Collections.Generic.List[string]]::new()
    for ($index = 1; $index -le 8; $index++) {
        $marker = "batch-{0:d2}" -f $index
        $job = Submit-ApiJob -Body @{ name = "sleep"; payload = @{ marker = $marker; seconds = 0.7 }; priority = 100; max_attempts = 1; timeout_seconds = 20 }
        $batchJobs.Add([string] $job.id)
    }
    $batchRuns = @(Wait-JobsSucceeded -JobIds @($batchJobs.ToArray()) -Seconds 45)
    $owners = @($batchRuns | ForEach-Object { [string] $_.leased_by } | Sort-Object -Unique)
    Add-Check -Name "two-workers-share-a-job-batch" -Passed ($owners -contains "baseline-worker-one" -and $owners -contains "baseline-worker-two") -Details @{ workerIds = $owners; runCount = $batchRuns.Count; runs = $batchRuns }

    $batchIdSql = (@($batchJobs.ToArray() | ForEach-Object { "'$_'::uuid" })) -join ","
    $batchAuditSql = "SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json)::text FROM (SELECT j.id::text AS run_id, j.status, j.attempt, j.leased_by, count(t.event_id) FILTER (WHERE t.to_status='leased') AS lease_events, count(DISTINCT t.to_owner) FILTER (WHERE t.to_status='leased') AS lease_owner_count FROM job_runs j LEFT JOIN baseline_harness.run_transitions t ON t.run_id=j.id WHERE j.job_id IN ($batchIdSql) GROUP BY j.id ORDER BY j.created_at) r;"
    $batchAuditRows = @(ConvertFrom-Json -InputObject (Save-SqlSnapshot -Name "two-worker-batch-lease-counts" -Sql $batchAuditSql))
    $batchLeaseRowsOk = $batchAuditRows.Count -eq 8 -and @($batchAuditRows | Where-Object { $_.attempt -ne 1 -or $_.lease_events -ne 1 -or $_.lease_owner_count -ne 1 -or $null -eq $_.leased_by -or $_.status -ne "succeeded" }).Count -eq 0
    Add-Check -Name "each-batch-run-has-one-exclusive-owner-one-lease-and-one-attempt" -Passed $batchLeaseRowsOk -Details @{ runCount = $batchAuditRows.Count; runs = $batchAuditRows }

    $invalidLeases = Invoke-DbText -Sql "SELECT count(*) FROM baseline_harness.run_transitions WHERE to_status='leased' AND (from_status IS DISTINCT FROM 'pending' OR to_owner IS NULL);"
    Add-Check -Name "lease-audit-has-no-double-claim-transition" -Passed ($invalidLeases -eq "0") -Details @{ invalidTransitionCount = [int] $invalidLeases; meaning = "every lease begins with one pending-to-leased row transition and has one recorded owner" }
    [void] (Save-SqlSnapshot -Name "two-worker-batch-ownership" -Sql "SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json)::text FROM (SELECT id::text, job_id::text, status, attempt, leased_by FROM job_runs WHERE job_id IN ($batchIdSql) ORDER BY created_at) r;")

    # Hard-kill a worker during a handler shorter than the six-second lease. The
    # zero-concurrency observer leaves time for the kill round trip and runs the
    # production janitor without claiming the reclaimed run.
    [void] (Invoke-Compose -ComposeArgs @("stop", "-t", "2", "worker-two"))
    $crashJob = Submit-ApiJob -Body @{ name = "sleep"; payload = @{ seconds = 4 }; max_attempts = 2; timeout_seconds = 15 }
    $crashRunning = Wait-RunStatus -JobId $crashJob.id -Statuses @("running") -Seconds 15
    Add-Check -Name "crash-target-is-running-on-worker-one" -Passed ($crashRunning.leased_by -eq "baseline-worker-one") -Details $crashRunning
    [void] (Invoke-Compose -ComposeArgs @("kill", "-s", "SIGKILL", "worker-one"))
    $script:JanitorCreated = $true
    [void] (Invoke-Compose -ComposeArgs @("run", "-d", "--no-deps", "--name", $script:JanitorContainer, "-e", "WORKER_ID=baseline-janitor-observer", "-e", "WORKER_CONCURRENCY=0", "worker-two"))
    $reclaimed = Wait-RunStatus -JobId $crashJob.id -Statuses @("pending") -Seconds 15
    Add-Check -Name "janitor-reclaims-expired-crashed-worker-lease" -Passed ($reclaimed.status -eq "pending" -and $reclaimed.attempt -eq 1 -and $null -eq $reclaimed.leased_by) -Details $reclaimed
    [void] (Save-SqlSnapshot -Name "lease-after-janitor-reclaim" -Sql "SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json)::text FROM (SELECT id::text, status, attempt, leased_by, lease_expires_at FROM job_runs WHERE job_id='$($crashJob.id)'::uuid) r;")
    $observerLogs = Invoke-Docker -DockerArgs @("logs", "--timestamps", $script:JanitorContainer) -AllowFailure
    if (-not [string]::IsNullOrWhiteSpace($observerLogs.Text)) {
        Add-Content -LiteralPath $script:LogPath -Value "`n===== janitor observer before restart =====`n$($observerLogs.Text)" -Encoding utf8
    }
    [void] (Invoke-Docker -DockerArgs @("stop", $script:JanitorContainer) -AllowFailure)
    [void] (Invoke-Docker -DockerArgs @("rm", "-f", $script:JanitorContainer) -AllowFailure)
    $script:JanitorCreated = $false
    [void] (Invoke-Compose -ComposeArgs @("up", "-d", "worker-two"))
    $crashComplete = Wait-RunStatus -JobId $crashJob.id -Statuses @("succeeded") -Seconds 25
    Add-Check -Name "reclaimed-run-is-reexecuted" -Passed ($crashComplete.attempt -eq 2 -and $crashComplete.leased_by -eq "baseline-worker-two") -Details $crashComplete
    $crashAuditText = Save-SqlSnapshot -Name "crashed-run-transition-sequence" -Sql "SELECT COALESCE(json_agg(row_to_json(t) ORDER BY event_id), '[]'::json)::text FROM (SELECT event_id, from_status, to_status, from_owner, to_owner, happened_at FROM baseline_harness.run_transitions WHERE run_id=(SELECT id FROM job_runs WHERE job_id='$($crashJob.id)'::uuid)) t;"
    $crashAuditRows = @(ConvertFrom-Json -InputObject $crashAuditText)
    $crashSequence = @($crashAuditRows | ForEach-Object { [string] $_.to_status }) -join ","
    Add-Check -Name "crash-audit-shows-expiry-reclaim-and-reexecution" -Passed ($crashSequence -eq "pending,leased,running,pending,leased,running,succeeded") -Details @{ expected = "pending,leased,running,pending,leased,running,succeeded"; observed = $crashSequence; events = $crashAuditRows }

    # A real handler timeout fails once, retries after backoff, then reaches the
    # durable dead-letter table after the configured second attempt.
    $retryJob = Submit-ApiJob -Body @{ name = "sleep"; payload = @{ seconds = 5 }; max_attempts = 2; timeout_seconds = 1 }
    $deadRun = Wait-RunStatus -JobId $retryJob.id -Statuses @("dead") -Seconds 30
    $deadLetters = Invoke-DbText -Sql "SELECT count(*) FROM dead_letters WHERE job_run_id='$($deadRun.id)'::uuid;"
    Add-Check -Name "retry-exhaustion-produces-dead-letter" -Passed ($deadRun.attempt -eq 2 -and $deadLetters -eq "1") -Details @{ run = $deadRun; deadLetterCount = [int] $deadLetters }
    [void] (Save-SqlSnapshot -Name "retry-dead-letter" -Sql "SELECT json_build_object('run', (SELECT row_to_json(r) FROM (SELECT id::text, status, attempt, error FROM job_runs WHERE id='$($deadRun.id)'::uuid) r), 'dead_letters', (SELECT json_agg(row_to_json(d)) FROM (SELECT id::text, reason, payload FROM dead_letters WHERE job_run_id='$($deadRun.id)'::uuid) d))::text;")

    # Observe the advisory lock holder through both per-scheduler metrics and SQL,
    # kill that exact leader, then prove the standby can promote and execute a job.
    $leadersBeforeFailover = @(Wait-Until -Description "one scheduler leader before failover" -Condition {
        $leaders = @(Get-LeaderServices)
        if ($leaders.Count -eq 1) { return $leaders }
        return $null
    })
    $oldLeader = [string] $leadersBeforeFailover[0]
    $standby = if ($oldLeader -eq "scheduler-one") { "scheduler-two" } else { "scheduler-one" }
    $lockSnapshot = Save-SqlSnapshot -Name "advisory-lock-before-failover" -Sql "SELECT COALESCE(json_agg(row_to_json(l)), '[]'::json)::text FROM (SELECT pid, classid::text, objid::text, objsubid, granted FROM pg_locks WHERE locktype='advisory' ORDER BY pid) l;"
    $lockRows = @(ConvertFrom-Json -InputObject $lockSnapshot)
    $leaderLockRows = @($lockRows | Where-Object { $_.granted -eq $true -and $_.objid -eq "727433001" -and $_.objsubid -eq 1 })
    Add-Check -Name "scheduler-leader-holds-postgres-advisory-lock" -Passed ($leaderLockRows.Count -eq 1) -Details $lockRows
    $oldLeaderPid = [int] $leaderLockRows[0].pid
    [void] (Invoke-Compose -ComposeArgs @("kill", "-s", "SIGKILL", $oldLeader))
    Wait-Until -Description "standby scheduler takes leadership" -Seconds 20 -Condition {
        (Get-MetricValue -Service $standby -Metric $script:LeaderMetricName) -eq 1
    } | Out-Null
    $postFailoverLocksSql = "SELECT json_build_object('locks', (SELECT COALESCE(json_agg(row_to_json(l)), '[]'::json) FROM (SELECT pid, classid::text, objid::text, objsubid, granted FROM pg_locks WHERE locktype='advisory' ORDER BY pid) l), 'old_pid_active', EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$oldLeaderPid))::text;"
    $postFailoverLocks = ConvertFrom-Json -InputObject (Save-SqlSnapshot -Name "advisory-lock-after-failover" -Sql $postFailoverLocksSql)
    $newLeaderLockRows = @($postFailoverLocks.locks | Where-Object { $_.granted -eq $true -and $_.objid -eq "727433001" -and $_.objsubid -eq 1 })
    $failoverMetricIsOne = (Get-MetricValue -Service $standby -Metric $script:LeaderMetricName) -eq 1
    $lockPidChanged = $newLeaderLockRows.Count -eq 1 -and [int] $newLeaderLockRows[0].pid -ne $oldLeaderPid
    Add-Check -Name "standby-takes-leadership-after-sigkill" -Passed ($failoverMetricIsOne -and $lockPidChanged -and -not $postFailoverLocks.old_pid_active) -Details @{ killedLeader = $oldLeader; oldLeaderPid = $oldLeaderPid; newLeader = $standby; newLeaderPid = if ($newLeaderLockRows.Count -eq 1) { $newLeaderLockRows[0].pid } else { $null }; oldPidActive = $postFailoverLocks.old_pid_active; metricValue = $failoverMetricIsOne }
    $failoverJob = Submit-ApiJob -Body @{ name = "echo"; payload = @{ phase = "after-scheduler-failover" }; max_attempts = 1; timeout_seconds = 10 }
    $failoverRun = Wait-RunStatus -JobId $failoverJob.id -Statuses @("succeeded") -Seconds 20
    Add-Check -Name "new-leader-promotes-and-worker-completes-job" -Passed ($failoverRun.status -eq "succeeded") -Details $failoverRun

    [void] (Save-SqlSnapshot -Name "final-run-state-counts" -Sql "SELECT COALESCE(json_agg(row_to_json(s)), '[]'::json)::text FROM (SELECT status, count(*) AS run_count FROM job_runs GROUP BY status ORDER BY status) s;")
    [void] (Save-SqlSnapshot -Name "final-transition-audit" -Sql "SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json)::text FROM (SELECT run_id::text, from_status, to_status, from_owner, to_owner, happened_at FROM baseline_harness.run_transitions ORDER BY event_id) t;")
}
catch {
    $script:Failure = $_.Exception.Message
    Write-Warning $script:Failure
}
finally {
    if ($null -ne $script:SkipLockJob) {
        Stop-Job -Job $script:SkipLockJob -ErrorAction SilentlyContinue
        Remove-Job -Job $script:SkipLockJob -Force -ErrorAction SilentlyContinue
    }

    if ($script:ComposeStarted) {
        try { Capture-Logs } catch { $script:Failure = if ($script:Failure) { "$script:Failure; log capture: $_" } else { "log capture: $_" } }
        if ($script:JanitorCreated) {
            [void] (Invoke-Docker -DockerArgs @("rm", "-f", $script:JanitorContainer) -AllowFailure)
        }
        $down = Invoke-Compose -ComposeArgs @("down", "--volumes", "--remove-orphans") -AllowFailure
        if ($down.ExitCode -ne 0) {
            $script:Failure = if ($script:Failure) { "$script:Failure; cleanup: $($down.Text)" } else { "cleanup: $($down.Text)" }
        }
    }
    if ($script:EnvironmentChanged) {
        if ($null -ne $oldSourceRoot) { $env:SOURCE_ROOT = $oldSourceRoot } else { Remove-Item Env:SOURCE_ROOT -ErrorAction SilentlyContinue }
        if ($null -ne $oldJwtSecret) { $env:JWT_SECRET = $oldJwtSecret } else { Remove-Item Env:JWT_SECRET -ErrorAction SilentlyContinue }
    }

    if ($null -ne $script:SummaryPath) {
        $summary = [ordered]@{
            schemaVersion = 1
            sourceRoot = $SourceRoot
            projectName = $ProjectName
            startedAtUtc = $script:StartedAt.ToString("o")
            finishedAtUtc = [DateTime]::UtcNow.ToString("o")
            outcome = if ($null -eq $script:Failure) { "passed" } else { "failed" }
            error = $script:Failure
            checks = $script:Checks.ToArray()
            sqlSnapshots = $script:Snapshots.ToArray()
            logsPath = $script:LogPath
        }
        Set-Content -LiteralPath $script:SummaryPath -Value ($summary | ConvertTo-Json -Depth 30) -Encoding utf8
        Write-Host "Runtime baseline $($summary.outcome): $($script:SummaryPath)"
        if ($script:LogPath -and (Test-Path -LiteralPath $script:LogPath)) { Write-Host "Container logs: $($script:LogPath)" }
    }
}

if ($null -ne $script:Failure) { exit 1 }
