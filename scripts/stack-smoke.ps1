[CmdletBinding()]
param(
    [string] $ApiUrl = "http://127.0.0.1:8080",
    [string] $ApiMetricsUrl = "http://127.0.0.1:9090",
    [string] $WorkerMetricsUrl = "http://127.0.0.1:9091",
    [string] $SchedulerMetricsUrl = "http://127.0.0.1:9092",
    [string] $PrometheusUrl = "http://127.0.0.1:9093",
    [string] $GrafanaUrl = "http://127.0.0.1:3000",
    [string] $JaegerUrl = "http://127.0.0.1:16686",
    [string] $JwtSecret = $(if ($env:JWT_SECRET) { $env:JWT_SECRET } else { "dev-secret-change-me" }),
    [string] $GrafanaUser = "admin",
    [string] $GrafanaPassword = $(if ($env:GRAFANA_ADMIN_PASSWORD) { $env:GRAFANA_ADMIN_PASSWORD } else { "admin" }),
    [ValidateRange(10, 600)]
    [int] $TimeoutSeconds = 90,
    [ValidateRange(1, 15)]
    [int] $PollIntervalSeconds = 2,
    [string] $EvidencePath = (Join-Path $PSScriptRoot "..\.baseline\reports\stack-smoke.json")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:StartedAtUtc = [DateTime]::UtcNow
$script:Checks = [System.Collections.Generic.List[object]]::new()
$script:Failure = $null
$script:RepoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$script:ComposeFile = Join-Path $script:RepoRoot "docker-compose.yml"

function Add-Check {
    param(
        [Parameter(Mandatory = $true)] [string] $Name,
        [Parameter(Mandatory = $true)] [bool] $Passed,
        [Parameter(Mandatory = $true)] $Details
    )

    $script:Checks.Add([pscustomobject]@{
        name = $Name
        passed = $Passed
        details = $Details
        atUtc = [DateTime]::UtcNow.ToString("o")
    }) | Out-Null

    if (-not $Passed) {
        throw "Stack smoke assertion failed: $Name. Details: $($Details | ConvertTo-Json -Depth 12 -Compress)"
    }
}

function Invoke-Http {
    param(
        [Parameter(Mandatory = $true)] [string] $Uri,
        [string] $Method = "GET",
        [hashtable] $Headers,
        [string] $Body,
        [string] $ContentType = "application/json"
    )

    $request = @{
        Uri = $Uri
        Method = $Method
        TimeoutSec = 10
        SkipHttpErrorCheck = $true
    }
    if ($null -ne $Headers) { $request.Headers = $Headers }
    if ($PSBoundParameters.ContainsKey("Body")) {
        $request.Body = $Body
        $request.ContentType = $ContentType
    }

    try {
        $response = Invoke-WebRequest @request
        return [pscustomobject]@{
            status = [int] $response.StatusCode
            content = [string] $response.Content
            error = $null
        }
    }
    catch {
        return [pscustomobject]@{
            status = 0
            content = ""
            error = $_.Exception.Message
        }
    }
}

function Convert-JsonContent {
    param([Parameter(Mandatory = $true)] $Response)

    try {
        return ConvertFrom-Json -InputObject $Response.content -ErrorAction Stop
    }
    catch {
        throw "Expected a JSON response, got HTTP $($Response.status): $($_.Exception.Message)"
    }
}

function Wait-ForProbe {
    param(
        [Parameter(Mandatory = $true)] [string] $Description,
        [Parameter(Mandatory = $true)] [scriptblock] $Probe,
        [int] $Seconds = $TimeoutSeconds
    )

    $watch = [Diagnostics.Stopwatch]::StartNew()
    $lastError = $null
    while ($watch.Elapsed.TotalSeconds -lt $Seconds) {
        try {
            $sample = & $Probe
            if ($null -ne $sample) { return $sample }
            $lastError = $null
        }
        catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Seconds $PollIntervalSeconds
    }

    $suffix = if ($lastError) { " Last probe error: $lastError" } else { "" }
    throw "Timed out waiting for $Description after $Seconds seconds.$suffix"
}

function New-JwtToken {
    param([Parameter(Mandatory = $true)] [string] $Secret)

    $utf8 = [Text.UTF8Encoding]::new($false)
    $toBase64Url = {
        param([byte[]] $Value)
        [Convert]::ToBase64String($Value).TrimEnd("=").Replace("+", "-").Replace("/", "_")
    }

    $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $headerJson = '{"alg":"HS256","typ":"JWT"}'
    $claimsJson = @{ sub = "atlas-stack-smoke"; iat = $now; exp = $now + 3600 } | ConvertTo-Json -Compress
    $headerBytes = $utf8.GetBytes($headerJson)
    $claimsBytes = $utf8.GetBytes($claimsJson)
    $header = & $toBase64Url $headerBytes
    $claims = & $toBase64Url $claimsBytes
    $signingInput = "$header.$claims"

    $hmac = [Security.Cryptography.HMACSHA256]::new($utf8.GetBytes($Secret))
    try {
        $signatureBytes = $hmac.ComputeHash($utf8.GetBytes($signingInput))
        $signature = & $toBase64Url $signatureBytes
    }
    finally {
        $hmac.Dispose()
    }

    return "$signingInput.$signature"
}

function Invoke-DockerText {
    param([Parameter(Mandatory = $true)] [string[]] $DockerArgs)

    $output = @(& docker @DockerArgs 2>&1)
    $exitCode = $LASTEXITCODE
    $text = [string]::Join([Environment]::NewLine, @($output | ForEach-Object { $_.ToString() })).Trim()
    if ($exitCode -ne 0) {
        throw "docker $($DockerArgs -join ' ') failed with exit code $exitCode. $text"
    }
    return $text
}

function Invoke-ComposeText {
    param([Parameter(Mandatory = $true)] [string[]] $ComposeArgs)

    $dockerArgs = @("compose", "--project-directory", $script:RepoRoot, "--file", $script:ComposeFile) + $ComposeArgs
    return Invoke-DockerText -DockerArgs $dockerArgs
}

function Get-PrometheusQuery {
    param([Parameter(Mandatory = $true)] [string] $Query)

    $encoded = [Uri]::EscapeDataString($Query)
    $response = Invoke-Http -Uri "$($PrometheusUrl.TrimEnd('/'))/api/v1/query?query=$encoded"
    if ($response.status -ne 200) { return $null }

    try {
        $body = Convert-JsonContent -Response $response
    }
    catch {
        return $null
    }
    if ($body.status -ne "success") { return $null }

    $results = @($body.data.result)
    if ($results.Count -eq 0) { return $null }
    return [pscustomobject]@{
        resultCount = $results.Count
        results = $results
    }
}

try {
    if ($PSVersionTable.PSVersion.Major -lt 7) {
        throw "PowerShell 7 or later is required."
    }
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "Docker CLI was not found on PATH."
    }
    if (-not (Test-Path -LiteralPath $script:ComposeFile)) {
        throw "Root docker-compose.yml was not found at $script:ComposeFile."
    }
    if ([string]::IsNullOrWhiteSpace($JwtSecret)) {
        throw "JWT secret must not be empty. Set JWT_SECRET or pass -JwtSecret."
    }

    $apiBase = $ApiUrl.TrimEnd('/')
    $health = Wait-ForProbe -Description "Atlas API health" -Probe {
        $response = Invoke-Http -Uri "$apiBase/healthz"
        if ($response.status -ne 200) { return $null }
        try { $body = Convert-JsonContent -Response $response } catch { return $null }
        if ($body.status -ne "ok") { return $null }
        return [pscustomobject]@{ status = $response.status; body = $body }
    }
    Add-Check -Name "api-health" -Passed ($health.status -eq 200 -and $health.body.status -eq "ok") -Details $health

    $token = New-JwtToken -Secret $JwtSecret
    $authHeaders = @{ Authorization = "Bearer $token" }
    $marker = "atlas-stack-smoke-$([guid]::NewGuid().ToString('N'))"
    $jobBody = @{
        name = "echo"
        payload = @{ smoke_marker = $marker }
        max_attempts = 1
        timeout_seconds = 30
    } | ConvertTo-Json -Depth 8 -Compress
    $createResponse = Invoke-Http -Uri "$apiBase/v1/jobs" -Method "POST" -Headers $authHeaders -Body $jobBody
    $createdJob = $null
    if ($createResponse.status -eq 201) {
        $createdJob = Convert-JsonContent -Response $createResponse
    }
    $hasJobID = $null -ne $createdJob -and -not [string]::IsNullOrWhiteSpace([string] $createdJob.id)
    Add-Check -Name "api-create-echo-job" -Passed ($createResponse.status -eq 201 -and $hasJobID) -Details @{
        status = $createResponse.status
        jobId = if ($hasJobID) { [string] $createdJob.id } else { $null }
    }
    if (-not $hasJobID) { throw "The API did not return a created job ID." }

    $jobID = [string] $createdJob.id
    $runObservation = Wait-ForProbe -Description "echo job to succeed through the API read path" -Probe {
        $response = Invoke-Http -Uri "$apiBase/v1/jobs/$jobID/runs?limit=20" -Headers $authHeaders
        if ($response.status -ne 200) { return $null }
        try { $runs = @(Convert-JsonContent -Response $response) } catch { return $null }
        $run = $runs | Where-Object { $_.job_id -eq $jobID } | Select-Object -First 1
        if ($null -eq $run) { return $null }
        if ($run.status -eq "succeeded" -or $run.status -eq "failed" -or $run.status -eq "dead") {
            return [pscustomobject]@{ run = $run }
        }
        return $null
    }
    $run = $runObservation.run
    $runSucceeded = $run.status -eq "succeeded"
    $echoPreservedPayload = $runSucceeded -and $run.result.smoke_marker -eq $marker
    Add-Check -Name "api-echo-job-succeeded" -Passed ($runSucceeded -and $echoPreservedPayload) -Details @{
        jobId = $jobID
        runId = [string] $run.id
        status = [string] $run.status
        attempt = [int] $run.attempt
        payloadReturned = [bool] $echoPreservedPayload
    }

    $streamingCount = Wait-ForProbe -Description "Postgres streaming replication" -Probe {
        $raw = Invoke-ComposeText -ComposeArgs @(
            "exec", "-T", "postgres", "psql", "-U", "atlas", "-d", "atlas", "-At", "-c",
            "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming';"
        )
        $count = 0
        if ([int]::TryParse($raw.Trim(), [ref] $count) -and $count -gt 0) {
            return [pscustomobject]@{ streamingConnections = $count }
        }
        return $null
    }
    Add-Check -Name "postgres-streaming-replication" -Passed ($streamingCount.streamingConnections -gt 0) -Details $streamingCount

    $replicaState = Wait-ForProbe -Description "Postgres replica recovery state" -Probe {
        $raw = Invoke-ComposeText -ComposeArgs @(
            "exec", "-T", "postgres-replica", "psql", "-U", "atlas", "-d", "atlas", "-At", "-c",
            "SELECT pg_is_in_recovery();"
        )
        if ($raw.Trim() -eq "t") { return [pscustomobject]@{ inRecovery = $true } }
        return $null
    }
    Add-Check -Name "postgres-replica-in-recovery" -Passed $replicaState.inRecovery -Details $replicaState

    $redis = Wait-ForProbe -Description "Redis PING" -Probe {
        $raw = Invoke-ComposeText -ComposeArgs @("exec", "-T", "redis", "redis-cli", "PING")
        if ($raw.Trim() -eq "PONG") { return [pscustomobject]@{ reply = "PONG" } }
        return $null
    }
    Add-Check -Name "redis-ping" -Passed ($redis.reply -eq "PONG") -Details $redis

    $targetStatus = Wait-ForProbe -Description "all three Atlas Prometheus targets to be up" -Probe {
        $response = Invoke-Http -Uri "$($PrometheusUrl.TrimEnd('/'))/api/v1/targets?state=active"
        if ($response.status -ne 200) { return $null }
        try { $body = Convert-JsonContent -Response $response } catch { return $null }
        if ($body.status -ne "success") { return $null }

        $targets = @($body.data.activeTargets)
        $expectedJobs = @("atlas-api", "atlas-worker", "atlas-scheduler")
        $found = @{}
        foreach ($job in $expectedJobs) {
            $matches = @($targets | Where-Object { $_.labels.job -eq $job })
            if ($matches.Count -gt 0) {
                $found[$job] = [string] $matches[0].health
            }
        }
        if ($found.Count -eq $expectedJobs.Count -and @($found.Values | Where-Object { $_ -ne "up" }).Count -eq 0) {
            return [pscustomobject]@{ jobs = $found }
        }
        return $null
    }
    Add-Check -Name "prometheus-atlas-targets-up" -Passed ($targetStatus.jobs.Count -eq 3) -Details $targetStatus

    $directMetrics = @(
        @{ name = "api"; url = "$($ApiMetricsUrl.TrimEnd('/'))/metrics"; pattern = '(?m)^atlas_jobs_submitted_total\s+' },
        @{ name = "worker"; url = "$($WorkerMetricsUrl.TrimEnd('/'))/metrics"; pattern = '(?m)^atlas_runs_completed_total(?:\{[^}]*\})?\s+' },
        @{ name = "scheduler"; url = "$($SchedulerMetricsUrl.TrimEnd('/'))/metrics"; pattern = '(?m)^atlas_scheduler_is_leader\s+1(?:\.0)?\s*$' }
    )
    foreach ($metric in $directMetrics) {
        $response = Wait-ForProbe -Description "$($metric.name) Atlas metrics" -Probe {
            $sample = Invoke-Http -Uri $metric.url
            if ($sample.status -eq 200 -and [regex]::IsMatch($sample.content, $metric.pattern)) {
                return [pscustomobject]@{ status = $sample.status; metricPattern = $metric.pattern }
            }
            return $null
        }
        Add-Check -Name "direct-metrics-$($metric.name)" -Passed ($response.status -eq 200) -Details $response
    }

    $submittedMetric = Wait-ForProbe -Description "Atlas job submission metric in Prometheus" -Probe {
        Get-PrometheusQuery -Query "atlas_jobs_submitted_total"
    }
    Add-Check -Name "prometheus-atlas-jobs-submitted-metric" -Passed ($submittedMetric.resultCount -gt 0) -Details $submittedMetric

    $completedMetric = Wait-ForProbe -Description "Atlas completed-run metric in Prometheus" -Probe {
        Get-PrometheusQuery -Query 'atlas_runs_completed_total{outcome="succeeded"}'
    }
    Add-Check -Name "prometheus-atlas-runs-completed-metric" -Passed ($completedMetric.resultCount -gt 0) -Details $completedMetric

    $leaderMetric = Wait-ForProbe -Description "Atlas scheduler leader metric in Prometheus" -Probe {
        Get-PrometheusQuery -Query 'max(atlas_scheduler_is_leader{job="atlas-scheduler"})'
    }
    $leaderValue = [double] $leaderMetric.results[0].value[1]
    Add-Check -Name "prometheus-atlas-scheduler-leader-metric" -Passed ($leaderValue -eq 1) -Details @{
        resultCount = $leaderMetric.resultCount
        leaderValue = $leaderValue
    }

    $grafanaHealth = Wait-ForProbe -Description "Grafana health" -Probe {
        $response = Invoke-Http -Uri "$($GrafanaUrl.TrimEnd('/'))/api/health"
        if ($response.status -ne 200) { return $null }
        try { $body = Convert-JsonContent -Response $response } catch { return $null }
        if ($body.database -ne "ok") { return $null }
        return [pscustomobject]@{ status = $response.status; database = [string] $body.database; version = [string] $body.version }
    }
    Add-Check -Name "grafana-health" -Passed ($grafanaHealth.database -eq "ok") -Details $grafanaHealth

    $basicAuth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${GrafanaUser}:${GrafanaPassword}"))
    $grafanaHeaders = @{ Authorization = "Basic $basicAuth" }
    $dataSource = Wait-ForProbe -Description "provisioned Grafana Prometheus datasource" -Probe {
        $response = Invoke-Http -Uri "$($GrafanaUrl.TrimEnd('/'))/api/datasources/uid/prometheus" -Headers $grafanaHeaders
        if ($response.status -ne 200) { return $null }
        try { $body = Convert-JsonContent -Response $response } catch { return $null }
        if ($body.uid -ne "prometheus" -or $body.type -ne "prometheus") { return $null }
        return [pscustomobject]@{
            uid = [string] $body.uid
            type = [string] $body.type
            url = [string] $body.url
        }
    }
    Add-Check -Name "grafana-prometheus-datasource-provisioned" -Passed ($dataSource.uid -eq "prometheus" -and $dataSource.type -eq "prometheus") -Details $dataSource

    $dashboard = Wait-ForProbe -Description "provisioned Atlas Grafana dashboard" -Probe {
        $response = Invoke-Http -Uri "$($GrafanaUrl.TrimEnd('/'))/api/dashboards/uid/atlas-overview" -Headers $grafanaHeaders
        if ($response.status -ne 200) { return $null }
        try { $body = Convert-JsonContent -Response $response } catch { return $null }
        if ($body.dashboard.uid -ne "atlas-overview") { return $null }
        return [pscustomobject]@{
            uid = [string] $body.dashboard.uid
            title = [string] $body.dashboard.title
        }
    }
    Add-Check -Name "grafana-atlas-dashboard-provisioned" -Passed ($dashboard.uid -eq "atlas-overview" -and $dashboard.title -eq "Atlas Overview") -Details $dashboard

    $jaeger = Wait-ForProbe -Description "Jaeger query UI endpoint" -Probe {
        $response = Invoke-Http -Uri "$($JaegerUrl.TrimEnd('/'))/"
        if ($response.status -eq 200) {
            return [pscustomobject]@{ status = $response.status }
        }
        return $null
    }
    Add-Check -Name "jaeger-query-endpoint" -Passed ($jaeger.status -eq 200) -Details $jaeger
}
catch {
    $script:Failure = $_.Exception.Message
}
finally {
    $script:FinishedAtUtc = [DateTime]::UtcNow
    $report = [ordered]@{
        schemaVersion = 1
        status = if ($script:Failure) { "failed" } else { "passed" }
        startedAtUtc = $script:StartedAtUtc.ToString("o")
        finishedAtUtc = $script:FinishedAtUtc.ToString("o")
        composeFile = $script:ComposeFile
        endpoints = [ordered]@{
            api = $ApiUrl
            prometheus = $PrometheusUrl
            grafana = $GrafanaUrl
            jaeger = $JaegerUrl
        }
        checks = $script:Checks.ToArray()
        failure = $script:Failure
    }

    $evidencePathToWrite = if ([IO.Path]::IsPathRooted($EvidencePath)) {
        $EvidencePath
    }
    else {
        Join-Path $script:RepoRoot $EvidencePath
    }
    $fullEvidencePath = [IO.Path]::GetFullPath($evidencePathToWrite)
    $evidenceDirectory = Split-Path -Parent $fullEvidencePath
    New-Item -ItemType Directory -Path $evidenceDirectory -Force | Out-Null
    $report | ConvertTo-Json -Depth 16 | Set-Content -LiteralPath $fullEvidencePath -Encoding utf8
}

if ($script:Failure) {
    Write-Error "Atlas stack smoke failed: $script:Failure. Evidence: $fullEvidencePath"
    exit 1
}

Write-Host "Atlas stack smoke passed ($($script:Checks.Count) checks). Evidence: $fullEvidencePath"
