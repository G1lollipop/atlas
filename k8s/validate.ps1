$ErrorActionPreference = "Stop"

function Get-KustomizeOutput {
    param([Parameter(Mandatory = $true)][string] $Path)

    $output = & kubectl kustomize $Path 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl kustomize failed for '$Path': $($output -join [Environment]::NewLine)"
    }
    $lines = @($output | ForEach-Object { ([string] $_).TrimEnd([char] 13, [char] 10) })
    return ($lines -join "`n")
}

function Assert-Contains {
    param(
        [Parameter(Mandatory = $true)][string] $Text,
        [Parameter(Mandatory = $true)][string] $Pattern,
        [Parameter(Mandatory = $true)][string] $Description
    )

    if ($Text -notmatch $Pattern) {
        throw "Expected $Description."
    }
}

function Assert-NotContains {
    param(
        [Parameter(Mandatory = $true)][string] $Text,
        [Parameter(Mandatory = $true)][string] $Pattern,
        [Parameter(Mandatory = $true)][string] $Description
    )

    if ($Text -match $Pattern) {
        throw "Did not expect $Description."
    }
}

function Assert-WorkerLabels {
    param(
        [Parameter(Mandatory = $true)][string] $Text,
        [Parameter(Mandatory = $true)][string] $DeploymentName,
        [Parameter(Mandatory = $true)][hashtable] $Expected
    )

    $deployment = $null
    $documents = $Text -split '(?m)^---[ \t]*$'
    $namePattern = '(?m)^[ \t]+name: ' + [regex]::Escape($DeploymentName) + '[ \t]*$'
    foreach ($document in $documents) {
        if ($document -match '(?m)^kind: Deployment$' -and $document -match $namePattern) {
            $deployment = $document
            break
        }
    }
    if ($null -eq $deployment) {
        throw "Could not find Deployment '$DeploymentName' in rendered manifests."
    }

    $envMatch = [regex]::Match(
        $deployment,
        '(?m)^[ \t]*- name: WORKER_LABELS[ \t]*\r?\n[ \t]+value: (?<yamlValue>[^\r\n]+)$'
    )
    if (-not $envMatch.Success) {
        throw "Deployment '$DeploymentName' has no rendered WORKER_LABELS value."
    }

    $yamlValue = $envMatch.Groups['yamlValue'].Value.Trim()
    if ($yamlValue.Length -lt 2 -or -not $yamlValue.StartsWith("'") -or -not $yamlValue.EndsWith("'")) {
        throw "Deployment '$DeploymentName' WORKER_LABELS must be a single-quoted JSON string."
    }
    $json = $yamlValue.Substring(1, $yamlValue.Length - 2)
    try {
        $labels = ConvertFrom-Json -InputObject $json -ErrorAction Stop
    }
    catch {
        throw "Deployment '$DeploymentName' WORKER_LABELS is not valid JSON: $($_.Exception.Message)"
    }

    $actualProperties = @($labels.PSObject.Properties)
    if ($actualProperties.Count -ne $Expected.Count) {
        throw "Deployment '$DeploymentName' WORKER_LABELS has $($actualProperties.Count) keys; expected $($Expected.Count)."
    }
    foreach ($key in $Expected.Keys) {
        $property = $labels.PSObject.Properties[$key]
        if ($null -eq $property -or $property.Value -isnot [string] -or $property.Value -cne $Expected[$key]) {
            throw "Deployment '$DeploymentName' WORKER_LABELS[$key] does not match the expected string value '$($Expected[$key])'."
        }
    }
}

$root = Split-Path -Parent $PSCommandPath
$base = Get-KustomizeOutput -Path $root
$nvidia = Get-KustomizeOutput -Path (Join-Path $root "gpu-nvidia")
$keda = Get-KustomizeOutput -Path (Join-Path $root "keda")

Assert-Contains -Text $base -Pattern '(?m)^\s+name: atlas-api$' -Description "the API Deployment in the core output"
Assert-Contains -Text $base -Pattern '(?m)^\s+name: atlas-scheduler$' -Description "the scheduler Deployment in the core output"
Assert-Contains -Text $base -Pattern '(?m)^\s+name: atlas-worker-cpu$' -Description "the CPU worker Deployment in the core output"
Assert-Contains -Text $base -Pattern '(?m)^\s+name: atlas-worker-gpu$' -Description "the simulated GPU worker Deployment in the core output"
Assert-Contains -Text $base -Pattern '(?m)^\s+value: simulated-nvidia-l4$' -Description "the simulated GPU type in the core output"
Assert-Contains -Text $base -Pattern '(?m)^\s+name: atlas-worker-gpu-metrics$' -Description "the GPU metrics Service in the core output"
Assert-NotContains -Text $base -Pattern 'apiVersion: keda\.sh/' -Description "KEDA custom resources in the core output"
Assert-NotContains -Text $base -Pattern 'nvidia\.com/gpu' -Description "an NVIDIA device limit in the simulated core output"
Assert-NotContains -Text $base -Pattern '(?s)kind: HorizontalPodAutoscaler\s+metadata:\s+name: atlas-worker' -Description "a worker HPA in the core output"

Assert-Contains -Text $nvidia -Pattern '(?m)^\s+nvidia\.com/gpu: 1$' -Description "the NVIDIA extended resource in the hardware overlay"
Assert-Contains -Text $nvidia -Pattern '(?m)^\s+atlas\.io/accelerator: nvidia-l4$' -Description "the hardware node selector"
Assert-Contains -Text $nvidia -Pattern '(?m)^\s+value: nvidia-l4$' -Description "the hardware accelerator type"
Assert-Contains -Text $nvidia -Pattern '(?m)^\s+atlas\.io/gpu-mode: hardware$' -Description "the hardware-mode pod label"

Assert-WorkerLabels -Text $base -DeploymentName "atlas-worker-cpu" -Expected @{ pool = "cpu"; execution = "cpu" }
Assert-WorkerLabels -Text $base -DeploymentName "atlas-worker-gpu" -Expected @{ pool = "gpu"; execution = "simulated"; accelerator = "nvidia-l4" }
Assert-WorkerLabels -Text $nvidia -DeploymentName "atlas-worker-gpu" -Expected @{ pool = "gpu"; execution = "hardware"; accelerator = "nvidia-l4" }

Assert-Contains -Text $keda -Pattern '(?m)^\s+name: atlas-worker-cpu$' -Description "the CPU ScaledObject"
Assert-Contains -Text $keda -Pattern '(?m)^\s+name: atlas-worker-gpu$' -Description "the GPU ScaledObject"
Assert-Contains -Text $keda -Pattern 'sum\(max by \(queue, resource_class\) \(atlas_job_queue_depth\{resource_class="cpu"\}\s+and on \(job, instance\)\s+\(up == 1\) and on \(job, instance\)\s+\(atlas_observability_up\s+== 1\)\)\)' -Description "the CPU queue query gated by scrape and snapshot health"
Assert-Contains -Text $keda -Pattern 'sum\(max by \(queue, resource_class\) \(atlas_job_queue_depth\{resource_class="gpu"\}\s+and on \(job, instance\)\s+\(up == 1\) and on \(job, instance\)\s+\(atlas_observability_up\s+== 1\)\)\)' -Description "the GPU queue query gated by scrape and snapshot health"
if ([regex]::Matches($keda, '(?m)^\s+ignoreNullValues: "false"$').Count -ne 2) {
    throw "Expected empty-result errors for both KEDA triggers."
}
Assert-Contains -Text $keda -Pattern '(?m)^  minReplicaCount: [1-9][0-9]*$' -Description "nonzero KEDA minimum replica counts"
Assert-Contains -Text $keda -Pattern '(?m)^    failureThreshold: 3$' -Description "KEDA fallback for scaler failures"

Write-Output "Static Kubernetes manifest validation passed for core, NVIDIA, and KEDA configurations."
