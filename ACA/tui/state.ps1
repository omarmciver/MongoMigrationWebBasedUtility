# ACA/tui/state.ps1 — dot-sourced module
# Part 1 (C2): Get-InstanceJobState (busy detection)
# Part 2 (C4): Get-AcaInstanceStatus, Show-AcaStatusTable (status aggregation)
# ============================================================
# PS 5.1 compatibility: no ternary, no ??, no &&
# No param() block at file level - this is a dot-sourced module
# ============================================================

# Per-instance AppId cache: avoids repeated az calls during a TUI session.
$script:_AppIdCache = @{}

# ---------------------------------------------------------------
# Get-InstanceJobState
# Queries the StateStore (MongoDB) to determine the active job phase
# for the given Container App instance.
#
# Returns a hashtable with keys:
#   Phase          = "Idle" | "Dumping" | "ChangeStreamTailing" | "Unknown"
#   Count          = number of active (non-completed, non-cancelled) jobs
#   Classification = "Safe" | "Busy" | "Sacred" | "Unknown"
#
# Returns Unknown on any error (mongosh unavailable, connection timeout,
# parse failure). NEVER returns a false-Safe result on failure.
# ---------------------------------------------------------------
function Get-InstanceJobState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$InstanceName
    )

    $unknownResult = @{ Phase = 'Unknown'; Count = 0; Classification = 'Unknown' }

    # Guard: mongosh required for busy detection
    if (-not $script:MongoshAvailable) {
        return $unknownResult
    }

    # ----------------------------------------------------------
    # Derive AppId from live container app env vars; cache result.
    # Fallback: AppId = InstanceName (deploy-script default).
    # ----------------------------------------------------------
    $appId = $null
    if ($script:_AppIdCache.ContainsKey($InstanceName)) {
        $appId = $script:_AppIdCache[$InstanceName]
    } else {
        $ErrorActionPreference = 'Continue'
        $appIdResult = az containerapp show `
            --name $InstanceName `
            --resource-group $AcaConfig.ResourceGroupName `
            --query "properties.template.containers[0].env[?name=='StateStoreAppID'].value | [0]" `
            --output tsv `
            2>$null
        $azExitCode = $LASTEXITCODE
        $ErrorActionPreference = 'Stop'

        if ($azExitCode -eq 0 -and -not [string]::IsNullOrEmpty($appIdResult)) {
            $appId = $appIdResult.Trim()
        } else {
            # Fallback: ContainerAppName is the deploy-script default AppId
            $appId = $InstanceName
        }
        $script:_AppIdCache[$InstanceName] = $appId
    }

    # ----------------------------------------------------------
    # Build JavaScript eval string.
    # StateStore: DB = mongomigrationwebappstorage, collection = datafiles
    # Doc ID: {appId}.migrationjobs_joblist_json
    # CDCMode: 0 = Offline (not tailing), != 0 = change-stream active (Sacred)
    # NOTE: connection string is the mongosh URI arg; NOT embedded in JS.
    # ----------------------------------------------------------
    $docId = "${appId}.migrationjobs_joblist_json"
    $evalScript = "var doc = db.getSiblingDB('mongomigrationwebappstorage').datafiles.findOne({ _id: '$docId' });" +
                  " if (!doc) { print(JSON.stringify({phase:'Idle',count:0})); }" +
                  " else {" +
                  " var jobs = doc.MigrationJobs || [];" +
                  " var active = jobs.filter(function(j){ return j.IsStarted && !j.IsCompleted && !j.IsCancelled; });" +
                  " var tailing = active.filter(function(j){ return j.CDCMode !== 0; });" +
                  " if (tailing.length > 0) { print(JSON.stringify({phase:'ChangeStreamTailing',count:active.length})); }" +
                  " else if (active.length > 0) { print(JSON.stringify({phase:'Dumping',count:active.length})); }" +
                  " else { print(JSON.stringify({phase:'Idle',count:0})); }" +
                  " }"

    # ----------------------------------------------------------
    # Run mongosh with connection timeouts.
    # mongosh 2.x does not accept --connectTimeoutMS/--serverSelectionTimeoutMS
    # as CLI flags — these must be appended as URI query params.
    # IMPORTANT: connection string (and its timeout-extended form) must NEVER
    # be logged or printed.
    # ----------------------------------------------------------
    $connStr = $AcaConfig.StateStoreConnectionString
    if ($connStr -match '\?') {
        $connWithTimeout = "${connStr}&connectTimeoutMS=5000&serverSelectionTimeoutMS=5000"
    } else {
        $connWithTimeout = "${connStr}?connectTimeoutMS=5000&serverSelectionTimeoutMS=5000"
    }

    $ErrorActionPreference = 'Continue'
    $rawOutput = mongosh $connWithTimeout `
        --eval $evalScript `
        --quiet `
        2>$null
    $mongoshExitCode = $LASTEXITCODE
    $ErrorActionPreference = 'Stop'
    Remove-Variable -Name connWithTimeout -ErrorAction SilentlyContinue

    if ($mongoshExitCode -ne 0) {
        return $unknownResult
    }

    # ----------------------------------------------------------
    # Parse JSON output: find last line containing a JSON object.
    # Trim to remove potential \r on Windows CRLF output.
    # ----------------------------------------------------------
    $jsonLine = ($rawOutput -split "`n") |
        Where-Object { $_ -match '^\s*\{' } |
        Select-Object -Last 1

    if ([string]::IsNullOrEmpty($jsonLine)) {
        return $unknownResult
    }

    $jsonLine = $jsonLine.Trim()

    $parsed = $null
    try {
        $parsed = $jsonLine | ConvertFrom-Json
    } catch {
        return $unknownResult
    }

    if ($null -eq $parsed) {
        return $unknownResult
    }

    $phase = $parsed.phase
    if ([string]::IsNullOrEmpty($phase)) {
        return $unknownResult
    }

    # Use PSObject.Properties to avoid ambiguity with automatic .Count on collections
    $count = 0
    $countProp = $parsed.PSObject.Properties['count']
    if ($null -ne $countProp) {
        $count = [int]$countProp.Value
    }

    # ----------------------------------------------------------
    # Map phase to classification:
    #   Safe:    Idle, Paused, Completed
    #   Busy:    Dumping, Restoring, IndexBuilding
    #   Sacred:  ChangeStreamTailing
    #   Unknown: any unrecognized phase
    # ----------------------------------------------------------
    $classification = 'Unknown'
    if ($phase -eq 'Idle' -or $phase -eq 'Paused' -or $phase -eq 'Completed') {
        $classification = 'Safe'
    } elseif ($phase -eq 'Dumping' -or $phase -eq 'Restoring' -or $phase -eq 'IndexBuilding') {
        $classification = 'Busy'
    } elseif ($phase -eq 'ChangeStreamTailing') {
        $classification = 'Sacred'
    }

    return @{
        Phase          = $phase
        Count          = $count
        Classification = $classification
    }
}

# ============================================================
# END Part 1 (C2) — Get-InstanceJobState
# ============================================================

# ---------------------------------------------------------------
# Part 2 (C4): Get-AcaInstanceStatus, Show-AcaStatusTable
# Added by task C4 — Status aggregation and display
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# Get-AcaInstanceStatus
# Lists all mongo-migrator-* container apps in the RG and returns
# a status object for each, combining:
#   - Azure provisioning/running state
#   - Image tag (from the container image reference)
#   - Job phase/classification (from Get-InstanceJobState)
#
# Returns [object[]] — empty array on any az failure.
# Uses $AcaConfig from caller scope.
# ---------------------------------------------------------------
function Get-AcaInstanceStatus {
    [CmdletBinding()]
    param()

    $results = @()

    # ----------------------------------------------------------
    # List all container apps whose name starts with the prefix.
    # --query filters server-side to avoid unnecessary data.
    # ----------------------------------------------------------
    # Retry once: a transient az failure (e.g. right after a long docker build)
    # must not be misreported as "no instances".
    $appsJson = $null
    $azExitCode = 1
    $attempt = 0
    while ($attempt -lt 2) {
        $attempt++
        $ErrorActionPreference = 'Continue'
        $appsJson = az containerapp list `
            --resource-group $AcaConfig.ResourceGroupName `
            --query "[?starts_with(name, '$($AcaConfig.ContainerAppNamePrefix)-')]" `
            --output json `
            2>$null
        $azExitCode = $LASTEXITCODE
        $ErrorActionPreference = 'Stop'
        if ($azExitCode -eq 0 -and -not [string]::IsNullOrEmpty($appsJson)) {
            break
        }
        if ($attempt -lt 2) {
            Start-Sleep -Seconds 2
        }
    }

    if ($azExitCode -ne 0 -or [string]::IsNullOrEmpty($appsJson)) {
        if ($azExitCode -ne 0) {
            Write-Host "  WARNING: Azure CLI query failed (az exit $azExitCode)." -ForegroundColor Yellow
            Write-Host "           Your 'az' login may have expired (common after a long build)." -ForegroundColor Yellow
            Write-Host "           Run 'az login' in another terminal, then choose '1. Status' to refresh." -ForegroundColor Yellow
        }
        return ,$results
    }

    $apps = $null
    try {
        $apps = $appsJson | ConvertFrom-Json
    } catch {
        return ,$results
    }

    if ($null -eq $apps -or $apps.Count -eq 0) {
        return ,$results
    }

    foreach ($app in $apps) {
        # ---- Name ----
        $name = $app.name

        # ---- ProvisioningState ----
        $provisioningState = 'Unknown'
        if (-not [string]::IsNullOrEmpty($app.properties.provisioningState)) {
            $provisioningState = $app.properties.provisioningState
        }

        # ---- RunningState ----
        # properties.runningStatus may not exist in all API versions.
        # Fall back to "Unknown" gracefully.
        $runningState = 'Unknown'
        $runStatusProp = $app.properties.PSObject.Properties['runningStatus']
        if ($null -ne $runStatusProp -and -not [string]::IsNullOrEmpty($runStatusProp.Value)) {
            $runningState = $runStatusProp.Value
        }

        # ---- ImageTag ----
        # Full image ref is e.g. "artifact.it.att.com:22609/repo/mongo-migration:202606221254"
        # We want the part after the last ':'.
        $imageTag = 'unknown'
        $imageRef = $null
        try {
            $imageRef = $app.properties.template.containers[0].image
        } catch {
            $imageRef = $null
        }
        if (-not [string]::IsNullOrEmpty($imageRef)) {
            $colonIdx = $imageRef.LastIndexOf(':')
            if ($colonIdx -ge 0 -and $colonIdx -lt ($imageRef.Length - 1)) {
                $imageTag = $imageRef.Substring($colonIdx + 1)
            }
        }

        # ---- Job Phase / Classification ----
        $jobState = Get-InstanceJobState -InstanceName $name
        $phase          = $jobState.Phase
        $count          = $jobState.Count
        $classification = $jobState.Classification

        # ---- IsHealthy ----
        # Healthy = Azure says Succeeded + running state is Running (or Unknown when API omits it).
        # If runningState is "Unknown" (field missing from API), we don't penalise — use
        # ProvisioningState alone as the proxy.
        $isHealthy = $false
        if ($provisioningState -eq 'Succeeded') {
            if ($runningState -eq 'Running' -or $runningState -eq 'Unknown') {
                $isHealthy = $true
            }
        }

        $statusObj = [PSCustomObject]@{
            Name              = $name
            ProvisioningState = $provisioningState
            RunningState      = $runningState
            ImageTag          = $imageTag
            Phase             = $phase
            Count             = $count
            Classification    = $classification
            IsHealthy         = $isHealthy
        }

        $results += $statusObj
    }

    # Sort by name so table order is deterministic (mongo-migrator-1 before -2).
    # @() + leading comma guarantees an array is returned (never $null or a bare
    # scalar), so callers and Show-AcaStatusTable never receive null.
    $sorted = @($results | Sort-Object -Property Name)

    return ,$sorted
}

# ---------------------------------------------------------------
# Show-AcaStatusTable
# Renders a formatted instance-status table to the terminal.
# Colour-codes rows by Classification (Sacred=Red, Busy=Yellow, Safe=Green).
# ---------------------------------------------------------------
function Show-AcaStatusTable {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $false)]
        [AllowNull()]
        [AllowEmptyCollection()]
        [object[]]$Statuses
    )

    Write-Host ""
    Write-Host "  Mongo Migrator -- Instance Status" -ForegroundColor Cyan
    if ($null -eq $Statuses -or $Statuses.Count -eq 0) {
        Write-Host "  No instances found (or status query temporarily unavailable)." -ForegroundColor Yellow
        Write-Host ""
        return
    }
    Write-Host ("  " + ("-" * 75)) -ForegroundColor DarkGray
    Write-Host ("  {0,-25} {1,-18} {2,-12} {3,-20} {4,-10}" -f "NAME", "IMAGE", "HEALTH", "PHASE", "CLASS") -ForegroundColor Gray

    foreach ($s in $Statuses) {
        $nameColor = 'Gray'
        if ($s.Classification -eq 'Sacred') {
            $nameColor = 'Red'
        } elseif ($s.Classification -eq 'Busy') {
            $nameColor = 'Yellow'
        } elseif ($s.Classification -eq 'Safe') {
            $nameColor = 'Green'
        }

        $health = $s.ProvisioningState
        if ($s.IsHealthy) {
            $health = 'Healthy'
        }

        $line = "  {0,-25} {1,-18} {2,-12} {3,-20} {4,-10}" -f $s.Name, $s.ImageTag, $health, $s.Phase, $s.Classification
        Write-Host $line -ForegroundColor $nameColor
    }

    Write-Host ("  " + ("-" * 75)) -ForegroundColor DarkGray

    Write-Host "  Legend: " -NoNewline -ForegroundColor Gray
    Write-Host "[Sacred=CS-Tailing no-touch] " -NoNewline -ForegroundColor Red
    Write-Host "[Busy=in-progress] " -NoNewline -ForegroundColor Yellow
    Write-Host "[Safe=ok-to-modify] " -NoNewline -ForegroundColor Green
    Write-Host "[Unknown=manual-confirm]" -ForegroundColor Gray
    Write-Host ""
}

# ============================================================
# END Part 2 (C4) — Get-AcaInstanceStatus, Show-AcaStatusTable
# ============================================================
