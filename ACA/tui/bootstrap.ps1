# ============================================================
# ACA TUI - Bootstrap Safety Module
# bootstrap.ps1 - dot-source this file; do not run directly
# ============================================================
# Provides:
#   Test-AcaDependencies  - verify required CLI tools are present
#   Assert-AzContext      - validate Azure login + RG accessibility
#   Write-AcaAudit        - append audit entry to ACA/.aca-audit.log
#   Enter-AcaLock         - acquire ACA/.aca.lock (concurrent session guard)
#   Exit-AcaLock          - release ACA/.aca.lock
#
# Consumed by: ACA/manage-aca.ps1 and ACA/tui/*.ps1
# ============================================================
# PS 5.1 compatibility: no ternary, no ??, no &&
# No param() block at file level - this is a dot-sourced module
# ============================================================

# Capture the script directory at dot-source time.
# $PSScriptRoot inside function bodies refers to the calling script's directory,
# so we snapshot the correct path here before any function is invoked.
$script:_BootstrapDir = $PSScriptRoot

# Module-level flags set by Test-AcaDependencies.
# Consumed by ACA/tui/ui.ps1 and ACA/tui/state.ps1.
$script:UseGum           = $false
$script:MongoshAvailable = $false

# ---------------------------------------------------------------
# Test-AcaDependencies
# Checks required (az, docker) and optional (gum, mongosh) tools.
# Sets $script:UseGum and $script:MongoshAvailable.
# Throws on missing required tools; warns on missing optional tools.
# ---------------------------------------------------------------
function Test-AcaDependencies {
    # az - required
    if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
        Write-Error "az CLI not found. Install Azure CLI."
        throw "az CLI not found. Install Azure CLI."
    }

    # docker - required
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        Write-Error "Docker not found. Install Docker Desktop."
        throw "Docker not found. Install Docker Desktop."
    }

    # gum - optional, enables enhanced TUI mode
    if (Get-Command gum -ErrorAction SilentlyContinue) {
        $script:UseGum = $true
        Write-Host "gum detected - enhanced TUI mode" -ForegroundColor Green
    } else {
        $script:UseGum = $false
        Write-Host "gum not found - using native prompts" -ForegroundColor Yellow
    }

    # mongosh - optional, used for busy detection against StateStore
    if (Get-Command mongosh -ErrorAction SilentlyContinue) {
        $script:MongoshAvailable = $true
    } else {
        $script:MongoshAvailable = $false
        Write-Host "mongosh not found - busy detection will show Unknown. Install: https://www.mongodb.com/docs/mongodb-shell/install/" -ForegroundColor Yellow
    }

    # Ensure console outputs UTF-8 (required for gum icons / box-drawing chars)
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
}

# ---------------------------------------------------------------
# Assert-AzContext
# Validates that az is logged in and the target RG is accessible.
# Throws with a clear message on any failure.
# ---------------------------------------------------------------
function Assert-AzContext {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ResourceGroupName
    )

    # 'Continue' prevents az's non-zero exit from being treated as a PS terminating error
    $ErrorActionPreference = 'Continue'

    az account show --output json 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Not logged into Azure. Run 'az login'."
    }

    az group show --name $ResourceGroupName --output none 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Resource group '$ResourceGroupName' not found or not accessible."
    }

    $ErrorActionPreference = 'Stop'
    Write-Host "Azure context OK: RG '$ResourceGroupName' accessible." -ForegroundColor Green
}

# ---------------------------------------------------------------
# Write-AcaAudit
# Appends a timestamped audit entry to ACA/.aca-audit.log.
# Format: {UTC-ISO8601} | {user}@{host} | {Action} | {Target} | {Result}
# NEVER log secrets or connection strings - pass safe summaries only.
# ---------------------------------------------------------------
function Write-AcaAudit {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Action,

        [Parameter(Mandatory = $true)]
        [string]$Target,

        [Parameter(Mandatory = $true)]
        [string]$Result
    )

    $timestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')

    $username = $env:USERNAME
    if ([string]::IsNullOrEmpty($username)) {
        $username = $env:USERDOMAIN
    }
    if ([string]::IsNullOrEmpty($username)) {
        $username = "unknown"
    }

    $computer = $env:COMPUTERNAME
    if ([string]::IsNullOrEmpty($computer)) {
        $computer = "unknown"
    }

    $line = "$timestamp | $username@$computer | $Action | $Target | $Result"
    $auditPath = Join-Path (Join-Path $script:_BootstrapDir "..") ".aca-audit.log"
    Add-Content -Path $auditPath -Value $line
}

# ---------------------------------------------------------------
# Enter-AcaLock
# Creates ACA/.aca.lock with current PID/host/UTC.
# Detects stale locks (dead process or >2 hours old) and removes them.
# Live locks prompt the operator before proceeding.
# ---------------------------------------------------------------
function Enter-AcaLock {
    $lockPath = Join-Path (Join-Path $script:_BootstrapDir "..") ".aca.lock"

    if (Test-Path -LiteralPath $lockPath) {
        $rawContent = Get-Content -LiteralPath $lockPath -Raw -ErrorAction SilentlyContinue
        $lockData   = $null

        if (-not [string]::IsNullOrEmpty($rawContent)) {
            try {
                $lockData = $rawContent | ConvertFrom-Json
            } catch {
                # Corrupt lock file - treat as stale, will be overwritten
                $lockData = $null
            }
        }

        if ($null -ne $lockData) {
            $lockPid     = [int]$lockData.pid
            $lockHost    = [string]$lockData.host
            $lockStarted = [string]$lockData.started

            $isStale = $false

            # Check if the owning process is still running
            $ownerProc = Get-Process -Id $lockPid -ErrorAction SilentlyContinue
            if ($null -eq $ownerProc) {
                $isStale = $true
            }

            # Check if lock is older than 2 hours (stale regardless of process state)
            if (-not $isStale) {
                $lockTime = $null
                try {
                    $lockTime = [datetime]::Parse($lockStarted)
                } catch {
                    # Unparseable timestamp - treat as stale
                    $isStale = $true
                }

                if ((-not $isStale) -and ($null -ne $lockTime)) {
                    $age = (Get-Date).ToUniversalTime() - $lockTime.ToUniversalTime()
                    if ($age.TotalHours -gt 2) {
                        $isStale = $true
                    }
                }
            }

            if ($isStale) {
                Write-Host "Stale lock removed (PID $lockPid on $lockHost)." -ForegroundColor Yellow
                Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue
            } else {
                Write-Warning "Another ACA session appears active (PID $lockPid on $lockHost, started $lockStarted). Proceed anyway? [y/N]"
                $answer = Read-Host
                if ($answer -ne 'y') {
                    throw "Aborted: another session is active."
                }
                Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue
            }
        } else {
            # Empty or corrupt lock file - remove and acquire fresh
            Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue
        }
    }

    $nowUtc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    $lockObject = [PSCustomObject]@{
        pid     = $PID
        host    = $env:COMPUTERNAME
        started = $nowUtc
    }
    $lockContent = $lockObject | ConvertTo-Json -Compress

    Set-Content -LiteralPath $lockPath -Value $lockContent -Encoding UTF8
    Write-Host "ACA session lock acquired." -ForegroundColor Gray
}

# ---------------------------------------------------------------
# Exit-AcaLock
# Removes ACA/.aca.lock on session exit. Safe to call even if
# lock does not exist (e.g., if Enter-AcaLock was never reached).
# ---------------------------------------------------------------
function Exit-AcaLock {
    $lockPath = Join-Path (Join-Path $script:_BootstrapDir "..") ".aca.lock"
    Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue
    Write-Host "ACA session lock released." -ForegroundColor Gray
}
