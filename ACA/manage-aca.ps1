# ============================================================
# ACA TUI - Main Entry Point
# manage-aca.ps1
# ============================================================
# Usage: .\manage-aca.ps1
# Requires: ACA/aca.config.local.ps1 (gitignored; copy from aca.config.local.example.ps1)
# ============================================================
# PS 5.1 compatible; no param() block
# ============================================================

# ---- Step 1: Dot-source local config ----
$configPath = Join-Path $PSScriptRoot "aca.config.local.ps1"
if (-not (Test-Path -LiteralPath $configPath)) {
    Write-Error "Config file not found: $configPath"
    Write-Error "Copy ACA/aca.config.local.example.ps1 to ACA/aca.config.local.ps1 and fill in real values."
    exit 1
}
. $configPath

# ---- Step 2: Dot-source tui modules ----
$tuiDir = Join-Path $PSScriptRoot "tui"
. (Join-Path $tuiDir "bootstrap.ps1")
. (Join-Path $tuiDir "ui.ps1")
. (Join-Path $tuiDir "state.ps1")
. (Join-Path $tuiDir "jfrog.ps1")
. (Join-Path $tuiDir "instances.ps1")
. (Join-Path $tuiDir "flows.ps1")

# ---- Step 3: Bootstrap safety ----
# Lock released in finally block
try {
    Test-AcaDependencies
    Assert-AzContext -ResourceGroupName $AcaConfig.ResourceGroupName
    Enter-AcaLock
    Write-AcaAudit -Action "Session-Start" -Target $AcaConfig.ResourceGroupName -Result "OK"

    # ---- Step 4: Menu loop ----
    $menuItems = @(
        "1. Status   - Refresh instance list"
        "2. Build    - Build and push new Docker image"
        "3. Add      - Add a new instance"
        "4. Update   - Update instance(s) to a new image"
        "5. Remove   - Remove instance(s)"
        "6. Rollback - Activate a previous revision"
        "7. Exit"
    )

    $continue = $true
    while ($continue) {
        # Always show current status at top of each loop
        Write-Host ""
        Write-Host "========================================" -ForegroundColor DarkCyan
        Write-Host "  Mongo Migrator ACA Manager" -ForegroundColor Cyan
        Write-Host "========================================" -ForegroundColor DarkCyan
        $statuses = Get-AcaInstanceStatus
        if ($null -eq $statuses) { $statuses = @() }
        Show-AcaStatusTable -Statuses $statuses

        $selection = Select-AcaItems -Items $menuItems -Prompt "Select action"
        if (-not $selection -or $selection.Count -eq 0) { continue }
        $choice = $selection[0]

        # Extract the leading number
        $choiceNum = ""
        if ($choice -match '^(\d+)\.') { $choiceNum = $Matches[1] }

        switch ($choiceNum) {
            "1" { $statuses = Get-AcaInstanceStatus; if ($null -eq $statuses) { $statuses = @() }; Show-AcaStatusTable -Statuses $statuses }
            "2" { Invoke-BuildFlow }
            "3" { Invoke-AddFlow }
            "4" { Invoke-UpdateFlow }
            "5" { Invoke-RemoveFlow }
            "6" { Invoke-RollbackFlow }
            "7" { $continue = $false }
            default { Write-Host "Unknown selection." -ForegroundColor Yellow }
        }
    }

    Write-AcaAudit -Action "Session-End" -Target $AcaConfig.ResourceGroupName -Result "OK"
    Write-Host "`nGoodbye." -ForegroundColor Gray
} finally {
    Exit-AcaLock
}
