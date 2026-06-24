# ============================================================
# ACA TUI - Instance Management
# instances.ps1 - dot-source this file; do not run directly
# ============================================================
# Provides:
#   Get-AcaGapFillIndex  - find lowest unused positive integer
#   Add-AcaInstance      - deploy a new container app instance
#   Remove-AcaInstance   - delete instance, remove env storage config, optional share wipe
#
# Consumed by: ACA/tui/flows.ps1
# ============================================================
# PS 5.1 compatibility: no ternary, no ??, no &&
# No param() block at file level - this is a dot-sourced module
# ============================================================

# ---------------------------------------------------------------
# Get-AcaGapFillIndex
# Returns the lowest positive integer not present in ExistingIndices.
#
# Examples:
#   @(1,3)  → 2
#   @(1,2)  → 3
#   @()     → 1
#   @(2,3)  → 1
# ---------------------------------------------------------------
function Get-AcaGapFillIndex {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $false)]
        [int[]]$ExistingIndices = @()
    )

    $i = 1
    while ($ExistingIndices -contains $i) {
        $i++
    }
    return $i
}

# ---------------------------------------------------------------
# Add-AcaInstance
# Deploys a new container app instance with the given image tag.
# Handles capacity pre-scaling, two-pass ARM deploy, health poll,
# and audit logging.
#
# Parameters:
#   InstanceName  - e.g. "mongo-migrator-3"
#   ImageTag      - the image tag to deploy (e.g. "20260623120000")
#   WhatIf        - if set, prints what WOULD run without executing
#
# Returns: [bool] $true on success, $false on failure
# ---------------------------------------------------------------
function Add-AcaInstance {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$InstanceName,

        [Parameter(Mandatory = $true)]
        [string]$ImageTag,

        [switch]$WhatIf
    )

    # ----------------------------------------------------------
    # Step 1: Derive FileShareName from instance number suffix
    # e.g. "mongo-migrator-3" -> index "3" -> "migration-data-3"
    # ----------------------------------------------------------
    $indexMatch = [regex]::Match($InstanceName, '\d+$')
    if (-not $indexMatch.Success) {
        Write-Host "Add-AcaInstance: Cannot determine index from instance name '$InstanceName'." -ForegroundColor Red
        return $false
    }
    $index = $indexMatch.Value
    $fileShare = "migration-data-$index"

    # ----------------------------------------------------------
    # Step 2: Read-only capacity state (no mutating az calls yet)
    # Count existing instances + 1 for the new one.
    # ----------------------------------------------------------
    $ErrorActionPreference = 'Continue'
    $existingAppsJson = az containerapp list `
        --resource-group $AcaConfig.ResourceGroupName `
        --query "[?starts_with(name, '$($AcaConfig.ContainerAppNamePrefix)-')].name" `
        --output tsv `
        2>$null
    $ErrorActionPreference = 'Stop'

    $existingCount = 0
    if (-not [string]::IsNullOrEmpty($existingAppsJson)) {
        $existingCount = @($existingAppsJson -split "`n" | Where-Object { $_ -ne '' }).Count
    }
    $required = $existingCount + 1
    if ($required -gt 10) { $required = 10 }

    $ErrorActionPreference = 'Continue'
    $currentMax = az containerapp env workload-profile list `
        --resource-group $AcaConfig.ResourceGroupName `
        --name $AcaConfig.EnvironmentName `
        --query "[?name=='Dedicated'].properties.maximumCount | [0]" `
        --output tsv `
        2>$null
    $ErrorActionPreference = 'Stop'

    $currentMaxNodes = 0
    $needsCapacityUpdate = $false
    if (-not [string]::IsNullOrEmpty($currentMax)) {
        $currentMaxNodes = [int]$currentMax
        if ($currentMaxNodes -lt $required) {
            $needsCapacityUpdate = $true
        }
    }

    # ----------------------------------------------------------
    # Step 3: WhatIf - print plan and return without any mutation
    # ----------------------------------------------------------
    if ($WhatIf) {
        Write-Host "WOULD CREATE instance: $InstanceName" -ForegroundColor Cyan
        Write-Host "  FileShare: $fileShare"
        Write-Host "  ImageTag: $ImageTag"
        Write-Host "  EnvironmentName: $($AcaConfig.EnvironmentName)"
        if ($needsCapacityUpdate) {
            Write-Host "  WOULD SCALE Dedicated max-nodes: $currentMaxNodes -> $required"
        }
        return $true
    }

    # ----------------------------------------------------------
    # Step 4a: Capacity scale (mutating - only when not WhatIf)
    # ----------------------------------------------------------
    if ($needsCapacityUpdate) {
        Write-Host "Increasing Dedicated workload profile max-nodes from $currentMaxNodes to $required..." -ForegroundColor Yellow
        $ErrorActionPreference = 'Continue'
        az containerapp env workload-profile update `
            --resource-group $AcaConfig.ResourceGroupName `
            --name $AcaConfig.EnvironmentName `
            --workload-profile-name Dedicated `
            --max-nodes $required `
            --output none
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Environment capacity updated successfully." -ForegroundColor Green
        } else {
            Write-Host "Failed to update workload profile max-nodes. Deployment may be capacity-constrained." -ForegroundColor Yellow
        }
        $ErrorActionPreference = 'Stop'
    } elseif ($currentMaxNodes -gt 0) {
        Write-Host "Environment capacity OK (Dedicated max-nodes: $currentMaxNodes)." -ForegroundColor Gray
    } else {
        Write-Host "Unable to read Dedicated workload profile capacity. Continuing without pre-scaling." -ForegroundColor Yellow
    }

    # ----------------------------------------------------------
    # Step 5: Deploy via deploy-to-aca-jfrog.ps1
    # Join-Path with 3 args is PS 6+; use nested Join-Path for PS 5.1.
    # ----------------------------------------------------------
    $deployPath = Join-Path (Join-Path $PSScriptRoot "..") "deploy-to-aca-jfrog.ps1"
    $deployArgs = @{
        ResourceGroupName              = $AcaConfig.ResourceGroupName
        ContainerAppName               = $InstanceName
        EnvironmentName                = $AcaConfig.EnvironmentName
        ReuseExistingEnvironment       = $true
        JFrogRegistryServer            = $AcaConfig.JFrogRegistryServer
        JFrogRegistryServerForACA      = $AcaConfig.JFrogRegistryServerForACA
        JFrogUsername                  = $AcaConfig.JFrogUsername
        JFrogPassword                  = $AcaConfig.JFrogPassword
        JFrogRepository                = $AcaConfig.JFrogRepository
        JFrogDebianRepo                = $AcaConfig.JFrogDebianRepo
        JFrogDebianDistribution        = 'bookworm'
        JFrogDebianComponent           = $AcaConfig.JFrogDebianComponent
        JFrogNuGetRepo                 = $AcaConfig.JFrogNuGetRepo
        Location                       = $AcaConfig.Location
        OwnerTag                       = $AcaConfig.OwnerTag
        VCores                         = $AcaConfig.VCores
        MemoryGB                       = $AcaConfig.MemoryGB
        StateStoreConnectionString     = $AcaConfig.StateStoreConnectionString
        ImageTag                       = $ImageTag
        SkipDockerBuild                = $true
        FileShareName                  = $fileShare
        StorageAccountResourceId       = $AcaConfig.SharedStorageAccountResourceId
        InfrastructureSubnetResourceId = $AcaConfig.InfrastructureSubnetResourceId
        TerminationGracePeriodSeconds  = 600
    }
    # deploy-to-aca-jfrog.ps1 uses a relative path for the bicep template,
    # so it must be invoked with CWD == ACA folder.
    $acaDir = Join-Path $PSScriptRoot ".."
    Push-Location $acaDir
    try {
        & $deployPath @deployArgs
        $success = ($LASTEXITCODE -eq 0)
    } finally {
        Pop-Location
    }

    # ----------------------------------------------------------
    # Step 5: Poll for healthy state (up to 5 minutes)
    # ----------------------------------------------------------
    $deadline = (Get-Date).AddMinutes(5)
    $healthy = $false
    while ((Get-Date) -lt $deadline -and -not $healthy) {
        Start-Sleep -Seconds 15
        $ErrorActionPreference = 'Continue'
        $state = az containerapp show `
            --name $InstanceName `
            --resource-group $AcaConfig.ResourceGroupName `
            --query "properties.provisioningState" `
            --output tsv `
            2>$null
        $ErrorActionPreference = 'Stop'
        if ($state -eq "Succeeded") {
            $healthy = $true
        }
    }

    # ----------------------------------------------------------
    # Step 6: Audit log
    # ----------------------------------------------------------
    $auditResult = "FAILED"
    if ($success -and $healthy) { $auditResult = "OK" }
    Write-AcaAudit -Action "Add" -Target $InstanceName -Result $auditResult

    # ----------------------------------------------------------
    # Step 7: Return
    # ----------------------------------------------------------
    return ($success -and $healthy)
}

# ---------------------------------------------------------------
# Remove-AcaInstance
# Deletes a container app instance and its env storage config.
# Optionally wipes the associated file share.
#
# Parameters:
#   InstanceName  - e.g. "mongo-migrator-3"
#   WipeShare     - if set, delete the migration data file share contents
#   WhatIf        - if set, prints what WOULD run without executing
#
# Returns: [bool] $true on success (app deleted), $false on failure
# ---------------------------------------------------------------
function Remove-AcaInstance {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$InstanceName,

        [switch]$WipeShare,

        [switch]$WhatIf
    )

    # ----------------------------------------------------------
    # Step 1: Derive index and derived names
    # e.g. "mongo-migrator-3" -> index "3"
    #   env storage config: "migrationdata3"  (no hyphen - ACA naming convention)
    #   file share name:    "migration-data-3" (with hyphens - Azure Files naming)
    # ----------------------------------------------------------
    $indexMatch = [regex]::Match($InstanceName, '\d+$')
    if (-not $indexMatch.Success) {
        Write-Host "Remove-AcaInstance: Cannot determine index from instance name '$InstanceName'." -ForegroundColor Red
        return $false
    }
    $index = $indexMatch.Value
    $storageConfigName = "migrationdata$index"
    $fileShareName = "migration-data-$index"

    # ----------------------------------------------------------
    # Step 2: WhatIf - print plan and return without any mutation
    # ----------------------------------------------------------
    if ($WhatIf) {
        Write-Host "WOULD DELETE: $InstanceName" -ForegroundColor Cyan
        Write-Host "  Env storage config: $storageConfigName"
        if ($WipeShare) {
            Write-Host "  WOULD WIPE file share: $fileShareName"
        }
        return $true
    }

    # ----------------------------------------------------------
    # Step 3: Delete container app
    # ----------------------------------------------------------
    $ErrorActionPreference = 'Continue'
    az containerapp delete `
        --name $InstanceName `
        --resource-group $AcaConfig.ResourceGroupName `
        --yes `
        --output none `
        2>$null
    $appDeleted = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = 'Stop'

    # ----------------------------------------------------------
    # Step 4: Remove env storage config (idempotent - don't fail if missing)
    # ----------------------------------------------------------
    $ErrorActionPreference = 'Continue'
    az containerapp env storage remove `
        --name $AcaConfig.EnvironmentName `
        --resource-group $AcaConfig.ResourceGroupName `
        --storage-name $storageConfigName `
        --yes `
        --output none `
        2>$null
    $ErrorActionPreference = 'Stop'

    # ----------------------------------------------------------
    # Step 5: Optionally wipe file share contents
    # ----------------------------------------------------------
    if ($WipeShare) {
        $ErrorActionPreference = 'Continue'
        $storageKey = az storage account keys list `
            --account-name "mongomigratorstor" `
            --resource-group $AcaConfig.ResourceGroupName `
            --query "[0].value" `
            --output tsv `
            2>$null
        $keyExitCode = $LASTEXITCODE
        $ErrorActionPreference = 'Stop'

        if ($keyExitCode -eq 0 -and -not [string]::IsNullOrEmpty($storageKey)) {
            $ErrorActionPreference = 'Continue'
            az storage file delete-batch `
                --source $fileShareName `
                --account-name "mongomigratorstor" `
                --account-key $storageKey `
                --output none `
                2>$null
            $ErrorActionPreference = 'Stop'
        }
    }

    # ----------------------------------------------------------
    # Step 6: Audit log
    # PS 5.1: if-else not assignable inline; use two-step assignment.
    # ----------------------------------------------------------
    $res = "PARTIAL_FAILED"
    if ($appDeleted) { $res = "OK" }
    Write-AcaAudit -Action "Remove" -Target $InstanceName -Result $res

    # ----------------------------------------------------------
    # Step 7: Return
    # ----------------------------------------------------------
    return $appDeleted
}
