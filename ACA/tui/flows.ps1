# ============================================================
# ACA TUI - Flow Orchestrations
# flows.ps1 - dot-source this file; do not run directly
# ============================================================
# Part D1: Invoke-AddFlow
# Part D2: Invoke-RemoveFlow  (added by D2)
# Part D3: Invoke-UpdateFlow  (added by D3)
# Part D4: Invoke-BuildFlow   (added by D4)
# Part D6: Invoke-RollbackFlow (added by D6)
# ============================================================
# PS 5.1 compatibility: no ternary, no ??, no &&
# No param() block at file level - this is a dot-sourced module
# ============================================================

# ---------------------------------------------------------------
# Invoke-AddFlow
# Orchestrates the end-to-end flow for adding a new ACA instance.
# Gap-fills the instance index, selects an image tag, confirms
# with the operator, then delegates to Add-AcaInstance.
#
# Uses $AcaConfig from caller scope.
# Depends on: Get-AcaInstanceStatus (state.ps1)
#             Get-AcaGapFillIndex   (instances.ps1)
#             Add-AcaInstance       (instances.ps1)
#             Get-AcaTags           (jfrog.ps1)
#             Select-AcaItems       (ui.ps1)
#             Confirm-AcaAction     (ui.ps1)
#             Show-AcaStatusTable   (state.ps1)
# ---------------------------------------------------------------
function Invoke-AddFlow {
    [CmdletBinding()]
    param()

    # ----------------------------------------------------------
    # Step 1: Get current instance list
    # ----------------------------------------------------------
    $statuses = Get-AcaInstanceStatus

    # ----------------------------------------------------------
    # Step 2: Show current status table so operator sees live state
    # ----------------------------------------------------------
    Show-AcaStatusTable -Statuses $statuses

    # ----------------------------------------------------------
    # Step 3: Extract existing indices from instance names
    # e.g. "mongo-migrator-1" -> 1, "mongo-migrator-2" -> 2
    # ----------------------------------------------------------
    $indices = @()
    foreach ($s in $statuses) {
        $m = [regex]::Match($s.Name, '\d+$')
        if ($m.Success) {
            $indices += [int]$m.Value
        }
    }

    # ----------------------------------------------------------
    # Step 4: Gap-fill to find the next instance index
    # ----------------------------------------------------------
    $nextIndex = Get-AcaGapFillIndex -ExistingIndices $indices
    $newName = "$($AcaConfig.ContainerAppNamePrefix)-$nextIndex"

    Write-Host ""
    Write-Host "New instance will be: $newName" -ForegroundColor Cyan

    # ----------------------------------------------------------
    # Step 5: Read available image tags
    # ----------------------------------------------------------
    Write-Host "Reading current tags..." -ForegroundColor Yellow
    $tagInfo = Get-AcaTags

    if ($tagInfo.Source -eq "None" -or $null -eq $tagInfo.Tags -or $tagInfo.Tags.Count -eq 0) {
        Write-Host "No built image found. Run 'Build' first." -ForegroundColor Yellow
        return
    }

    Write-Host "Tag source: $($tagInfo.Source)" -ForegroundColor Gray

    # ----------------------------------------------------------
    # Step 6: Select image tag - default to most recent, offer choice
    # ----------------------------------------------------------
    $selectedTag = $tagInfo.Tags[0]

    if ($tagInfo.Tags.Count -gt 1) {
        $offerChoice = Confirm-AcaAction -Message "Use most recent tag '$selectedTag'? (No to pick a different tag)"  -Default
        if (-not $offerChoice) {
            $picked = Select-AcaItems -Items $tagInfo.Tags -Prompt "Select image tag"
            if ($picked.Count -gt 0) {
                $selectedTag = $picked[0]
            }
        }
    } else {
        Write-Host "Using tag: $selectedTag" -ForegroundColor Gray
    }

    # ----------------------------------------------------------
    # Step 7: Final confirmation before deploy
    # ----------------------------------------------------------
    $proceed = Confirm-AcaAction -Message "Proceed to deploy $newName with tag '$selectedTag'?"
    if (-not $proceed) {
        Write-Host "Aborted." -ForegroundColor Yellow
        return
    }

    # ----------------------------------------------------------
    # Step 8: Deploy
    # ----------------------------------------------------------
    Write-Host "Deploying $newName with tag $selectedTag..." -ForegroundColor Yellow
    $success = Add-AcaInstance -InstanceName $newName -ImageTag $selectedTag

    # ----------------------------------------------------------
    # Step 9: Report result
    # ----------------------------------------------------------
    if ($success) {
        Write-Host "DONE: $newName deployed successfully." -ForegroundColor Green
    } else {
        Write-Host "FAILED: $newName deployment failed. Check logs." -ForegroundColor Red
    }
}

# ---------------------------------------------------------------
# Invoke-RemoveFlow
# Orchestrates the end-to-end flow for removing ACA instances.
# Enforces the sacred-instance guard: ChangeStreamTailing instances
# require typing "INTERRUPT-TAILING" exactly to proceed.
#
# Uses $AcaConfig from caller scope.
# Depends on: Get-AcaInstanceStatus (state.ps1)
#             Remove-AcaInstance    (instances.ps1)
#             Show-AcaStatusTable   (state.ps1)
#             Select-AcaItems       (ui.ps1)
#             Confirm-AcaAction     (ui.ps1)
#             Read-AcaHighFriction  (ui.ps1)
# ---------------------------------------------------------------
function Invoke-RemoveFlow {
    [CmdletBinding()]
    param()

    # ----------------------------------------------------------
    # Step 1: Get current instance list
    # ----------------------------------------------------------
    $statuses = Get-AcaInstanceStatus
    if ($statuses.Count -eq 0) {
        Write-Host "No instances found." -ForegroundColor Yellow
        return
    }

    # ----------------------------------------------------------
    # Step 2: Show current status table so operator sees live state
    # ----------------------------------------------------------
    Show-AcaStatusTable -Statuses $statuses

    # ----------------------------------------------------------
    # Step 3: Build display list and let operator select instances
    # ----------------------------------------------------------
    $displayItems = $statuses | ForEach-Object {
        "$($_.Name) [$($_.Classification)] Image: $($_.ImageTag)"
    }

    $selected = Select-AcaItems `
        -Items $displayItems `
        -Prompt "Select instances to REMOVE (SPACE to select, ENTER to confirm)" `
        -Multi

    if (-not $selected -or $selected.Count -eq 0) {
        Write-Host "No instances selected." -ForegroundColor Yellow
        return
    }

    # ----------------------------------------------------------
    # Step 4: Extract instance names from display strings
    # Display format: "mongo-migrator-3 [Safe] Image: 20260623120000"
    # Split on space; first token is the name.
    # ----------------------------------------------------------
    $selectedNames = $selected | ForEach-Object { ($_ -split ' ')[0] }

    # ----------------------------------------------------------
    # Step 5: Process each selected instance
    # ----------------------------------------------------------
    foreach ($name in $selectedNames) {
        $status = $statuses | Where-Object { $_.Name -eq $name }

        $doRemove = $false

        if ($status.Classification -eq "Sacred") {
            # ChangeStreamTailing - high friction required
            Write-Host ""
            Write-Host "[SACRED] $name is in ChangeStreamTailing phase!" -ForegroundColor Red
            Write-Host "Removing this instance WILL interrupt an active change-stream migration." -ForegroundColor Red
            $confirmed = Read-AcaHighFriction `
                -Phrase "INTERRUPT-TAILING" `
                -Context "Remove $name (ChangeStreamTailing - live migration in progress)"
            $doRemove = $confirmed
        } elseif ($status.Classification -eq "Busy") {
            # Busy but not Sacred
            Write-Host ""
            Write-Host "[BUSY] $name has an active migration job." -ForegroundColor Yellow
            $doRemove = Confirm-AcaAction -Message "Remove $name anyway? Job will be interrupted."
        } elseif ($status.Classification -eq "Unknown") {
            # Unknown state - require manual confirm, no default yes
            Write-Host ""
            Write-Host "[UNKNOWN] Cannot determine job state for $name." -ForegroundColor Yellow
            $doRemove = Confirm-AcaAction -Message "Remove $name? State is unknown - proceed with caution."
        } else {
            # Safe - standard confirm with default Yes
            $doRemove = Confirm-AcaAction -Message "Remove $name?" -Default
        }

        if (-not $doRemove) {
            Write-Host "Skipped: $name" -ForegroundColor Gray
            continue
        }

        # ----------------------------------------------------------
        # Prompt for share wipe (default No - irreversible operation)
        # ----------------------------------------------------------
        $wipeShare = Confirm-AcaAction -Message "Wipe file share data for $name? (CAUTION: irreversible)"

        Write-Host "Removing $name..." -ForegroundColor Yellow
        $success = Remove-AcaInstance -InstanceName $name -WipeShare:$wipeShare
        if ($success) {
            Write-Host "REMOVED: $name" -ForegroundColor Green
        } else {
            Write-Host "PARTIAL FAILURE removing $name. Check az CLI output above." -ForegroundColor Red
        }
    }
}

# ---------------------------------------------------------------
# Part D3: Invoke-UpdateFlow — image-update selected instances
# ---------------------------------------------------------------
function Invoke-UpdateFlow {
    [CmdletBinding()]
    param()

    # ----------------------------------------------------------
    # Step 1: Get current instance statuses
    # ----------------------------------------------------------
    $statuses = Get-AcaInstanceStatus
    if ($statuses.Count -eq 0) {
        Write-Host "No instances found." -ForegroundColor Yellow
        return
    }

    # ----------------------------------------------------------
    # Step 2: Show current status table
    # ----------------------------------------------------------
    Show-AcaStatusTable -Statuses $statuses

    # ----------------------------------------------------------
    # Step 3: Build display list and multi-select instances
    # ----------------------------------------------------------
    $displayItems = $statuses | ForEach-Object {
        "$($_.Name) [$($_.Classification)] Tag: $($_.ImageTag)"
    }

    $selected = Select-AcaItems `
        -Items $displayItems `
        -Prompt "Select instances to UPDATE" `
        -Multi

    if (-not $selected -or $selected.Count -eq 0) {
        Write-Host "No instances selected." -ForegroundColor Yellow
        return
    }

    $selectedNames = $selected | ForEach-Object { ($_ -split ' ')[0] }

    # ----------------------------------------------------------
    # Step 4: Choose image tag
    # ----------------------------------------------------------
    $tagInfo = Get-AcaTags
    if ($tagInfo.Source -eq "None") {
        Write-Host "No built image found. Run 'Build' first." -ForegroundColor Yellow
        return
    }

    $defaultTag = $tagInfo.Tags[0]
    $useDefault = Confirm-AcaAction -Message "Update to last-built tag '$defaultTag'?" -Default

    $chosenTag = ""
    if ($useDefault) {
        $chosenTag = $defaultTag
    } else {
        $tagChoices = $tagInfo.Tags + @("Enter manually...")
        $tagDisplay = Select-AcaItems -Items $tagChoices -Prompt "Select image tag"
        $chosenTag = $tagDisplay[0]
        if ($chosenTag -eq "Enter manually...") {
            $chosenTag = Read-Host "Enter exact image tag"
        }
    }

    if ([string]::IsNullOrEmpty($chosenTag)) {
        Write-Host "No tag selected." -ForegroundColor Yellow
        return
    }

    # ----------------------------------------------------------
    # Step 5: Summary before proceeding
    # ----------------------------------------------------------
    Write-Host ""
    Write-Host "Plan: update $($selectedNames.Count) instance(s) to tag '$chosenTag'" -ForegroundColor Cyan
    foreach ($n in $selectedNames) {
        Write-Host "  - $n" -ForegroundColor Cyan
    }

    $proceed = Confirm-AcaAction -Message "Proceed?" -Default
    if (-not $proceed) { return }

    # ----------------------------------------------------------
    # Step 6: Process each selected instance with busy-guard
    # ----------------------------------------------------------
    foreach ($name in $selectedNames) {
        $status = $statuses | Where-Object { $_.Name -eq $name }

        $doUpdate = $false

        if ($status.Classification -eq "Sacred") {
            # ChangeStreamTailing — high friction required
            Write-Host ""
            Write-Host "[SACRED] $name is in ChangeStreamTailing phase!" -ForegroundColor Red
            $confirmed = Read-AcaHighFriction `
                -Phrase "INTERRUPT-TAILING" `
                -Context "Update $name (ChangeStreamTailing — WILL restart the running migration)"
            $doUpdate = $confirmed
        } elseif ($status.Classification -eq "Busy") {
            Write-Host ""
            Write-Host "[BUSY] $name has an active migration job." -ForegroundColor Yellow
            $doUpdate = Confirm-AcaAction -Message "Update $name anyway? Job will be interrupted."
        } elseif ($status.Classification -eq "Unknown") {
            Write-Host ""
            Write-Host "[UNKNOWN] Cannot determine job state for $name." -ForegroundColor Yellow
            $doUpdate = Confirm-AcaAction -Message "Update $name? State is unknown."
        } else {
            $doUpdate = $true
        }

        if (-not $doUpdate) {
            Write-Host "Skipped: $name" -ForegroundColor Gray
            continue
        }

        # Build full image reference (ACA VNet port 22609)
        $fullImage = "$($AcaConfig.JFrogRegistryServerForACA)/$($AcaConfig.JFrogRepository)/mongo-migration:${chosenTag}"

        Write-Host "Updating $name to $fullImage..." -ForegroundColor Yellow
        $ErrorActionPreference = 'Continue'
        az containerapp update `
            --name $name `
            --resource-group $AcaConfig.ResourceGroupName `
            --image $fullImage `
            --output none `
            2>$null
        $updateOk = ($LASTEXITCODE -eq 0)
        $ErrorActionPreference = 'Stop'

        if ($updateOk) {
            # Poll for Succeeded provisioning state (up to 3 minutes, 10s intervals)
            $deadline = (Get-Date).AddMinutes(3)
            $healthy = $false
            while ((Get-Date) -lt $deadline -and -not $healthy) {
                Start-Sleep -Seconds 10
                $ErrorActionPreference = 'Continue'
                $state = az containerapp show `
                    --name $name `
                    --resource-group $AcaConfig.ResourceGroupName `
                    --query "properties.provisioningState" `
                    --output tsv `
                    2>$null
                $ErrorActionPreference = 'Stop'
                if ($state -eq "Succeeded") { $healthy = $true }
            }

            # PS 5.1: no ternary — explicit assignment before conditional
            $auditResult = "TIMEOUT"
            if ($healthy) { $auditResult = "OK" }
            Write-AcaAudit -Action "Update" -Target "${name}->${chosenTag}" -Result $auditResult

            if ($healthy) {
                Write-Host "UPDATED: $name to $chosenTag" -ForegroundColor Green
            } else {
                Write-Host "UPDATE sent but health check timed out: $name" -ForegroundColor Yellow
            }
        } else {
            Write-AcaAudit -Action "Update" -Target "${name}->${chosenTag}" -Result "FAILED"
            Write-Host "FAILED to update: $name" -ForegroundColor Red
        }
    }
}

# ---------------------------------------------------------------
# Part D6: Invoke-RollbackFlow -- activate a previous revision
# ---------------------------------------------------------------
function Invoke-RollbackFlow {
    [CmdletBinding()]
    param()

    # ----------------------------------------------------------
    # Step 1-3: Get instance list and show status table
    # ----------------------------------------------------------
    $statuses = Get-AcaInstanceStatus
    if ($statuses.Count -eq 0) {
        Write-Host "No instances found." -ForegroundColor Yellow
        return
    }
    Show-AcaStatusTable -Statuses $statuses

    # ----------------------------------------------------------
    # Step 4: Single-select the instance to roll back
    # ----------------------------------------------------------
    $displayItems = $statuses | ForEach-Object { "$($_.Name) [$($_.Classification)]" }
    $selected = Select-AcaItems -Items $displayItems -Prompt "Select instance to roll back"
    if (-not $selected -or $selected.Count -eq 0) { return }
    $instanceName = ($selected[0] -split ' ')[0]

    # ----------------------------------------------------------
    # Step 5: Apply busy-guard
    # ----------------------------------------------------------
    $status = $statuses | Where-Object { $_.Name -eq $instanceName }
    $doRollback = $false
    if ($status.Classification -eq "Sacred") {
        Write-Host "[SACRED] $instanceName is in ChangeStreamTailing!" -ForegroundColor Red
        $confirmed = Read-AcaHighFriction -Phrase "INTERRUPT-TAILING" `
            -Context "Rollback $instanceName (ChangeStreamTailing active - WILL interrupt migration)"
        $doRollback = $confirmed
    } elseif ($status.Classification -eq "Busy") {
        $doRollback = Confirm-AcaAction -Message "Rollback $instanceName? Active migration will be interrupted."
    } elseif ($status.Classification -eq "Unknown") {
        $doRollback = Confirm-AcaAction -Message "Rollback $instanceName? State unknown - proceed with caution."
    } else {
        $doRollback = Confirm-AcaAction -Message "Roll back $instanceName?" -Default
    }
    if (-not $doRollback) { Write-Host "Rollback cancelled." -ForegroundColor Gray; return }

    # ----------------------------------------------------------
    # Step 6: List revisions
    # ----------------------------------------------------------
    $ErrorActionPreference = 'Continue'
    $revisionsJson = az containerapp revision list `
        --name $instanceName `
        --resource-group $AcaConfig.ResourceGroupName `
        --query "[].{name:name, active:properties.active, createdTime:properties.createdTime, replicas:properties.replicas}" `
        --output json `
        2>$null
    $ErrorActionPreference = 'Stop'
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrEmpty($revisionsJson)) {
        Write-Host "Could not list revisions for $instanceName." -ForegroundColor Red; return
    }
    $revisions = $revisionsJson | ConvertFrom-Json
    if ($revisions.Count -eq 0) { Write-Host "No revisions found."; return }

    # ----------------------------------------------------------
    # Step 7: Build revision display list and select
    # PS 5.1: no ternary - use if/else block
    # ----------------------------------------------------------
    $revDisplayItems = $revisions | ForEach-Object {
        $label = "       "
        if ($_.active) { $label = "[ACTIVE]" }
        "$($_.name) $label  created: $($_.createdTime)"
    }
    $revSelected = Select-AcaItems -Items $revDisplayItems -Prompt "Select revision to activate"
    if (-not $revSelected -or $revSelected.Count -eq 0) { return }
    $revisionName = ($revSelected[0] -split ' ')[0]

    # ----------------------------------------------------------
    # Step 8: Confirm and activate
    # ----------------------------------------------------------
    $confirmed = Confirm-AcaAction -Message "Activate revision '$revisionName' for $instanceName?"
    if (-not $confirmed) { return }

    Write-Host "Activating revision $revisionName..." -ForegroundColor Yellow
    $ErrorActionPreference = 'Continue'
    az containerapp revision activate `
        --revision $revisionName `
        --resource-group $AcaConfig.ResourceGroupName `
        --output none `
        2>$null
    $activateOk = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = 'Stop'

    # PS 5.1: no ternary
    $auditResult = "FAILED"
    if ($activateOk) { $auditResult = "OK" }
    Write-AcaAudit -Action "Rollback" -Target "${instanceName}->${revisionName}" -Result $auditResult

    if ($activateOk) {
        Write-Host "ACTIVATED: $revisionName for $instanceName" -ForegroundColor Green
    } else {
        Write-Host "FAILED to activate revision. Check az CLI output." -ForegroundColor Red
    }
}

# ---------------------------------------------------------------
# Part D4: Invoke-BuildFlow -- build image + offer update handoff
# ---------------------------------------------------------------
function Invoke-BuildFlow {
    [CmdletBinding()]
    param()

    # ----------------------------------------------------------
    # Step 1: Announce intent
    # ----------------------------------------------------------
    Write-Host "Building new Docker image..." -ForegroundColor Cyan

    # ----------------------------------------------------------
    # Step 2: Confirm before kicking off build
    # ----------------------------------------------------------
    $confirmed = Confirm-AcaAction -Message "Build a new image (docker build + push to JFrog)?" -Default
    if (-not $confirmed) { return }

    # ----------------------------------------------------------
    # Step 3: Run the build
    # ----------------------------------------------------------
    Write-Host "Starting build..." -ForegroundColor Yellow
    $builtTag = $null
    try {
        $builtTag = Invoke-AcaBuild
    } catch {
        Write-Host "Build failed: $_" -ForegroundColor Red
        return
    }
    Write-Host "Build complete. Tag: $builtTag" -ForegroundColor Green

    # ----------------------------------------------------------
    # Step 4: Offer update handoff
    # ----------------------------------------------------------
    $doUpdate = Confirm-AcaAction -Message "Update instances now with tag '$builtTag'?"
    if ($doUpdate) {
        Invoke-UpdateFlow
    } else {
        Write-Host "Tag '$builtTag' cached. Use 'Update' from the main menu when ready." -ForegroundColor Gray
    }
}
