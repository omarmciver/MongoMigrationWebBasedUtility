# ============================================================
# ACA TUI - JFrog Image Build and Tag Lifecycle
# jfrog.ps1 - dot-source this file; do not run directly
# ============================================================
# Provides:
#   New-AcaImageTag  - generate second-precision timestamp tag (yyyyMMddHHmmss)
#   Invoke-AcaBuild  - build and push Docker image to JFrog Artifactory
#   Get-AcaTags      - retrieve available image tags with fallback chain
#
# Consumed by: ACA/tui/flows.ps1 and ACA/manage-aca.ps1
# ============================================================
# PS 5.1 compatibility: no ternary, no ??, no &&
# No param() block at file level - this is a dot-sourced module
# $AcaConfig is loaded by ACA/manage-aca.ps1 before dot-sourcing this file
# Write-AcaAudit is provided by ACA/tui/bootstrap.ps1
# ============================================================

# Capture the module directory at dot-source time.
# $PSScriptRoot inside function bodies refers to the CALLING script's directory,
# so we snapshot the correct path here before any function is invoked.
$script:_JFrogDir = $PSScriptRoot

# ---------------------------------------------------------------
# New-AcaImageTag
# Returns a second-precision timestamp tag (yyyyMMddHHmmss, 14 chars).
# Second precision avoids minute-precision collisions that older scripts had.
# ---------------------------------------------------------------
function New-AcaImageTag {
    [CmdletBinding()]
    param()
    return (Get-Date -Format "yyyyMMddHHmmss")
}

# ---------------------------------------------------------------
# Invoke-AcaBuild
# Builds the Docker image and pushes it to JFrog Artifactory.
# Matches the docker build/push pattern from deploy-to-aca-jfrog.ps1 exactly.
#
# Parameters:
#   -Tag       Image tag to use; calls New-AcaImageTag if omitted
#   -WhatIf    Print commands that would run without executing them
#   -SkipLogin Skip the docker login step
#
# Returns: [string] the image tag used (even in -WhatIf mode)
# ---------------------------------------------------------------
function Invoke-AcaBuild {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$false)]
        [string]$Tag = "",

        [Parameter(Mandatory=$false)]
        [switch]$WhatIf,

        [Parameter(Mandatory=$false)]
        [switch]$SkipLogin
    )

    if ([string]::IsNullOrEmpty($Tag)) {
        $Tag = New-AcaImageTag
    }

    # Full image path for local Docker operations (standard port, not ACA port)
    $fullPath = "$($AcaConfig.JFrogRegistryServer)/$($AcaConfig.JFrogRepository)/mongo-migration"
    $fullImageWithTag = "${fullPath}:${Tag}"

    # Derive base image registry and repository — mirrors deploy-to-aca-jfrog.ps1 defaults:
    #   JFrogBaseImageRegistry  = JFrogRegistryServer (same server)
    #   JFrogBaseImageRepository = JFrogRepository with -stage/-local replaced by -group
    $baseImageRegistry = $AcaConfig.JFrogRegistryServer
    $baseImageRepo     = $AcaConfig.JFrogRepository -replace '-stage$', '-group' -replace '-local$', '-group'

    # Build arg values — defaults match deploy-to-aca-jfrog.ps1
    $debianRepo      = $AcaConfig.JFrogDebianRepo
    $debianComponent = $AcaConfig.JFrogDebianComponent
    $debianDistro    = "bookworm"
    $nugetRepo       = $AcaConfig.JFrogNuGetRepo
    $debianUrl       = "https://$($AcaConfig.JFrogRegistryServer)/artifactory/$debianRepo"
    $nugetUrl        = "https://$($AcaConfig.JFrogRegistryServer)/artifactory/api/nuget/v3/$nugetRepo/index.json"

    # Repo root = ACA/tui/../../  (PS5.1: Join-Path is 2-arg only)
    $acaroot  = Join-Path $script:_JFrogDir ".."
    $repoRoot = (Resolve-Path (Join-Path $acaroot "..")).Path
    $cachePath = Join-Path $acaroot ".aca-last-tag"

    # --- WhatIf mode: print commands without executing ---
    if ($WhatIf) {
        if (-not $SkipLogin) {
            Write-Host "WOULD RUN: docker login $($AcaConfig.JFrogRegistryServer) -u $($AcaConfig.JFrogUsername) --password-stdin" -ForegroundColor Cyan
        }
        Write-Host "WOULD RUN: (cd $repoRoot)" -ForegroundColor Cyan
        Write-Host ("WOULD RUN: docker build" +
            " -f MongoMigrationWebApp/Dockerfile.jfrog" +
            " --build-arg JFROG_REGISTRY=$baseImageRegistry" +
            " --build-arg JFROG_REPOSITORY=$baseImageRepo" +
            " --build-arg JFROG_DEBIAN_REPO=$debianRepo" +
            " --build-arg JFROG_DEBIAN_URL=`"$debianUrl`"" +
            " --build-arg JFROG_DEBIAN_DISTRIBUTION=$debianDistro" +
            " --build-arg JFROG_DEBIAN_COMPONENT=$debianComponent" +
            " --build-arg JFROG_NUGET_URL=`"$nugetUrl`"" +
            " --build-arg JFROG_USERNAME=$($AcaConfig.JFrogUsername)" +
            " --build-arg JFROG_PASSWORD=***" +
            " -t $fullImageWithTag" +
            " .") -ForegroundColor Cyan
        Write-Host "WOULD RUN: docker push $fullImageWithTag" -ForegroundColor Cyan
        return $Tag
    }

    # --- Step A: docker login ---
    if (-not $SkipLogin) {
        Write-Host "Logging in to JFrog registry..." -ForegroundColor Gray
        $ErrorActionPreference = 'Continue'
        $AcaConfig.JFrogPassword | docker login $AcaConfig.JFrogRegistryServer -u $AcaConfig.JFrogUsername --password-stdin 2>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) {
            $ErrorActionPreference = 'Stop'
            throw "Build failed at step: docker login"
        }
        $ErrorActionPreference = 'Stop'
        Write-Host "Successfully logged in to JFrog." -ForegroundColor Green
    }

    # --- Step B: docker build (from repo root, matching deploy-to-aca-jfrog.ps1) ---
    Write-Host "Building Docker image with JFrog base images..." -ForegroundColor Gray
    Write-Host "  Base registry:   $baseImageRegistry" -ForegroundColor Gray
    Write-Host "  Base repository: $baseImageRepo" -ForegroundColor Gray
    Write-Host "  Target image:    $fullImageWithTag" -ForegroundColor Gray
    $ErrorActionPreference = 'Continue'
    Push-Location $repoRoot
    try {
        docker build --progress=plain `
            -f MongoMigrationWebApp/Dockerfile.jfrog `
            --build-arg JFROG_REGISTRY=$baseImageRegistry `
            --build-arg JFROG_REPOSITORY=$baseImageRepo `
            --build-arg JFROG_DEBIAN_REPO=$debianRepo `
            --build-arg "JFROG_DEBIAN_URL=$debianUrl" `
            --build-arg JFROG_DEBIAN_DISTRIBUTION=$debianDistro `
            --build-arg JFROG_DEBIAN_COMPONENT=$debianComponent `
            --build-arg "JFROG_NUGET_URL=$nugetUrl" `
            --build-arg JFROG_USERNAME=$($AcaConfig.JFrogUsername) `
            --build-arg JFROG_PASSWORD=$($AcaConfig.JFrogPassword) `
            -t $fullImageWithTag `
            . 2>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) {
            $ErrorActionPreference = 'Stop'
            throw "Build failed at step: docker build"
        }
    } finally {
        Pop-Location
    }
    $ErrorActionPreference = 'Stop'
    Write-Host "Docker image built successfully." -ForegroundColor Green

    # --- Step C: docker push ---
    Write-Host "Pushing image to JFrog..." -ForegroundColor Gray
    $ErrorActionPreference = 'Continue'
    docker push $fullImageWithTag 2>&1 | Out-Host
    if ($LASTEXITCODE -ne 0) {
        $ErrorActionPreference = 'Stop'
        throw "Build failed at step: docker push"
    }
    $ErrorActionPreference = 'Stop'
    Write-Host "Docker image pushed successfully." -ForegroundColor Green

    # Write tag to cache file (ACA/.aca-last-tag)
    Set-Content -Path $cachePath -Value $Tag -NoNewline

    # Audit log — NEVER include JFrogPassword
    Write-AcaAudit -Action "Build" -Target "${fullPath}:${Tag}" -Result "OK"

    return $Tag
}

# ---------------------------------------------------------------
# Get-AcaTags
# Returns available image tags using a fallback chain:
#   1. Cache file (ACA/.aca-last-tag) — fastest, set after each successful build
#   2. Artifactory Docker Registry API — best-effort REST query, top $TopN tags
#   3. Running ACA container apps — current image tags from live instances
#   4. Sentinel @{ Tags=@(); Source="None" } — caller should prompt to build first
#
# Parameters:
#   -TopN              How many tags to return from Artifactory REST (default: 5)
#   -ResourceGroupName Override resource group (defaults to $AcaConfig.ResourceGroupName)
#   -EnvironmentName   ACA environment name (reserved for future use)
#
# Returns: [hashtable] @{
#   Tags   = [string[]]  ordered list of tags, most recent first
#   Source = "Cache" | "ArtifactoryREST" | "RunningInstances" | "None"
# }
# ---------------------------------------------------------------
function Get-AcaTags {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$false)]
        [int]$TopN = 5,

        [Parameter(Mandatory=$false)]
        [string]$ResourceGroupName = "",

        [Parameter(Mandatory=$false)]
        [string]$EnvironmentName = ""
    )

    $cachePath = Join-Path (Join-Path $script:_JFrogDir "..") ".aca-last-tag"

    # 1. Cache file check
    if (Test-Path -LiteralPath $cachePath) {
        $cached = (Get-Content -LiteralPath $cachePath -Raw)
        if (-not [string]::IsNullOrWhiteSpace($cached)) {
            return @{ Tags = @($cached.Trim()); Source = "Cache" }
        }
    }

    # 2. Best-effort: Artifactory Docker Registry API
    try {
        $credBytes = [System.Text.Encoding]::ASCII.GetBytes("$($AcaConfig.JFrogUsername):$($AcaConfig.JFrogPassword)")
        $creds = [System.Convert]::ToBase64String($credBytes)
        $uri = "https://$($AcaConfig.JFrogRegistryServer)/artifactory/api/docker/$($AcaConfig.JFrogRepository)/v2/mongo-migration/tags/list"
        $response = Invoke-WebRequest `
            -Uri $uri `
            -Headers @{ Authorization = "Basic $creds" } `
            -UseBasicParsing `
            -TimeoutSec 10 `
            -ErrorAction Stop
        $parsed = $response.Content | ConvertFrom-Json
        if ($null -ne $parsed -and $null -ne $parsed.tags -and $parsed.tags.Count -gt 0) {
            $tags = @($parsed.tags | Sort-Object -Descending | Select-Object -First $TopN)
            if ($tags.Count -gt 0) {
                return @{ Tags = $tags; Source = "ArtifactoryREST" }
            }
        }
    } catch {
        # Graceful failure — REST call is best-effort only; do not surface exception
    }

    # 3. Fallback: current image tags from running ACA container apps
    $rgName = $ResourceGroupName
    if ([string]::IsNullOrEmpty($rgName)) {
        $rgName = $AcaConfig.ResourceGroupName
    }

    try {
        $ErrorActionPreference = 'Continue'
        $images = az containerapp list `
            --resource-group $rgName `
            --query "[?starts_with(name,'$($AcaConfig.ContainerAppNamePrefix)')].properties.template.containers[0].image" `
            --output tsv 2>$null
        if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrEmpty($images)) {
            $tags = @(
                $images -split "`n" |
                Where-Object { $_ -match ':' } |
                ForEach-Object { ($_ -split ':')[-1] } |
                Select-Object -Unique
            )
            if ($tags.Count -gt 0) {
                $ErrorActionPreference = 'Stop'
                return @{ Tags = $tags; Source = "RunningInstances" }
            }
        }
        $ErrorActionPreference = 'Stop'
    } catch {
        $ErrorActionPreference = 'Stop'
    }

    # 4. Sentinel: no tags found — caller should prompt the user to build first
    return @{ Tags = @(); Source = "None" }
}
