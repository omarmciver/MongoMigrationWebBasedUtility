# ============================================================
# ACA TUI - UI Abstraction Layer
# ui.ps1 - dot-source this file; do not run directly
# ============================================================
# Provides:
#   Select-AcaItems        - multi or single select from a list
#   Confirm-AcaAction      - yes/no confirmation prompt
#   Show-AcaSpin           - run a scriptblock with a spinner message
#   Read-AcaHighFriction   - high-friction exact-phrase confirmation
#
# Consumed by: ACA/tui/flows.ps1 and ACA/manage-aca.ps1
# ============================================================
# PS 5.1 compatibility: no ternary, no ??, no &&
# No param() block at file level - this is a dot-sourced module
# $script:UseGum is set by ACA/tui/bootstrap.ps1
# ============================================================

# ---------------------------------------------------------------
# Select-AcaItems
# Shows a selectable list; returns [string[]] of chosen items.
# ---------------------------------------------------------------
function Select-AcaItems {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Items,

        [Parameter(Mandatory = $true)]
        [string]$Prompt,

        [switch]$Multi,

        [string[]]$Headers
    )

    if ($script:UseGum) {
        # gum backend
        if ($Multi) {
            $raw = $Items | & gum choose --no-limit --header $Prompt
        } else {
            $raw = $Items | & gum choose --header $Prompt
        }

        if ($null -eq $raw -or $raw -eq '') {
            return ,[string[]]@()
        }

        $selected = $raw -split "`n" | Where-Object { $_ -ne '' }
        return ,[string[]]$selected
    } else {
        # Native backend
        Write-Host ""
        Write-Host $Prompt -ForegroundColor Cyan

        if ($Headers -and $Headers.Count -gt 0) {
            Write-Host ("  " + ($Headers -join "  ")) -ForegroundColor DarkGray
        }

        for ($i = 0; $i -lt $Items.Count; $i++) {
            Write-Host "  [$($i + 1)] $($Items[$i])"
        }

        if ($Multi) {
            Write-Host "  [0] Done (finish selection)" -ForegroundColor DarkGray
            $selected = @()

            :multiLoop while ($true) {
                $raw = Read-Host "Enter number (0 to finish)"
                $parsed = 0
                if (-not [int]::TryParse($raw, [ref]$parsed)) {
                    Write-Host "  Invalid input. Enter a number." -ForegroundColor Yellow
                    continue
                }

                if ($parsed -eq 0) {
                    break multiLoop
                }

                if ($parsed -lt 1 -or $parsed -gt $Items.Count) {
                    Write-Host "  Out of range. Enter 1-$($Items.Count) or 0 to finish." -ForegroundColor Yellow
                    continue
                }

                $item = $Items[$parsed - 1]
                if ($selected -notcontains $item) {
                    $selected += $item
                    Write-Host "  Selected: $item" -ForegroundColor Green
                } else {
                    Write-Host "  Already selected: $item" -ForegroundColor DarkGray
                }
            }

            return ,[string[]]$selected
        } else {
            $parsed = 0
            :singleLoop while ($true) {
                $raw = Read-Host "Enter number"
                if (-not [int]::TryParse($raw, [ref]$parsed)) {
                    Write-Host "  Invalid input. Enter a number." -ForegroundColor Yellow
                    continue
                }

                if ($parsed -lt 1 -or $parsed -gt $Items.Count) {
                    Write-Host "  Out of range. Enter 1-$($Items.Count)." -ForegroundColor Yellow
                    continue
                }

                break singleLoop
            }

            return ,[string[]]@($Items[$parsed - 1])
        }
    }
}

# ---------------------------------------------------------------
# Confirm-AcaAction
# Yes/no prompt; returns [bool].
# ---------------------------------------------------------------
function Confirm-AcaAction {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message,

        [switch]$Default
    )

    if ($script:UseGum) {
        # gum backend
        if ($Default) {
            & gum confirm $Message
        } else {
            & gum confirm --default=no $Message
        }
        return ($LASTEXITCODE -eq 0)
    } else {
        # Native backend
        if ($Default) {
            $choice = Read-Host "$Message [Y/n]"
            return ($choice -eq '' -or $choice -eq 'y' -or $choice -eq 'Y')
        } else {
            $choice = Read-Host "$Message [y/N]"
            return ($choice -eq 'y' -or $choice -eq 'Y')
        }
    }
}

# ---------------------------------------------------------------
# Show-AcaSpin
# Runs a scriptblock with a status message; no return value.
# ---------------------------------------------------------------
function Show-AcaSpin {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message,

        [Parameter(Mandatory = $true)]
        [scriptblock]$Action
    )

    # Both backends use synchronous execution.
    # gum spin integration is kept simple to avoid Windows background-job complexity.
    Write-Host "$Message..." -NoNewline -ForegroundColor Yellow
    & $Action
    Write-Host " done." -ForegroundColor Green
}

# ---------------------------------------------------------------
# Read-AcaHighFriction
# Forces user to type exact phrase; returns [bool].
# NEVER trims or normalizes input - exact case-sensitive match only.
# ---------------------------------------------------------------
function Read-AcaHighFriction {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Phrase,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    # Flush any pending console input to prevent newline bleed
    # from gum or prior Read-Host calls.
    if ([Console]::KeyAvailable) {
        while ([Console]::KeyAvailable) {
            [void][Console]::ReadKey($true)
        }
    }

    if ($script:UseGum) {
        # gum backend
        Write-Host ""
        Write-Host "[WARNING] $Context" -ForegroundColor Red
        Write-Host "Type exactly: $Phrase" -ForegroundColor Yellow
        $typed = & gum input --placeholder "$Phrase"
    } else {
        # Native backend
        Write-Host ""
        Write-Host "[WARNING] $Context" -ForegroundColor Red
        Write-Host "To proceed, type exactly: $Phrase" -ForegroundColor Yellow
        Write-Host "(Any other input will cancel)" -ForegroundColor Gray
        $typed = Read-Host ">"
    }

    # Exact case-sensitive match only - no trimming, no normalization
    return ($typed -ceq $Phrase)
}
