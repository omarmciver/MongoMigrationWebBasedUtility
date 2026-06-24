# ACA Deployment — Mongo Migrator

## Overview

The `manage-aca.ps1` TUI provides selective add/remove/update/build operations on individual Container App instances **without disrupting running migrations**.

## Quick Start

### Prerequisites

- Azure CLI (`az`) — logged in and targeting the correct subscription
- Docker CLI (Docker Desktop) — running
- `ACA/aca.config.local.ps1` — copy from `ACA/aca.config.local.example.ps1` and fill in real values

### Optional Tools

- `gum` — enhances the TUI with styled menus. Install: https://github.com/charmbracelet/gum#installation
- `mongosh` — enables busy detection via StateStore query. Install: https://www.mongodb.com/docs/mongodb-shell/install/

### Running the TUI

```powershell
# From the repo root:
.\ACA\manage-aca.ps1
```

## Config Split

| File | Committed | Purpose |
|---|---|---|
| `ACA/aca.config.local.example.ps1` | Yes | Placeholder template — commit this |
| `ACA/aca.config.local.ps1` | No (gitignored) | Your real values — never commit |

To set up: `Copy-Item ACA/aca.config.local.example.ps1 ACA/aca.config.local.ps1` then edit `aca.config.local.ps1` with your JFrog password and StateStore connection string.

## Safety Guarantees

### ChangeStreamTailing Protection

Instances in **ChangeStreamTailing** phase (active online migration) are **sacred**. The TUI will:

- Skip them by default in any mutating action (Add / Update / Remove)
- Require typing the literal phrase `INTERRUPT-TAILING` to override

**No revision is ever cycled on an existing instance without your explicit per-instance confirmation.**

### Gap-fill Naming

When you remove instance `-2` from `[1,2,3]`, the next Add reuses `-2` (not `-4`). Each instance has a dedicated file share (`migration-data-N`) in the shared storage account.

## Module Layout

```
ACA/
  manage-aca.ps1                   # Entry point — run this
  aca.config.local.ps1             # Your real config (gitignored)
  aca.config.local.example.ps1     # Template (committed)
  tui/
    bootstrap.ps1     # Dependency checks, audit log, lock file, Azure context
    ui.ps1            # gum / native prompt abstraction
    state.ps1         # Instance status aggregation + busy detection
    jfrog.ps1         # Docker build + tag lifecycle
    instances.ps1     # Gap-fill naming, Add/Remove per-instance
    flows.ps1         # High-level flows: Add, Remove, Update, Build, Rollback
  deploy-to-aca-jfrog.ps1          # Low-level per-instance ARM deploy (called by TUI)
  aca_main_jfrog.bicep             # ACA infrastructure template
```

## Audit Log

Every mutating action (Add / Remove / Update / Build / Rollback) is appended to `ACA/.aca-audit.log` (gitignored). Format: `UTC | user@host | Action | Target | Result`.

## Previous Entry Point (Deprecated)

`ACA/private.watchtower.ps1` was the previous orchestrator. It redeploys ALL instances on every run and is now superseded by `manage-aca.ps1`. See the deprecation notice in that file.
