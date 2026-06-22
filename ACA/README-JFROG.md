# Azure Container Apps Deployment with JFrog Artifactory

This guide covers deploying the MongoDB Migration Web-Based Utility to Azure Container Apps (ACA) using JFrog Artifactory as the container registry. It includes single-instance and multi-instance deployments.

## Overview

The JFrog deployment differs from the standard ACR deployment in the following ways:

| Aspect | ACR Deployment | JFrog Deployment |
|--------|----------------|------------------|
| Base Images | Pulled from `mcr.microsoft.com` | Pulled from JFrog (mirrored/proxied) |
| Build Location | Azure ACR Tasks (cloud) | Local Docker (your machine) |
| Image Storage | Azure Container Registry | JFrog Artifactory |
| Authentication | Managed Identity | Username/Password or API Key |

## Prerequisites

### 1. JFrog Artifactory Setup

Ensure you have JFrog Artifactory configured with:

1. **Docker Repository** — one of:
   - A **local repository** (e.g., `my-dkr-stage`) for pushing your built images
   - A **remote repository** that proxies `mcr.microsoft.com` for .NET base images
   - A **virtual repository** that combines local and remote

2. **.NET 9.0 Base Images Available** via one of:
   - Remote repository proxying Microsoft Container Registry (MCR)
   - Manual upload of `dotnet/sdk:9.0` and `dotnet/aspnet:9.0`
   - Virtual repository that resolves from a remote proxy

3. **A Debian/APT repository** for `apt-get` packages used in the Docker build.

4. **A NuGet repository** for .NET package restore during the Docker build.

5. **Authentication Credentials**:
   - JFrog username
   - JFrog API key (preferred over password for automation)

### 2. Local Requirements

- **Docker Desktop** or Docker CLI installed and running
- **Azure CLI** installed and logged in (`az login`)
- Network access to your JFrog Artifactory instance

## Files

| File | Description |
|------|-------------|
| `Dockerfile.jfrog` | Dockerfile with parameterized JFrog base images |
| `aca_main_jfrog.bicep` | Bicep template for ACA with JFrog registry config |
| `deploy-to-aca-jfrog.ps1` | Deployment script — full infra + app, or image-update only |

## JFrog Repository Structure

Recommended JFrog structure:

```
yourcompany.jfrog.io/
├── my-dkr-stage/           # Local Docker repo — push your built images here
├── my-dkr-group/           # Virtual Docker repo — resolves base images (proxied from MCR)
│   └── dotnet/
│       ├── sdk:9.0
│       └── aspnet:9.0
├── my-deb-group/           # Debian/APT virtual repo (for apt-get during build)
└── my-ngt-group/           # NuGet virtual repo (for dotnet restore during build)
```

---

## Single-Instance Deployment

### Full Deployment (First Time)

Deploys all infrastructure (Storage Account, Managed Identity, Container Apps Environment) and the Container App itself.

```powershell
.\deploy-to-aca-jfrog.ps1 `
    -ResourceGroupName        "my-rg" `
    -ContainerAppName         "mongo-migrator" `
    -JFrogRegistryServer      "yourcompany.jfrog.io" `
    -JFrogRegistryServerForACA "yourcompany.jfrog.io:22609" `
    -JFrogUsername            "your-username" `
    -JFrogPassword            "your-api-key" `
    -JFrogRepository          "my-dkr-stage" `
    -JFrogDebianRepo          "my-deb-group" `
    -JFrogDebianDistribution  "bookworm" `
    -JFrogDebianComponent     "main" `
    -JFrogNuGetRepo           "my-ngt-group" `
    -Location                 "eastus2" `
    -OwnerTag                 "your-email@company.com" `
    -StateStoreConnectionString "mongodb+srv://..." `
    -VCores                   8 `
    -MemoryGB                 32
```

> **Note on `JFrogRegistryServerForACA`**: In environments using an NVA or Bastion with Binary Port Mapping, ACA may need to reach JFrog on a different port (e.g., `22609` mapped to `:443`). If no port remapping is in use, omit this parameter and it defaults to `JFrogRegistryServer`.

### Update Image Only (Subsequent Deployments)

Use `-UpdateOnly` to push a new image and create a new revision without redeploying infrastructure. This is the normal path for code updates.

```powershell
.\deploy-to-aca-jfrog.ps1 `
    -UpdateOnly `
    -ResourceGroupName   "my-rg" `
    -ContainerAppName    "mongo-migrator" `
    -JFrogRegistryServer "yourcompany.jfrog.io" `
    -JFrogUsername       "your-username" `
    -JFrogPassword       "your-api-key" `
    -JFrogRepository     "my-dkr-stage"
```

Add `-SkipDockerBuild` if the image was already pushed and you only want to update the revision pointer.

---

## Multi-Instance Deployment

You can deploy **multiple independent Container Apps** that all run the same image, sharing a single ACA Environment. Each instance:

- Gets its own **subdomain** under the environment's wildcard domain (`*.salmonwave-84147fd0.eastus2.azurecontainerapps.io`) — no additional private endpoints required.
- Gets its own **dedicated CPU/memory node** — resources are not shared or contested between instances.
- Gets its own **Storage Account** and Azure Files mount for migration data.
- Gets its own **isolated namespace in the StateStore DocumentDB** — the `ContainerAppName` is used as the `StateStoreAppID`, which prefixes all persisted documents (e.g., `mongo-migrator-1.migrationjobs_...`). The same connection string can be shared across all instances.

### How to deploy multiple instances

Call `deploy-to-aca-jfrog.ps1` once per instance, passing:
- A unique `ContainerAppName` per instance (e.g., `mongo-migrator-1`, `mongo-migrator-2`)
- A **shared** `EnvironmentName` so all instances land in the same ACA Environment
- `WorkloadProfileMaxCount` set to the **total number of instances** so the dedicated node pool is sized correctly

```powershell
$BaseAppName    = "mongo-migrator"
$InstanceCount  = 3
$EnvironmentName = "$BaseAppName-env"

for ($i = 1; $i -le $InstanceCount; $i++) {
    .\deploy-to-aca-jfrog.ps1 `
        -ResourceGroupName        "my-rg" `
        -ContainerAppName         "$BaseAppName-$i" `
        -EnvironmentName          $EnvironmentName `
        -WorkloadProfileMaxCount  $InstanceCount `
        -JFrogRegistryServer      "yourcompany.jfrog.io" `
        -JFrogRegistryServerForACA "yourcompany.jfrog.io:22609" `
        -JFrogUsername            "your-username" `
        -JFrogPassword            "your-api-key" `
        -JFrogRepository          "my-dkr-stage" `
        -JFrogDebianRepo          "my-deb-group" `
        -JFrogDebianDistribution  "bookworm" `
        -JFrogDebianComponent     "main" `
        -JFrogNuGetRepo           "my-ngt-group" `
        -Location                 "eastus2" `
        -OwnerTag                 "your-email@company.com" `
        -StateStoreConnectionString "mongodb+srv://..." `
        -VCores                   16 `
        -MemoryGB                 64
}
```

This produces:
| Instance | URL | StateStore namespace |
|----------|-----|----------------------|
| `mongo-migrator-1` | `https://mongo-migrator-1.<env-domain>` | `mongo-migrator-1.*` |
| `mongo-migrator-2` | `https://mongo-migrator-2.<env-domain>` | `mongo-migrator-2.*` |
| `mongo-migrator-3` | `https://mongo-migrator-3.<env-domain>` | `mongo-migrator-3.*` |

To update all instances to a new image, run the same loop with `-UpdateOnly` (and optionally `-SkipDockerBuild` after the first iteration if the image tag is shared).

---

## All Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `ResourceGroupName` | Yes | — | Azure resource group |
| `ContainerAppName` | Yes | — | Container App name (also used as `StateStoreAppID`) |
| `JFrogRegistryServer` | Yes | — | JFrog hostname used for local Docker build/push |
| `JFrogUsername` | Yes | — | JFrog username |
| `JFrogRepository` | Yes | — | JFrog Docker repository for the app image |
| `JFrogPassword` | No | *(prompted)* | JFrog password or API key |
| `JFrogRegistryServerForACA` | No | `JFrogRegistryServer` | JFrog hostname used by ACA for image pulls (useful when port differs from build server) |
| `JFrogBaseImageRegistry` | No | `JFrogRegistryServer` | Registry for .NET base images |
| `JFrogBaseImageRepository` | No | derived from `JFrogRepository` | Repository for .NET base images |
| `JFrogDebianRepo` | No* | — | JFrog Debian/APT repo (required for full deployment) |
| `JFrogDebianDistribution` | No | `bookworm` | Debian distribution |
| `JFrogDebianComponent` | No* | — | Debian component (required for full deployment) |
| `JFrogNuGetRepo` | No* | — | JFrog NuGet repo (required for full deployment) |
| `Location` | No* | — | Azure region (required for full deployment) |
| `OwnerTag` | No* | — | Resource owner tag (required for full deployment) |
| `StateStoreConnectionString` | No | *(prompted)* | MongoDB connection string for job state persistence |
| `StateStoreAppID` | No | `ContainerAppName` | Namespace prefix for StateStore documents |
| `StorageAccountName` | No | derived | Storage account name |
| `ImageName` | No | `mongo-migration` | Image name within the repository |
| `ImageTag` | No | timestamp | Docker image tag; auto-generated timestamp ensures unique revisions |
| `VCores` | No | `8` | vCPU count per container (1–32) |
| `MemoryGB` | No | `32` | Memory per container in GB (2–64) |
| `EnvironmentName` | No | derived | Shared ACA Environment name for multi-instance deployments |
| `WorkloadProfileMaxCount` | No | `1` | Max dedicated nodes in the workload profile; set to instance count for multi-instance |
| `InfrastructureSubnetResourceId` | No | — | VNet subnet resource ID for private network integration |
| `UseEntraIdForAzureStorage` | No | `false` | Use Managed Identity (Blob SDK) instead of Azure Files mount |
| `SkipDockerBuild` | No | `false` | Skip Docker build; assume image already exists in JFrog |
| `UpdateOnly` | No | `false` | Skip infrastructure deployment; only push image and create new revision |

---

## Authentication Flow

1. **During deployment**: The script logs in to JFrog with your credentials to build and push the image, then logs out.
2. **Stored in Azure**: The JFrog password is stored as a Container App secret (`jfrog-password`) so ACA can pull the image at runtime.
3. **At runtime**: ACA uses the stored secret to authenticate with JFrog when pulling the image on startup or after a revision update.

---

## Troubleshooting

### Docker Build Fails — Base Image Not Found

**Error**: `failed to solve: yourcompany.jfrog.io/dotnet/sdk:9.0: failed to resolve source metadata`

**Cause**: .NET base images are not available in JFrog.

**Fix**:
1. Ensure your JFrog remote repository is configured to proxy `mcr.microsoft.com`.
2. Or upload base images manually to a JFrog local repository.
3. Verify: `docker pull yourcompany.jfrog.io/my-dkr-group/dotnet/sdk:9.0`

### Authentication Fails

**Error**: `unauthorized: authentication required`

**Fix**:
1. Verify username and API key.
2. Check JFrog user has read/write permissions on the target repository.
3. Test manually: `docker login yourcompany.jfrog.io -u your-username`

### Container App Fails to Start — Image Pull Error

**Fix**:
1. Confirm the full image path is correct in the Azure Portal under the Container App revision.
2. Check the `jfrog-password` secret in the Container App is not expired.
3. If on a private VNet, ensure the NVA/firewall allows outbound traffic to the JFrog host on the configured port.

### Multi-Instance: Environment Already Exists

When deploying additional instances into an existing shared environment, Bicep will detect the environment resource as already deployed and skip re-creating it — this is expected and safe. Infrastructure for the new instance (Storage Account, Managed Identity) will be created fresh.

---

## Security Considerations

1. **API Keys over passwords**: Use JFrog API keys scoped to the minimum required repositories.
2. **Minimal permissions**: The JFrog user needs push access for the image repository and pull access for base image and package repositories only.
3. **Credential rotation**: Rotate JFrog API keys periodically and redeploy to update the ACA secret.
4. **Network restrictions**: If JFrog uses IP allowlists, add the ACA environment's outbound IPs.

---

## Comparison: ACR vs JFrog

| Feature | ACR | JFrog |
|---------|-----|-------|
| **Build Location** | Cloud (ACR Tasks) | Local (Docker) |
| **Cost** | ACR pricing | JFrog licensing |
| **Integration** | Native Azure | Cross-platform |
| **Authentication** | Managed Identity | Username/Password or API Key |
| **Artifact Types** | Containers only | Universal (npm, maven, NuGet, apt, etc.) |
| **Corporate proxy** | Not needed | Required for VNet-restricted environments |
