@description('Location for all resources')
param location string = resourceGroup().location

@description('Owner tag required by Azure Policy')
param ownerTag string

@description('Name of the Container App')
param containerAppName string

@description('JFrog Registry server URL (e.g., yourcompany.jfrog.io)')
param jfrogRegistryServer string

@description('JFrog Registry username')
param jfrogUsername string

@secure()
@description('JFrog Registry password or API key')
param jfrogPassword string

@description('Full image path in JFrog (e.g., yourcompany.jfrog.io/docker-local/mongomigration)')
param jfrogImageRepository string

@description('Docker image tag to deploy')
param imageTag string = 'latest'

@description('Storage account name for persistent migration files. Ignored when storageAccountResourceId is provided.')
param storageAccountName string = take('${replace(containerAppName, '-', '')}stor', 24)

@description('Optional: Resource ID of an existing storage account to use instead of creating one. When provided, the storage account is referenced (not created) and must already have proper networking (e.g., private endpoint linked to the ACA environment\'s VNet). Must be in the same resource group as this deployment when using Azure Files mount mode. Default: empty (creates new account).')
param storageAccountResourceId string = ''

@description('Name of the file share to use for migration data. When sharing a storage account across multiple container apps, use unique share names per app (e.g., migration-data-1, migration-data-2). Default: migration-data.')
param fileShareName string = 'migration-data'

@secure()
@description('StateStore connection string for the container')
param stateStoreConnectionString string = ''

@description('StateStore App ID for the container')
param stateStoreAppID string = ''

@description('Number of vCores for the container')
@minValue(1)
@maxValue(32)
param vCores int = 8

@description('Memory in GB for the container')
@minValue(2)
@maxValue(64)
param memoryGB int = 32

@description('ASP.NET Core environment setting')
param aspNetCoreEnvironment string = 'Development'

@description('Optional: Resource ID of the subnet for VNet integration (e.g., /subscriptions/{sub-id}/resourceGroups/{rg-name}/providers/Microsoft.Network/virtualNetworks/{vnet-name}/subnets/{subnet-name})')
param infrastructureSubnetResourceId string = ''

@description('Use Entra ID (Managed Identity) for Azure Blob Storage instead of mounting Azure Files. When true, UseBlobServiceClient env var is set and no volume is mounted.')
param useEntraIdForStorage bool = false

@description('Name of the Container Apps Environment. When deploying multiple container apps to the same environment, provide a shared name so all instances reuse the same environment. Defaults to {containerAppName}-env-{workloadProfileType}.')
param environmentName string = ''

@description('Reuse an existing Container Apps Environment instead of creating one. Set this when environmentName points to an already deployed environment.')
param reuseExistingEnvironment bool = false

@description('Maximum number of dedicated workload profile nodes in the environment. Increase to match the number of container app instances sharing this environment.')
@minValue(1)
@maxValue(100)
param workloadProfileMaxCount int = 1

// Variables for dynamic workload profile selection
var workloadProfileType = vCores <= 4 ? 'D4' : vCores <= 8 ? 'D8' : vCores <= 16 ? 'D16' : 'D32'
var workloadProfileName = 'Dedicated'

// Effective environment name: use provided shared name or derive from container app name
var effectiveEnvironmentName = !empty(environmentName) ? environmentName : '${containerAppName}-env-${workloadProfileType}'

// Pre-configured storage account handling: when storageAccountResourceId is provided,
// reference the existing account instead of creating a new one. This lets multiple container
// apps share a single storage account (with existing private endpoint) and use per-instance
// file shares for data isolation.
var usePreConfiguredStorageAccount = !empty(storageAccountResourceId)
var effectiveStorageAccountName = usePreConfiguredStorageAccount ? last(split(storageAccountResourceId, '/')) : storageAccountName
var storageAccountSubscriptionId = usePreConfiguredStorageAccount ? split(storageAccountResourceId, '/')[2] : subscription().subscriptionId
var storageAccountResourceGroupName = usePreConfiguredStorageAccount ? split(storageAccountResourceId, '/')[4] : resourceGroup().name

// Storage configuration name on the managed environment.
// IMPORTANT: ACA disallows changing accountName/shareName on an existing storage definition
// (only the account key can be updated). When using a pre-configured storage account, derive
// the storage config name from the file share name so each unique share gets its own fresh
// storage definition. Any old/broken storage definitions tied to the prior name remain orphaned
// in the environment (safe to delete manually after redeploy).
// Single-storage-account mode falls back to the legacy container-app-derived name for
// backward compatibility.
var storageConfigurationName = usePreConfiguredStorageAccount
  ? take(replace(fileShareName, '-', ''), 24)
  : take(replace(containerAppName, '-', ''), 24)

// Resolve the storage account key via listKeys() function form so it works whether the account
// is created here or already exists. ARM control-plane listKeys works regardless of storage
// account network policy (private endpoint, firewall rules, etc.).
var storageAccountKey = listKeys(
  resourceId(storageAccountSubscriptionId, storageAccountResourceGroupName, 'Microsoft.Storage/storageAccounts', effectiveStorageAccountName),
  '2023-01-01'
).keys[0].value

// Managed Identity for Container App (used for Azure Storage access, not registry)
resource managedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${containerAppName}-identity'
  location: location
  tags: {
    owner: ownerTag
  }
}

// Storage Account for persistent migration files
// Only created when storageAccountResourceId is NOT provided
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = if (!usePreConfiguredStorageAccount) {
  name: effectiveStorageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    // Disable shared key access when using Entra ID - required by some org policies
    allowSharedKeyAccess: !useEntraIdForStorage
  }
  tags: {
    owner: ownerTag
  }
}

// File Share for migration data - only needed when NOT using Entra ID
// Idempotent: creates the share if missing on either a new or pre-configured account
// (works in same RG; cross-RG storage requires deploying file share separately)
resource fileShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-01-01' = if (!useEntraIdForStorage) {
  name: '${effectiveStorageAccountName}/default/${fileShareName}'
  properties: {
    shareQuota: 100
    enabledProtocols: 'SMB'
  }
  dependsOn: usePreConfiguredStorageAccount ? [] : [
    storageAccount
  ]
}

// Container Apps Environment with Dedicated Plan
resource containerAppEnvironment 'Microsoft.App/managedEnvironments@2023-05-01' = if (!reuseExistingEnvironment) {
  name: effectiveEnvironmentName
  location: location
  tags: {
    owner: ownerTag
  }
  properties: union(
    {
      workloadProfiles: [
        {
          name: 'Consumption'
          workloadProfileType: 'Consumption'
        }
        {
          name: workloadProfileName
          workloadProfileType: workloadProfileType
          minimumCount: 0
          maximumCount: workloadProfileMaxCount
        }
      ]
    },
    infrastructureSubnetResourceId != '' ? {
      vnetConfiguration: {
        infrastructureSubnetId: infrastructureSubnetResourceId
        internal: true  // Private endpoint - no public access (required by corporate policy)
      }
    } : {}
  )
}

// Existing environment reference used when reusing a pre-created environment
resource existingContainerAppEnvironment 'Microsoft.App/managedEnvironments@2023-05-01' existing = if (reuseExistingEnvironment) {
  name: effectiveEnvironmentName
}

// Storage configuration for Container Apps Environment (only when not using Entra ID)
// Name is derived from containerAppName to be unique per instance when sharing an environment.
// Uses effectiveStorageAccountName + listKeys() function form so it works for both
// newly created and pre-configured storage accounts.
resource storageConfiguration 'Microsoft.App/managedEnvironments/storages@2023-05-01' = if (!useEntraIdForStorage && !reuseExistingEnvironment) {
  parent: containerAppEnvironment
  name: storageConfigurationName
  properties: {
    azureFile: {
      accountName: effectiveStorageAccountName
      accountKey: storageAccountKey
      shareName: fileShareName
      accessMode: 'ReadWrite'
    }
  }
  dependsOn: [
    fileShare
  ]
}

resource storageConfigurationExisting 'Microsoft.App/managedEnvironments/storages@2023-05-01' = if (!useEntraIdForStorage && reuseExistingEnvironment) {
  parent: existingContainerAppEnvironment
  name: storageConfigurationName
  properties: {
    azureFile: {
      accountName: effectiveStorageAccountName
      accountKey: storageAccountKey
      shareName: fileShareName
      accessMode: 'ReadWrite'
    }
  }
  dependsOn: [
    fileShare
  ]
}

// Role assignment for Managed Identity to access Blob Storage (only when using Entra ID)
// Only deployed for newly created storage accounts. For pre-configured storage accounts,
// the role assignment must be created separately (cross-RG role assignment requires a module).
resource storageBlobDataContributorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (useEntraIdForStorage && !usePreConfiguredStorageAccount) {
  name: guid(storageAccount.id, managedIdentity.id, 'storageBlobDataContributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') // Storage Blob Data Contributor
    principalId: managedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// Container App with JFrog Registry
resource containerApp 'Microsoft.App/containerApps@2023-05-01' = {
  name: containerAppName
  location: location
  tags: {
    owner: ownerTag
  }
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${managedIdentity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: reuseExistingEnvironment ? existingContainerAppEnvironment.id : containerAppEnvironment.id
    workloadProfileName: workloadProfileName
    configuration: {
      secrets: concat(
        [
          {
            name: 'jfrog-password'
            value: jfrogPassword
          }
        ],
        stateStoreConnectionString != '' ? [
          {
            name: 'statestore-connection'
            value: stateStoreConnectionString
          }
        ] : []
      )
      // JFrog Registry configuration with username/password authentication
      registries: [
        {
          server: jfrogRegistryServer
          username: jfrogUsername
          passwordSecretRef: 'jfrog-password'
        }
      ]
      ingress: {
        external: true
        targetPort: 8080
        allowInsecure: false
        traffic: [
          {
            weight: 100
            latestRevision: true
          }
        ]
      }
    }
    template: {
      containers: [
        {
          name: containerAppName
          image: stateStoreAppID == '' ? 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest' : '${jfrogImageRepository}:${imageTag}'
          resources: {
            cpu: vCores
            memory: '${memoryGB}Gi'
          }
          volumeMounts: useEntraIdForStorage ? [] : [
            {
              volumeName: 'migration-data-volume'
              mountPath: '/app/migration-data'
            }
          ]
          env: concat([
            {
              name: 'ASPNETCORE_ENVIRONMENT'
              value: aspNetCoreEnvironment
            }
            {
              name: 'ASPNETCORE_HTTP_PORTS'
              value: '8080'
            }
            {
              name: 'StateStoreAppID'
              value: stateStoreAppID
            }
            {
              name: 'ResourceDrive'
              value: '/app/migration-data'
            }
          ], stateStoreConnectionString != '' ? [
            {
              name: 'StateStoreConnectionStringOrPath'
              secretRef: 'statestore-connection'
            }
          ] : [], useEntraIdForStorage ? [
            {
              name: 'UseBlobServiceClient'
              value: 'true'
            }
            {
              name: 'BlobServiceClientURI'
              value: 'https://${effectiveStorageAccountName}.blob.${environment().suffixes.storage}'
            }
            {
              name: 'BlobContainerName'
              value: 'migration-data'
            }
            {
              name: 'AZURE_CLIENT_ID'
              value: managedIdentity.properties.clientId
            }
          ] : [])
          probes: [
            {
              type: 'Startup'
              httpGet: {
                path: '/api/HealthCheck/ping'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: 10
              periodSeconds: 5
              failureThreshold: 30
              successThreshold: 1
              timeoutSeconds: 3
            }
            {
              type: 'Liveness'
              httpGet: {
                path: '/api/HealthCheck/ping'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: 0
              periodSeconds: 30
              failureThreshold: 3
              successThreshold: 1
              timeoutSeconds: 5
            }
            {
              type: 'Readiness'
              httpGet: {
                path: '/api/HealthCheck/ping'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: 5
              periodSeconds: 10
              failureThreshold: 3
              successThreshold: 1
              timeoutSeconds: 3
            }
          ]
        }
      ]
      volumes: useEntraIdForStorage ? [] : [
        {
          name: 'migration-data-volume'
          storageType: 'AzureFile'
          storageName: storageConfigurationName
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
  dependsOn: useEntraIdForStorage ? [
    storageBlobDataContributorRole
  ] : [
    reuseExistingEnvironment ? storageConfigurationExisting : storageConfiguration
  ]
}

// Outputs
@description('Container Apps Environment ID')
output containerAppEnvironmentId string = containerAppEnvironment.id

@description('Container App FQDN')
output containerAppFQDN string = stateStoreAppID != '' ? containerApp.properties.configuration.ingress.fqdn : 'not-ready'

@description('Container App URL')
output containerAppUrl string = stateStoreAppID == '' ? 'not-ready' : 'https://${containerApp.properties.configuration.ingress.fqdn}'

@description('Managed Identity Resource ID')
output managedIdentityId string = managedIdentity.id

@description('Managed Identity Client ID')
output managedIdentityClientId string = managedIdentity.properties.clientId

@description('Storage Account Name for migration data')
output storageAccountName string = effectiveStorageAccountName

@description('File Share Name for migration data (only when not using Entra ID)')
output fileShareName string = fileShareName

@description('Resource Drive Mount Path in container')
output resourceDrivePath string = '/app/migration-data'

@description('Storage mode: MountedAzureFiles or EntraIdBlobStorage')
output storageMode string = useEntraIdForStorage ? 'EntraIdBlobStorage' : 'MountedAzureFiles'

@description('Blob Service URI (only when using Entra ID)')
output blobServiceUri string = useEntraIdForStorage ? 'https://${effectiveStorageAccountName}.blob.${environment().suffixes.storage}' : ''

@description('JFrog Registry Server')
output jfrogRegistry string = jfrogRegistryServer

@description('Container Image (full path with tag)')
output containerImage string = stateStoreAppID == '' ? 'placeholder' : '${jfrogImageRepository}:${imageTag}'
