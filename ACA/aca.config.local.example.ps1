# ============================================================
# ACA TUI Config — Example with Placeholder Tokens
# ============================================================
# Copy this file to aca.config.local.ps1 and fill in real values.
# This example file is committed to git and contains NO secrets.
#
# Consumed by: ACA/manage-aca.ps1 and ACA/tui/*.ps1
# ============================================================

$AcaConfig = @{
    # Azure
    ResourceGroupName              = "31298-eastus2-nprd-document-db-rg"
    ContainerAppNamePrefix         = "mongo-migrator"
    EnvironmentName                = "mongo-migrator-env-D8"
    Location                       = "eastus2"
    OwnerTag                       = "om098h@att.com"
    SharedStorageAccountResourceId = "/subscriptions/<SUBSCRIPTION_ID>/resourceGroups/<RESOURCE_GROUP>/providers/Microsoft.Storage/storageAccounts/<STORAGE_ACCOUNT_NAME>"
    InfrastructureSubnetResourceId = "/subscriptions/<SUBSCRIPTION_ID>/resourceGroups/<VNET_RG>/providers/Microsoft.Network/virtualNetworks/<VNET_NAME>/subnets/<SUBNET_NAME>"

    # Container resources
    VCores                         = 8
    MemoryGB                       = 32

    # JFrog registry
    JFrogRegistryServer            = "artifact.it.att.com"
    JFrogRegistryServerForACA      = "artifact.it.att.com:22609"
    JFrogUsername                  = "om098h@att.com"
    JFrogPassword                  = "<JFROG_API_KEY>"
    JFrogRepository                = "apm0013439-dkr-stage"
    JFrogDebianRepo                = "apm0013439-deb-group"
    JFrogDebianComponent           = "main"
    JFrogNuGetRepo                 = "apm0013439-ngt-group"

    # StateStore
    StateStoreConnectionString     = "<STATESTORE_CONNECTION_STRING>"
}
