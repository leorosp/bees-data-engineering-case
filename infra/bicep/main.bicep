targetScope = 'resourceGroup'

@description('Project name prefix')
param projectName string = 'bees-case'

@description('Environment name')
param environment string = 'dev'

@description('Azure region')
param location string = resourceGroup().location

@description('Tenant ID used by Azure Key Vault.')
param tenantId string = tenant().tenantId

@description('Optional alert email for the monitoring action group.')
param notificationEmail string = ''

@description('Retention period in days for Log Analytics data.')
@minValue(30)
@maxValue(730)
param logAnalyticsRetentionInDays int = 30

@description('Azure Databricks pricing tier.')
@allowed([
  'standard'
  'premium'
  'trial'
])
param databricksSkuName string = 'standard'

@description('If true, Azure Databricks will be provisioned with no public IP for clusters.')
param databricksEnableNoPublicIp bool = false

var nameSuffix = substring(uniqueString(resourceGroup().id, projectName, environment), 0, 6)
var resourcePrefix = '${projectName}-${environment}'
var storageAccountName = 'st${uniqueString(resourceGroup().id, projectName, environment)}'
var keyVaultName = 'kv-${projectName}-${environment}-${nameSuffix}'
var dataFactoryName = '${resourcePrefix}-adf'
var dataBricksWorkspaceName = '${resourcePrefix}-dbw'
var databricksManagedResourceGroupName = '${resourcePrefix}-dbw-mrg'
var logAnalyticsName = '${resourcePrefix}-law'
var monitorActionGroupName = '${resourcePrefix}-ag'
var monitorActionGroupShortName = substring(toUpper(replace('${projectName}${environment}', '-', '')), 0, 12)
var tags = {
  application: projectName
  environment: environment
  managedBy: 'bicep'
  workload: 'bees-data-engineering-case'
}

module storage './modules/storage.bicep' = {
  name: 'storageDeployment'
  params: {
    storageAccountName: storageAccountName
    location: location
    tags: tags
    containers: [
      'bronze'
      'silver'
      'gold'
      'ops'
    ]
  }
}

module keyVault './modules/key-vault.bicep' = {
  name: 'keyVaultDeployment'
  params: {
    keyVaultName: keyVaultName
    location: location
    tenantId: tenantId
    tags: tags
  }
}

module logAnalytics './modules/log-analytics.bicep' = {
  name: 'logAnalyticsDeployment'
  params: {
    workspaceName: logAnalyticsName
    location: location
    retentionInDays: logAnalyticsRetentionInDays
    tags: tags
  }
}

module actionGroup './modules/action-group.bicep' = {
  name: 'actionGroupDeployment'
  params: {
    actionGroupName: monitorActionGroupName
    groupShortName: monitorActionGroupShortName
    notificationEmail: notificationEmail
    tags: tags
  }
}

module dataFactory './modules/data-factory.bicep' = {
  name: 'dataFactoryDeployment'
  params: {
    dataFactoryName: dataFactoryName
    location: location
    lakehouseBasePath: storage.outputs.dfsEndpoint
    tags: tags
  }
}

module databricks './modules/databricks.bicep' = {
  name: 'databricksDeployment'
  params: {
    workspaceName: dataBricksWorkspaceName
    location: location
    skuName: databricksSkuName
    managedResourceGroupName: databricksManagedResourceGroupName
    enableNoPublicIp: databricksEnableNoPublicIp
    tags: tags
  }
}

output plannedServices array = [
  'Azure Data Lake Storage Gen2'
  'Azure Data Factory'
  'Azure Databricks'
  'Azure Key Vault'
  'Log Analytics Workspace'
  'Azure Monitor Action Group'
]

output naming object = {
  storageAccountName: storageAccountName
  keyVaultName: keyVaultName
  dataFactoryName: dataFactoryName
  dataBricksWorkspaceName: dataBricksWorkspaceName
  databricksManagedResourceGroupName: databricksManagedResourceGroupName
  logAnalyticsName: logAnalyticsName
  monitorActionGroupName: monitorActionGroupName
  location: location
}

output resourceIds object = {
  storageAccountId: storage.outputs.storageAccountId
  keyVaultId: keyVault.outputs.keyVaultId
  logAnalyticsWorkspaceId: logAnalytics.outputs.workspaceId
  actionGroupId: actionGroup.outputs.actionGroupId
  dataFactoryId: dataFactory.outputs.dataFactoryId
  databricksWorkspaceId: databricks.outputs.workspaceId
}

output observability object = {
  logAnalyticsWorkspaceName: logAnalytics.outputs.workspaceName
  actionGroupName: actionGroup.outputs.actionGroupName
}
