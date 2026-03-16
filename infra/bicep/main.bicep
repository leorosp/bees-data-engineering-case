targetScope = 'resourceGroup'

@description('Project name prefix')
param projectName string = 'bees-case'

@description('Environment name')
param environment string = 'dev'

@description('Azure region')
param location string = resourceGroup().location

var storageAccountName = toLower(replace('${projectName}${environment}adls', '-', ''))
var keyVaultName = '${projectName}-${environment}-kv'
var dataFactoryName = '${projectName}-${environment}-adf'
var logAnalyticsName = '${projectName}-${environment}-law'
var monitorActionGroupName = '${projectName}-${environment}-ag'

// TODO:
// - Add ADLS Gen2 storage account
// - Add Azure Data Factory
// - Add Azure Key Vault
// - Add Log Analytics Workspace
// - Add Action Group
// - Add Databricks workspace module

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
  logAnalyticsName: logAnalyticsName
  monitorActionGroupName: monitorActionGroupName
  location: location
}
