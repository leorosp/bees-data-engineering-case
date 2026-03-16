param workspaceName string
param location string
param skuName string = 'standard'
param managedResourceGroupName string
param enableNoPublicIp bool = false
param tags object = {}

resource workspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: workspaceName
  location: location
  sku: {
    name: skuName
  }
  tags: tags
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedResourceGroupName)
    parameters: {
      enableNoPublicIp: {
        value: enableNoPublicIp
      }
    }
    publicNetworkAccess: 'Enabled'
    requiredNsgRules: 'AllRules'
  }
}

output workspaceId string = workspace.id
output workspaceName string = workspace.name
output workspaceUrl string = workspace.properties.workspaceUrl
