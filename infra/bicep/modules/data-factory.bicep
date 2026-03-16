param dataFactoryName string
param location string
param lakehouseBasePath string
param tags object = {}

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  tags: tags
  properties: {
    globalParameters: {
      lakehouseBasePath: {
        type: 'String'
        value: lakehouseBasePath
      }
      lakehouseFileSystem: {
        type: 'String'
        value: split(split(lakehouseBasePath, '://')[1], '@')[0]
      }
    }
    publicNetworkAccess: 'Enabled'
  }
}

output dataFactoryId string = dataFactory.id
output dataFactoryName string = dataFactory.name
output principalId string = dataFactory.identity.principalId
