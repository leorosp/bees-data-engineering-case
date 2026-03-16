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
      bronzeContainer: {
        type: 'String'
        value: 'bronze'
      }
      goldContainer: {
        type: 'String'
        value: 'gold'
      }
      lakehouseBasePath: {
        type: 'String'
        value: lakehouseBasePath
      }
      opsContainer: {
        type: 'String'
        value: 'ops'
      }
      silverContainer: {
        type: 'String'
        value: 'silver'
      }
    }
    publicNetworkAccess: 'Enabled'
  }
}

output dataFactoryId string = dataFactory.id
output dataFactoryName string = dataFactory.name
output principalId string = dataFactory.identity.principalId
