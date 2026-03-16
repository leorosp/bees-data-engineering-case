param keyVaultName string
param location string
param tenantId string
param tags object = {}
param enablePurgeProtection bool = true

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    enablePurgeProtection: enablePurgeProtection
    enableRbacAuthorization: true
    enabledForDeployment: false
    enabledForDiskEncryption: false
    enabledForTemplateDeployment: false
    publicNetworkAccess: 'Enabled'
    sku: {
      family: 'A'
      name: 'standard'
    }
    softDeleteRetentionInDays: 90
    tenantId: tenantId
  }
}

output keyVaultId string = keyVault.id
output keyVaultName string = keyVault.name
output vaultUri string = keyVault.properties.vaultUri
