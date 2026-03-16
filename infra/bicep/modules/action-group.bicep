param actionGroupName string
param groupShortName string
param notificationEmail string = ''
param tags object = {}

resource actionGroup 'Microsoft.Insights/actionGroups@2023-01-01' = {
  name: actionGroupName
  location: 'global'
  tags: tags
  properties: {
    enabled: true
    groupShortName: groupShortName
    emailReceivers: empty(notificationEmail) ? [] : [
      {
        name: 'primary-email'
        emailAddress: notificationEmail
        useCommonAlertSchema: true
      }
    ]
  }
}

output actionGroupId string = actionGroup.id
output actionGroupName string = actionGroup.name
