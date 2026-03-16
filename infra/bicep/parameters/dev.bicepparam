using '../main.bicep'

param projectName = 'bees-case'
param environment = 'dev'
param notificationEmail = ''
param logAnalyticsRetentionInDays = 30
param databricksSkuName = 'standard'
param databricksEnableNoPublicIp = false
