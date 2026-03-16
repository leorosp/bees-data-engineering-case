# Infraestrutura Azure

Esta pasta contem a base de infraestrutura como codigo para a stack inicial do projeto.

## O que ja esta provisionado no template

- `Azure Data Lake Storage Gen2`
- `Azure Data Factory`
- `Azure Databricks Workspace`
- `Azure Key Vault`
- `Log Analytics Workspace`
- `Azure Monitor Action Group`

## Estrutura

```text
infra/
|- README.md
`- bicep/
   |- main.bicep
   |- modules/
   |  |- action-group.bicep
   |  |- data-factory.bicep
   |  |- databricks.bicep
   |  |- key-vault.bicep
   |  |- log-analytics.bicep
   |  `- storage.bicep
   `- parameters/
      `- dev.bicepparam
```

## Como implantar

Exemplo com Azure CLI:

```bash
az group create --name rg-bees-case-dev --location eastus
az deployment group create \
  --resource-group rg-bees-case-dev \
  --template-file infra/bicep/main.bicep \
  --parameters infra/bicep/parameters/dev.bicepparam
```

## Observacoes

- O arquivo `dev.bicepparam` ainda deixa `notificationEmail` em branco.
- O template provisiona a fundacao da plataforma, mas ainda nao cria:
  - role assignments
  - diagnosticos detalhados por recurso
  - pipelines do ADF
  - notebooks/jobs do Databricks

Esses itens entram nas proximas etapas.
