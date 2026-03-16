# Infraestrutura Azure Opcional

Esta pasta contem uma referencia opcional de infraestrutura como codigo para uma evolucao enterprise em Azure.

Ela nao faz parte do caminho principal do projeto, que hoje e:

- `Google Colab`
- `PySpark`
- `Streamlit`

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
- O template cria um filesystem `lake` no ADLS Gen2.
- As camadas `bronze`, `silver`, `gold` e `ops` sao pastas dentro desse filesystem.
- O template provisiona a fundacao da plataforma, mas ainda nao cria:
  - role assignments
  - diagnosticos detalhados por recurso
  - pipelines do ADF
  - notebooks/jobs do Databricks

Esses itens entram apenas se voce decidir seguir pela trilha opcional de Azure.
