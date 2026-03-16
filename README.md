# BEES Data Engineering Case

Repositorio base para o case de Data Engineering da Open Brewery DB, desenhado para rodar no ecossistema Azure com arquitetura medallion.

## Objetivo

Entregar uma solucao propria, inspirada nas melhores ideias dos repositorios analisados, mas com um desenho Azure-first:

- ingestao da API via Azure Data Factory
- data lake em Azure Data Lake Storage Gen2
- transformacoes em Azure Databricks
- camadas `bronze`, `silver` e `gold`
- qualidade, observabilidade e alertas nativos da Azure

## Servicos Azure escolhidos

- `Azure Data Factory`: orquestracao, ingestao REST e agendamento
- `Azure Data Lake Storage Gen2`: armazenamento bronze/silver/gold
- `Azure Databricks`: processamento PySpark e Delta
- `Azure Key Vault`: gestao de segredos
- `Azure Monitor` + `Log Analytics Workspace` + `Action Groups`: monitoramento e alertas
- `Power BI`: camada de consumo executiva
- `GitHub Actions`: CI/CD do repositorio

Detalhes e justificativas estao em [docs/services.md](./docs/services.md).

## Estrutura do repositorio

```text
bees-data-engineering-case/
|- README.md
|- docs/
|  |- architecture.md
|  |- services.md
|  `- backlog.md
|- infra/
|  |- README.md
|  `- bicep/
|     |- main.bicep
|     |- modules/
|     |  |- action-group.bicep
|     |  |- data-factory.bicep
|     |  |- databricks.bicep
|     |  |- key-vault.bicep
|     |  |- log-analytics.bicep
|     |  `- storage.bicep
|     `- parameters/
|        `- dev.bicepparam
|- orchestration/
|  `- adf/
|     `- README.md
|- databricks/
|  `- notebooks/
|     |- README.md
|     |- bronze/.gitkeep
|     |- silver/.gitkeep
|     `- gold/.gitkeep
|- src/
|  |- README.md
|  |- common/.gitkeep
|  |- jobs/
|  |  |- bronze/.gitkeep
|  |  |- silver/.gitkeep
|  |  `- gold/.gitkeep
|  `- quality/.gitkeep
`- tests/
   |- README.md
   |- unit/.gitkeep
   |- integration/.gitkeep
   `- data_quality/.gitkeep
```

## Leitura recomendada

- [Arquitetura](./docs/architecture.md)
- [Escolha dos servicos](./docs/services.md)
- [Backlog de implementacao](./docs/backlog.md)
- [Infraestrutura](./infra/README.md)

## Referencias usadas

- [ocamposfaria/bees-data-engineering-case](https://github.com/ocamposfaria/bees-data-engineering-case)
- [Gabriel0598/BEES-Breweries-Case](https://github.com/Gabriel0598/BEES-Breweries-Case)
- [brunobws/aws-api-capture-dl-medallion](https://github.com/brunobws/aws-api-capture-dl-medallion)
- [wuldson-franco/breweries_case](https://github.com/wuldson-franco/breweries_case)
- [Azure Data Factory REST connector](https://learn.microsoft.com/en-us/azure/data-factory/connector-rest)
- [Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/upgrade-to-data-lake-storage-gen2)
- [Azure Databricks schema evolution](https://learn.microsoft.com/en-us/azure/databricks/data-engineering/schema-evolution)
- [Azure Monitor alerts overview](https://learn.microsoft.com/en-us/azure/azure-monitor/alerts/alerts-overview)
- [Azure Monitor action groups](https://learn.microsoft.com/en-us/azure/azure-monitor/alerts/action-groups)
