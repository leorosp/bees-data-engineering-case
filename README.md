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
|- pyproject.toml
|- requirements-dev.txt
|- README.md
|- docs/
|  |- architecture.md
|  |- services.md
|  |- backlog.md
|  `- runbook.md
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
|     |- README.md
|     |- linkedServices/
|     |  |- ls_key_vault.json
|     |  `- ls_azure_databricks_workspace.json
|     `- pipelines/
|        `- pl_bees_orchestration.json
|- databricks/
|  `- notebooks/
|     |- README.md
|     |- bronze/01_ingest_open_brewery_db.py
|     |- silver/02_build_silver.py
|     |- gold/03_build_gold.py
|     `- gold/04_quality_and_ops.py
|- dashboard/
|  |- README.md
|  `- app.py
|- .github/
|  `- workflows/
|     `- ci.yml
|- src/
|  |- README.md
|  `- bees_case/
|     |- __init__.py
|     |- bronze.py
|     |- config.py
|     |- contracts.py
|     |- dashboard_data.py
|     |- observability.py
|     |- pyspark_local.py
|     `- quality.py
`- tests/
   |- README.md
   |- unit/test_config.py
   |- unit/test_quality.py
   |- integration/test_bronze.py
   `- data_quality/.gitkeep
```

## Leitura recomendada

- [Arquitetura](./docs/architecture.md)
- [Escolha dos servicos](./docs/services.md)
- [Backlog de implementacao](./docs/backlog.md)
- [Runbook](./docs/runbook.md)
- [Quickstart local](./docs/local-quickstart.md)
- [Dashboard](./dashboard/README.md)
- [Guia de teste Azure](./docs/azure-test-guide.md)
- [Infraestrutura](./infra/README.md)

## O que esta implementado agora

- infraestrutura Azure modular em Bicep
- fluxo orquestrado no ADF via notebooks Databricks
- fluxo local e Google Colab em `PySpark` para validar `bronze/silver/gold/ops` sem Azure
- notebook bronze para ingestao paginada da API
- notebook silver para limpeza, tipagem e deduplicacao
- notebook gold para agregacoes finais
- notebook de quality e observabilidade
- checagem de duplicidade no `bronze` antes da deduplicacao do `silver`
- dashboard local em `Streamlit` lendo `gold` e `ops`
- testes unitarios e de integracao basicos
- workflow de CI no GitHub Actions

## Evidencia de validacao em PySpark

O projeto ja foi validado em `Google Colab` com `PySpark` em dois cenarios:

- cenario feliz:
  - `source_record_count = 4`
  - `silver_record_count = 4`
  - `gold_record_count = 3`
  - todas as regras de qualidade em `pass`
- cenario com falha controlada:
  - `source_record_count = 3`
  - `silver_record_count = 2`
  - `required_fields = fail`
  - `duplicate_primary_keys = fail`
  - `negative_brewery_count = pass`

Isso confirma:

- preservacao do payload bruto no `bronze`
- deduplicacao no `silver`
- agregacao correta no `gold`
- deteccao de campos obrigatorios ausentes
- deteccao de chaves duplicadas no `bronze`

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
