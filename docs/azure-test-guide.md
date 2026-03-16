# Guia de teste na Azure (Opcional)

Este documento ficou mantido apenas como referencia opcional.

O caminho principal do projeto e `Google Colab + PySpark`.

## 1. Deploy da infraestrutura

No Azure Cloud Shell:

```bash
git clone https://github.com/leorosp/bees-data-engineering-case.git
cd bees-data-engineering-case

az group create --name rg-bees-case-dev --location eastus2
az deployment group create \
  --resource-group rg-bees-case-dev \
  --template-file infra/bicep/main.bicep \
  --parameters infra/bicep/parameters/dev.bicepparam
```

No final do deploy, guarde o valor de `lakehouse.basePath`.

## 2. Validacao do storage

No portal Azure, abra a `Storage Account` criada e confirme a existencia do filesystem:

- `lake`

As camadas do projeto vao ficar como pastas dentro dele:

- `bronze/`
- `silver/`
- `gold/`
- `ops/`

## 3. Teste manual no Azure Databricks

Antes de envolver o `ADF`, valide os notebooks direto no Databricks.

1. Abra o workspace do `Azure Databricks`.
2. Crie um cluster simples para teste.
3. Clone o repositrio `https://github.com/leorosp/bees-data-engineering-case` em um `Git folder` do Databricks.
4. Abra os notebooks dentro da pasta:
   - `databricks/notebooks/bronze/01_ingest_open_brewery_db.py`
   - `databricks/notebooks/silver/02_build_silver.py`
   - `databricks/notebooks/gold/03_build_gold.py`
   - `databricks/notebooks/gold/04_quality_and_ops.py`

Parametros sugeridos para o primeiro teste:

- `base_path`: `abfss://lake@<storage-account>.dfs.core.windows.net`
- `landing_date`: data de hoje no formato `YYYY-MM-DD`
- `run_id`: `manual-test-001`
- `per_page`: `50`
- `max_pages`: `3`

Ordem de execucao:

1. `bronze/01_ingest_open_brewery_db.py`
2. `silver/02_build_silver.py`
3. `gold/03_build_gold.py`
4. `gold/04_quality_and_ops.py`

## 4. O que verificar depois do teste manual

No storage:

- `lake/bronze/landing_date=<data>/`
- `lake/silver/breweries/`
- `lake/gold/breweries_by_type_location/`
- `lake/ops/quality_results/`
- `lake/ops/execution_events/`

No Databricks:

- o notebook bronze deve retornar `bronze_output_path`
- o notebook silver deve retornar `silver_output_path`
- o notebook gold deve retornar `gold_output_path`
- o notebook quality deve retornar `quality_results_path` e `execution_events_path`

## 5. Teste do ADF depois do manual

So depois que o teste manual passar:

1. Gere um `PAT` no Databricks.
2. Salve o segredo `databricks-pat` no `Azure Key Vault`.
3. Ajuste os placeholders em `orchestration/adf/linkedServices/ls_azure_databricks_workspace.json`.
4. Publique os linked services e o pipeline `orchestration/adf/pipelines/pl_bees_orchestration.json`.
5. Execute o pipeline com:
   - `basePath`: `abfss://lake@<storage-account>.dfs.core.windows.net`
   - `landingDate`: data do teste
   - `runId`: `adf-test-001`

## 6. Se algo falhar

Pontos mais comuns:

- `basePath` com nome errado da `Storage Account`
- notebooks rodando fora do `Git folder` do Databricks
- cluster sem permissao de acesso ao storage
- `PAT` ausente ou salvo com outro nome no `Key Vault`
- placeholders ainda nao substituidos no linked service do Databricks
