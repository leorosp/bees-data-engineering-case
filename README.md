# BEES Data Engineering Case

Repositorio do case de Data Engineering da Open Brewery DB, com implementacao principal em `Google Colab + PySpark` e arquitetura medallion.

## Objetivo

Entregar uma solucao propria, inspirada nas melhores ideias dos repositorios analisados, com um caminho principal simples e validado:

- ingestao da API ou uso de dataset de exemplo no `Google Colab`
- transformacoes em `PySpark`
- camadas `bronze`, `silver` e `gold`
- qualidade, observabilidade e dashboard
- possibilidade de evolucao para `GCP`

## Caminho principal e evolucao

- `Google Colab`: ambiente principal de execucao e validacao
- `PySpark`: processamento e transformacoes
- `Streamlit`: dashboard executivo e operacional
- `GitHub Actions`: CI/CD do repositorio
- `GCP` como trilha de producao opcional:
  - `Google Cloud Storage`
  - `Dataproc Serverless`
  - `BigQuery`
  - `Looker Studio`

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
- [Guia Colab/GCP](./docs/gcp-colab-guide.md)
- [Dashboard](./dashboard/README.md)
- [Notas Azure opcionais](./docs/azure-notes.md)

## O que esta implementado agora

- fluxo local e Google Colab em `PySpark` para validar `bronze/silver/gold/ops` sem Azure
- dataset de exemplo para reproducao controlada
- notebook bronze para ingestao paginada da API
- notebook silver para limpeza, tipagem e deduplicacao
- notebook gold para agregacoes finais
- notebook de quality e observabilidade
- checagem de duplicidade no `bronze` antes da deduplicacao do `silver`
- dashboard local em `Streamlit` lendo `gold` e `ops`
- testes unitarios e de integracao basicos
- workflow de CI no GitHub Actions
- arquitetura de evolucao para `GCP`

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

## Como abrir o dashboard

O dashboard local faz parte do caminho principal do projeto e le os artefatos gerados pelo fluxo `PySpark`.

No `PowerShell`, rode a partir da pasta do repositorio:

```powershell
cd "C:\Users\leona\Documents\GitHub\bees-data-engineering-case"
pip install -e ".[local,dashboard]"
python scripts/run_local_pyspark_demo.py
python -m streamlit run dashboard/app.py
```

Depois abra `http://localhost:8501`.

Uso esperado:

- `Artifacts folder = local_output`: mostra o cenario saudavel
- `Artifacts folder = local_output_bad`: mostra o cenario com falhas de qualidade

Se aparecer erro de `pyproject.toml`, `scripts/run_local_pyspark_demo.py` ou `dashboard/app.py` inexistente, isso normalmente significa que o terminal nao esta na pasta do projeto.

## Escopo atual

- `implementado e validado`: `Google Colab + PySpark + Streamlit`
- `documentado como evolucao`: `GCP`
- `mantido apenas como referencia opcional`: arquivos de `Azure` em `infra/` e `orchestration/adf/`

## Referencias

- [Open Brewery DB](https://www.openbrewerydb.org/)
- [Google Colab](https://colab.research.google.com/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Streamlit Documentation](https://docs.streamlit.io/)
