# Runbook

## Ordem de execucao recomendada

1. Rodar o pipeline em `Google Colab` ou local com `PySpark`
2. Validar as camadas `bronze`, `silver`, `gold` e `ops`
3. Abrir o dashboard em `Streamlit`
4. Demonstrar o cenario feliz
5. Demonstrar o cenario com falha controlada
6. So depois considerar evolucao para `GCP`

## Parametros minimos do demo

- `source-file`: arquivo JSON com breweries
- `output-dir`: pasta onde os artefatos serao escritos
- `landing-date`: data logica da ingestao
- `run-id`: identificador unico da execucao

## Evidencias de sucesso

- arquivos raw em `bronze/landing_date=...`
- artefato `silver/breweries`
- artefato `gold/breweries_by_type_location`
- quality logs em `ops/quality_results`
- execution logs em `ops/execution_events`
- dashboard carregando `gold` e `ops`

## Sequencia de teste recomendada

1. Rodar `python scripts/run_local_pyspark_demo.py`
2. Inspecionar os artefatos gerados
3. Rodar `streamlit run dashboard/app.py`
4. Reexecutar com um dataset ruim
5. Confirmar `fail` nas regras esperadas

## Pendencias para producao

- falha automatica da execucao quando regras criticas forem violadas
- persistencia em cloud
- monitoramento real
- camada de BI em GCP
