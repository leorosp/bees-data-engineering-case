# Runbook

## Ordem de execucao recomendada

1. Rodar o pipeline local ou no `Google Colab` com `PySpark`
2. Validar as camadas `bronze`, `silver`, `gold` e `ops`
3. Abrir o dashboard em `Streamlit`
4. Demonstrar a execucao de referencia
5. Demonstrar o quality gate com dataset problematico
6. So depois considerar evolucao para `GCP`

## Parametros minimos do demo

- `source-file`: arquivo JSON com breweries
- `output-dir`: pasta onde os artefatos serao escritos
- `landing-date`: data logica da ingestao
- `run-id`: identificador unico da execucao
- `fail-on-critical-quality`: interrompe a execucao se uma regra critica falhar

## Evidencias de sucesso

- arquivos raw em `bronze/landing_date=...`
- artefato `silver/breweries`
- artefato `gold/breweries_by_type_location`
- quality logs em `ops/quality_results`
- execution logs em `ops/execution_events`
- `quality_gate_status = pass`
- dashboard carregando `gold` e `ops`

## Sequencia de teste recomendada

1. Rodar `python scripts/run_local_pyspark_demo.py`
2. Inspecionar os artefatos gerados
3. Rodar `python -m streamlit run dashboard/app.py`
4. Reexecutar com `examples/sample_breweries_bad.json`
5. Confirmar `fail` nas regras esperadas

## Quality gate em execucao

```bash
python scripts/run_local_pyspark_demo.py \
  --source-file examples/sample_breweries_bad.json \
  --output-dir local_output_bad \
  --run-id bad-case-001 \
  --landing-date 2026-03-16 \
  --fail-on-critical-quality
```

## Pendencias para producao

- persistencia em cloud
- monitoramento real
- camada de BI em `GCP`
- screenshots publicadas no README principal
