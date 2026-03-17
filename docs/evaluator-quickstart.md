# Evaluator Quickstart

Este guia existe para reduzir o atrito de avaliacao. Todos os comandos abaixo devem ser executados a partir da raiz do repositorio.

## Demo rapida

```bash
pip install -e ".[dev,local,dashboard]"
python scripts/run_local_pyspark_demo.py
python -m streamlit run dashboard/app.py
```

Abra `http://localhost:8501`.

## Orquestracao rapida com Luigi

```bash
python -m luigi --module orchestration.luigi_pipeline PipelineOrchestration \
  --local-scheduler \
  --source-file examples/sample_breweries.json \
  --output-dir luigi_output \
  --landing-date 2026-03-16 \
  --run-id luigi-run-001
```

## O que verificar

1. O pipeline gera `bronze`, `silver`, `gold` e `ops` em `local_output/`
2. A `silver` e gravada em `parquet` particionado por `country` e `state_province`
3. O dashboard mostra o resumo executivo e a saude do pipeline
4. A execucao padrao termina com `quality_gate_status = pass`
5. A camada `ops` registra qualidade e execucao para observabilidade

## Exercitando o Quality Gate

```bash
python scripts/run_local_pyspark_demo.py \
  --source-file examples/sample_breweries_bad.json \
  --output-dir local_output_bad \
  --run-id bad-case-001 \
  --landing-date 2026-03-16 \
  --fail-on-critical-quality
```

## Resultado esperado

- o processo termina com erro por design
- `required_fields` falha
- `duplicate_primary_keys` falha
- `local_output_bad/ops/quality_results/` continua disponivel para inspecao

## Monitoramento e alertas

O projeto tambem inclui um desenho explicito de monitoramento e alertas para:

- falha de pipeline
- falha critica de qualidade
- atraso de execucao
- queda anormal de volume

Detalhes em [monitoring-alerting.md](./monitoring-alerting.md).

## Se quiser avaliar so os dados

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.read.parquet("local_output/gold/breweries_by_type_location").show(truncate=False)
spark.read.parquet("local_output/ops/quality_results").show(truncate=False)
```
