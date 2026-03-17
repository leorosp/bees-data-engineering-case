# Local Quickstart

Este e o caminho principal do projeto para validar o case com `PySpark`, sem depender de cloud ou runtime externo.

## O que esse fluxo valida

- geracao da camada `bronze`
- transformacao da camada `silver`
- agregacao da camada `gold`
- resultados de qualidade e eventos em `ops`
- quality gate critico para `required_fields` e `duplicate_primary_keys`

## Como rodar localmente

1. Clone o repositorio e entre na pasta:

```bash
git clone https://github.com/leorosp/bees-data-engineering-case.git
cd bees-data-engineering-case
```

2. Instale o projeto:

```bash
pip install -e ".[dev,local,dashboard]"
```

3. Rode o demo local:

```bash
python scripts/run_local_pyspark_demo.py
```

4. Confira os artefatos gerados em `local_output/`:

- `bronze/landing_date=.../`
- `silver/breweries/`
- `gold/breweries_by_type_location/`
- `ops/quality_results/`
- `ops/execution_events/`

5. Se quiser explorar visualmente:

```bash
python -m streamlit run dashboard/app.py
```

## Como rodar no Google Colab

Em uma celula do Colab:

```python
!git clone https://github.com/leorosp/bees-data-engineering-case.git
%cd bees-data-engineering-case
!pip install -q -e ".[dev,local,dashboard]"
!python scripts/run_local_pyspark_demo.py
```

## Como exercitar o quality gate

O repositorio ja inclui `examples/sample_breweries_bad.json`.

```bash
python scripts/run_local_pyspark_demo.py \
  --source-file examples/sample_breweries_bad.json \
  --output-dir local_output_bad \
  --run-id bad-case-001 \
  --landing-date 2026-03-16 \
  --fail-on-critical-quality
```

Resultado esperado:

- o comando termina com erro
- `required_fields = fail`
- `duplicate_primary_keys = fail`
- `negative_brewery_count = pass`

## Observacao importante

Nesse fluxo, `silver`, `gold` e `ops` sao gravados em `parquet` para manter compatibilidade com `PySpark` puro.

## Proximo passo recomendado

Depois que esse fluxo passar:

- abra o dashboard
- documente a evidencia visual
- ou evolua a persistencia para `GCP`
