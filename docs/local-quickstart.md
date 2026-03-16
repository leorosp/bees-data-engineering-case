# Local Quickstart

Este e o caminho mais facil para validar o projeto com `PySpark`, sem Azure e sem Databricks.

## O que esse fluxo valida

- geracao da camada `bronze`
- transformacao da camada `silver`
- agregacao da camada `gold`
- resultados de qualidade em `ops`

## Como rodar localmente

1. Clone o repositorio:

```bash
git clone https://github.com/leorosp/bees-data-engineering-case.git
cd bees-data-engineering-case
```

2. Instale o projeto com PySpark:

```bash
pip install -e ".[local]"
```

3. Rode o demo local:

```bash
python scripts/run_local_pyspark_demo.py
```

4. Confira os arquivos gerados em `local_output/`:

- `local_output/bronze/landing_date=.../`
- `local_output/silver/breweries/`
- `local_output/gold/breweries_by_type_location/`
- `local_output/ops/quality_results/`
- `local_output/ops/execution_events/`

## Como rodar no Google Colab

Em uma celula do Colab:

```python
!git clone https://github.com/leorosp/bees-data-engineering-case.git
%cd bees-data-engineering-case
!pip install -q -e ".[local]"
!python scripts/run_local_pyspark_demo.py
```

Depois disso, voce pode abrir o notebook:

- `output/jupyter-notebook/bees-case-local-validation.ipynb`

## Observacao importante

Para esse caminho local, `silver`, `gold` e `ops` sao gravados em `parquet` para manter compatibilidade com `PySpark` puro fora do Databricks.

## Proximo passo recomendado

Depois que esse fluxo passar, ai sim vale voltar para a versao Azure, porque voce ja vai estar com a logica do pipeline validada.
