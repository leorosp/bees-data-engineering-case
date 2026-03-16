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

5. Se quiser explorar visualmente, rode o dashboard:

```bash
pip install -e ".[local,dashboard]"
streamlit run dashboard/app.py
```

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

## Como forcar um cenario de falha

1. Crie um arquivo de teste com duplicidade e campos obrigatorios faltando:

```python
import json
from pathlib import Path

bad_records = [
    {
        "id": "brew-1",
        "name": "Sample Brewery A",
        "brewery_type": "micro",
        "city": "Denver",
        "state_province": "Colorado",
        "country": "United States"
    },
    {
        "id": "brew-1",
        "name": "Sample Brewery A Duplicate",
        "brewery_type": "micro",
        "city": "Denver",
        "state_province": "Colorado",
        "country": "United States"
    },
    {
        "id": "brew-9",
        "name": "",
        "brewery_type": "micro",
        "city": "Austin",
        "state_province": "Texas",
        "country": ""
    }
]

Path("examples/sample_breweries_bad.json").write_text(
    json.dumps(bad_records, indent=2),
    encoding="utf-8"
)
```

2. Rode o pipeline com esse arquivo:

```bash
python scripts/run_local_pyspark_demo.py \
  --source-file examples/sample_breweries_bad.json \
  --output-dir local_output_bad \
  --run-id bad-case-001 \
  --landing-date 2026-03-16
```

3. Confira a qualidade:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.read.parquet("local_output_bad/ops/quality_results").show(truncate=False)
```

Saida esperada:

- `required_fields = fail`
- `duplicate_primary_keys = fail`
- `negative_brewery_count = pass`

## Observacao importante

Para esse caminho local, `silver`, `gold` e `ops` sao gravados em `parquet` para manter compatibilidade com `PySpark` puro fora do Databricks.

## Proximo passo recomendado

Depois que esse fluxo passar, ai sim vale voltar para a versao Azure, porque voce ja vai estar com a logica do pipeline validada.

## Cenarios esperados

- no dataset de exemplo padrao, todas as regras de qualidade devem ficar em `pass`
- em um dataset com `id` duplicado, a regra `duplicate_primary_keys` deve falhar olhando o `bronze`
- em um dataset com `name` ou `country` vazios, a regra `required_fields` deve falhar no `silver`
