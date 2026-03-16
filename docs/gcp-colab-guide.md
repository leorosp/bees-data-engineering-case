# Guia Colab e GCP

Este e o caminho oficial do projeto a partir desta versao.

## Caminho atual validado

- execucao em `Google Colab`
- processamento em `PySpark`
- artefatos em `local_output/`
- dashboard em `Streamlit`

## Como rodar no Colab

```python
!git clone https://github.com/leorosp/bees-data-engineering-case.git
%cd bees-data-engineering-case
!pip install -q -e ".[local,dashboard]"
!python scripts/run_local_pyspark_demo.py
```

Depois disso:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.read.parquet("local_output/gold/breweries_by_type_location").show(truncate=False)
spark.read.parquet("local_output/ops/quality_results").show(truncate=False)
```

## Como evoluir para GCP

1. Trocar `local_output/` por um bucket no `Google Cloud Storage`
2. Executar o mesmo pipeline em `Dataproc Serverless`
3. Publicar os agregados no `BigQuery`
4. Construir o dashboard executivo no `Looker Studio`

## O que ja esta pronto para essa evolucao

- logica do pipeline em `PySpark`
- artefatos `bronze/silver/gold/ops`
- regras de qualidade
- dashboard local como referencia funcional
