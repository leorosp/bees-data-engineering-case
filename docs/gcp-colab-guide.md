# Colab and GCP Guide

This is the official project path from the current version onward.

## Current Validated Path

- execution in `Google Colab`
- processing in `PySpark`
- artifacts in `local_output/`
- dashboard in `Streamlit`

## How To Run In Colab

```python
!git clone https://github.com/leorosp/bees-data-engineering-case.git
%cd bees-data-engineering-case
!pip install -q -e ".[dev,local,dashboard]"
!python scripts/run_api_pyspark_pipeline.py --output-dir local_output
```

For a deterministic demo in Colab:

```python
!python scripts/run_local_pyspark_demo.py
```

After that:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.read.parquet("local_output/gold/breweries_by_type_location").show(truncate=False)
spark.read.parquet("local_output/ops/quality_results").show(truncate=False)
```

## How To Evolve To GCP

1. Replace `local_output/` with a `Google Cloud Storage` bucket
2. Execute the same pipeline in `Dataproc Serverless`
3. Publish the aggregates in `BigQuery`
4. Build the executive dashboard in `Looker Studio`

## What Is Already Ready For That Evolution

- pipeline logic in `PySpark`
- `bronze/silver/gold/ops` artifacts
- quality rules
- critical quality gate with optional automatic failure
- local dashboard as a functional reference
