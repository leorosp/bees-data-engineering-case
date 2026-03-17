# Evaluator Quickstart

This guide exists to reduce evaluation friction. Run every command below from the repository root.

## Fast Demo

```bash
pip install -e ".[dev,local,dashboard]"
python scripts/run_api_pyspark_pipeline.py --output-dir local_output
python -m streamlit run dashboard/app.py
```

Open `http://localhost:8501`.

If you want a deterministic path that does not depend on the live API:

```bash
python scripts/run_local_pyspark_demo.py
```

## Quick Luigi Run

```bash
python -m luigi --module orchestration.luigi_pipeline PipelineOrchestration \
  --local-scheduler \
  --output-dir luigi_output \
  --landing-date 2026-03-16 \
  --run-id luigi-run-001
```

Deterministic option:

```bash
python -m luigi --module orchestration.luigi_pipeline PipelineOrchestration \
  --local-scheduler \
  --source-mode sample \
  --source-file examples/sample_breweries.json \
  --output-dir luigi_output \
  --landing-date 2026-03-16 \
  --run-id luigi-run-001
```

## What To Verify

1. The primary path consumes the Open Brewery DB API
2. The pipeline generates `bronze`, `silver`, `gold`, and `ops` under `local_output/`
3. `silver` is written in `parquet` partitioned by `country` and `state_province`
4. The dashboard shows the executive summary and pipeline health
5. The standard execution ends with `quality_gate_status = pass`
6. The `ops` layer records quality and execution data for observability

## Exercising The Quality Gate

```bash
python scripts/run_local_pyspark_demo.py \
  --source-file examples/sample_breweries_bad.json \
  --output-dir local_output_bad \
  --run-id bad-case-001 \
  --landing-date 2026-03-16 \
  --fail-on-critical-quality
```

## Expected Result

- the process fails by design
- `required_fields` fails
- `duplicate_primary_keys` fails
- `local_output_bad/ops/quality_results/` remains available for inspection

## Monitoring and Alerting

The project also includes an explicit monitoring and alerting design for:

- pipeline failure
- critical quality failure
- execution delay
- abnormal volume drops

Details in [monitoring-alerting.md](./monitoring-alerting.md).

## If You Only Want To Inspect The Data

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.read.parquet("local_output/gold/breweries_by_type_location").show(truncate=False)
spark.read.parquet("local_output/ops/quality_results").show(truncate=False)
```
