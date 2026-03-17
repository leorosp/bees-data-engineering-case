# Runbook

## Recommended Execution Order

1. Run the pipeline locally or in `Google Colab` with `PySpark`
2. Validate the orchestrated execution with `Luigi`
3. Validate the `bronze`, `silver`, `gold`, and `ops` layers
4. Open the dashboard in `Streamlit`
5. Demonstrate the reference execution
6. Demonstrate the quality gate with the problematic dataset
7. Only then consider the `GCP` evolution path

## Minimum Demo Parameters

- `source-file`: JSON file containing breweries
- `output-dir`: folder where artifacts will be written
- `landing-date`: logical ingestion date
- `run-id`: unique execution identifier
- `fail-on-critical-quality`: interrupts the execution if a critical rule fails

## Success Evidence

- raw files under `bronze/landing_date=...`
- `silver/breweries` artifact
- `country=.../state_province=...` partitions under `silver/breweries`
- `gold/breweries_by_type_location` artifact
- quality logs in `ops/quality_results`
- execution logs in `ops/execution_events`
- `quality_gate_status = pass`
- dashboard loading `gold` and `ops`

## Monitoring and Alerting

The pipeline already produces the operational baseline that supports observability:

- `ops/quality_results` for quality checks
- `ops/execution_events` for status, volume, and execution details
- optional automatic failure with `--fail-on-critical-quality`
- retries in `Luigi`

Recommended production alert rules are documented in [monitoring-alerting.md](./monitoring-alerting.md).

## Recommended Test Sequence

1. Run `python scripts/run_api_pyspark_pipeline.py --output-dir local_output`
2. Run `python -m luigi --module orchestration.luigi_pipeline PipelineOrchestration --local-scheduler`
3. Inspect the generated artifacts
4. Run `python -m streamlit run dashboard/app.py`
5. Re-run with `examples/sample_breweries_bad.json`
6. Confirm `fail` for the expected rules

## Quality Gate In Action

```bash
python scripts/run_local_pyspark_demo.py \
  --source-file examples/sample_breweries_bad.json \
  --output-dir local_output_bad \
  --run-id bad-case-001 \
  --landing-date 2026-03-16 \
  --fail-on-critical-quality
```

## Production Follow-Ups

- cloud persistence
- managed alert routing in cloud
- BI layer in `GCP`
