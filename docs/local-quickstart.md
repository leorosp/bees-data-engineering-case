# Local Quickstart

This is the primary runtime path for validating the case with `PySpark`, without depending on cloud infrastructure or an external managed runtime.

## What This Flow Validates

- `bronze` generation
- `silver` transformation with location-based partitioning
- `gold` aggregation
- quality results and execution events in `ops`
- critical quality gate for `required_fields` and `duplicate_primary_keys`
- optional orchestration with `Luigi`

## How To Run Locally

1. Clone the repository and enter the project folder:

```bash
git clone https://github.com/leorosp/bees-data-engineering-case.git
cd bees-data-engineering-case
```

2. Install the project:

```bash
pip install -e ".[dev,local,dashboard]"
```

3. Run the primary flow against the Open Brewery DB API:

```bash
python scripts/run_api_pyspark_pipeline.py --output-dir local_output
```

4. If you want a deterministic demo execution:

```bash
python scripts/run_local_pyspark_demo.py
```

5. Check the generated artifacts under `local_output/`:

- `bronze/landing_date=.../`
- `silver/breweries/`
- `gold/breweries_by_type_location/`
- `ops/quality_results/`
- `ops/execution_events/`

6. If you want to explore the output visually:

```bash
python -m streamlit run dashboard/app.py
```

7. If you want to execute the orchestrated path:

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

## How To Run With Docker

If you want to validate the containerized path:

```bash
docker compose run --rm pipeline
docker compose up dashboard
```

If you want to exercise containerized orchestration:

```bash
docker compose run --rm orchestrator
```

## How To Run In Google Colab

In a Colab cell:

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

## How To Exercise The Quality Gate

The repository already includes `examples/sample_breweries_bad.json`.

```bash
python scripts/run_local_pyspark_demo.py \
  --source-file examples/sample_breweries_bad.json \
  --output-dir local_output_bad \
  --run-id bad-case-001 \
  --landing-date 2026-03-16 \
  --fail-on-critical-quality
```

Expected result:

- the command ends with an error
- `required_fields = fail`
- `duplicate_primary_keys = fail`
- `negative_brewery_count = pass`

## Important Note

In this flow, `silver`, `gold`, and `ops` are written in `parquet` to preserve compatibility with pure `PySpark`.

## Recommended Next Step

Once this flow succeeds:

- open the dashboard
- document the visual evidence
- or evolve persistence to `GCP`
