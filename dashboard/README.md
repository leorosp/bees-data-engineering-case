# Streamlit Dashboard

The dashboard is the presentation layer of the project. It organizes the experience into three blocks:

- `Overview`: `gold` KPIs, pipeline status, and latest execution
- `Analytics`: type distribution, geographic concentration, and a filterable table
- `Operational Details`: quality checks, execution summary, and local run instructions

## How To Run

Run the commands below from the repository root:

```bash
pip install -e ".[dev,local,dashboard]"
python scripts/run_api_pyspark_pipeline.py --output-dir local_output
python -m streamlit run dashboard/app.py
```

Open `http://localhost:8501`.

If `local_output/` does not exist yet, the app opens automatically with the `Demo Dataset`.

For a deterministic local run that does not depend on the live API:

```bash
python scripts/run_local_pyspark_demo.py
```

## Dashboard Controls

Filters and controls live in the collapsed sidebar:

- `Data Source`: chooses between `Demo Dataset` and any available local artifacts
- `Refresh Dashboard`: reloads the data
- `Generate Demo Output`: attempts to generate `local_output` from the app itself
- `Brewery Types` and `States`: apply filters to the visualization

## Data Sources

When local artifacts exist, the app reads:

- `local_output/gold/breweries_by_type_location`
- `local_output/ops/quality_results`
- `local_output/ops/execution_events`

## Note About Local PySpark

If `Generate Demo Output` fails, the dashboard still works normally with the bundled demo dataset.
