# Source Layout

Shared Python project code lives here.

- `bees_case/config.py`: pipeline parameters and medallion paths
- `bees_case/bronze.py`: HTTP ingestion helpers and raw row preparation
- `bees_case/dashboard_data.py`: `gold` and `ops` artifact loading for the dashboard
- `bees_case/pyspark_local.py`: local and Colab validation in PySpark
- `bees_case/contracts.py`: column contracts and required fields
- `bees_case/observability.py`: structured execution events
- `bees_case/quality.py`: reusable quality checks
