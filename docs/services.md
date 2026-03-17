# Service and Runtime Choices

## Primary V1 Path

| Component | Role in the project | Why it was chosen |
| --- | --- | --- |
| Google Colab | Primary execution and validation runtime | Low friction for demonstrating the case |
| PySpark | Transformations, validations, and aggregations | Technical alignment with the challenge |
| Luigi | Orchestration across bronze-silver-gold-ops | Covers scheduling concepts, retries, and error handling with low overhead |
| Local files / parquet-json artifacts | Persistence for `bronze`, `silver`, `gold`, and `ops` | Simple, reproducible, and sufficient for the MVP |
| Streamlit | Executive and operational dashboard | Visual layer that enriches the project |
| `ops/quality_results` and `ops/execution_events` | Operational monitoring baseline | Tracks quality, failures, and volume per execution |
| GitHub Actions | CI/CD | Integrates naturally with the repository |

## Recommended GCP Evolution

| Service | Role in the project | When to use it |
| --- | --- | --- |
| Google Cloud Storage | Bronze/silver/gold data lake | When cloud persistence becomes necessary |
| Dataproc Serverless | Scaled PySpark execution | When the pipeline moves beyond Colab |
| BigQuery | Serving and analytical querying | When SQL and BI consumption are needed |
| Looker Studio | Cloud-native executive dashboard | When a Google-native BI layer is desired |
| Cloud Monitoring | Observability and alerting | When execution moves to GCP |
| Secret Manager | Secrets management | When API or service credentials are introduced |

## Monitoring and Alerting in the Current Design

Even in the local or Colab MVP, the pipeline already produces enough signals for observability:

- execution success or failure in `ops/execution_events`
- quality checks in `ops/quality_results`
- retries in `Luigi`
- optional critical quality gate failure

The full alerting design and the path to `Cloud Monitoring` are described in [monitoring-alerting.md](./monitoring-alerting.md).

## What Is Not Required For The Case

| Item | Reason |
| --- | --- |
| Airflow | Heavier than necessary for V1, given that `Luigi` already covers orchestration |
| Docker | Optional in the prompt |
| BigQuery in V1 | The MVP is already strong with Colab + PySpark + dashboard |

## Current Repository Path

| Status | Item |
| --- | --- |
| Implemented | Colab + PySpark |
| Implemented | Luigi orchestration |
| Implemented | Streamlit dashboard |
| Implemented | Quality evidence for success and failure scenarios |
| Implemented | Monitoring and alerting design |
| Documented | GCP evolution path |
