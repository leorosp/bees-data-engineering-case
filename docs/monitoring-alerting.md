# Monitoring and Alerting

This document closes the requirement in the case that asks how the pipeline would be monitored and alerted.

## What Already Exists In The MVP

The project already generates two operational artifacts that act as the observability baseline:

- `ops/quality_results`: the result of each quality check, including layer, status, metric, and message
- `ops/execution_events`: execution status, input volume, output volume, details, and timestamp

These artifacts are produced by the `PySpark` pipeline and also surfaced in the `Streamlit` dashboard.

## Events That Should Be Monitored

### 1. Pipeline Failure

Signal:
- a `Luigi` stage fails even after retries
- `status = failed` in `ops/execution_events`

Action:
- open a high-priority alert
- send a notification to email or team channel
- block promotion of the data for downstream consumption

### 2. Critical Quality Failure

Signal:
- any critical check in `ops/quality_results` with `status = fail`
- pipeline executed with `--fail-on-critical-quality`

Action:
- fail the job immediately
- trigger a high-priority alert
- attach the `run_id`, failing check, and observed metric to the alert

### 3. Abnormal Volume Drop

Signal:
- relevant difference between `records_in` and `records_out`
- unexpected drop between `bronze`, `silver`, and `gold`

Action:
- medium-priority alert
- review whether the drop came from legitimate deduplication, expected filtering, or a source problem

Example rule:
- alert when the volume reduction exceeds `30%` outside a known test dataset

### 4. Missing Or Delayed Execution

Signal:
- no execution occurred at the expected time
- latest execution timestamp is above the defined SLA

Action:
- medium-priority alert
- check scheduler, credentials, and API availability

### 5. Growth In Nulls Or Duplicates

Signal:
- growth in `missing_required_fields`
- growth in `duplicate_primary_keys`

Action:
- medium-priority alert
- keep historical trends by `run_id` to distinguish noise from regression

## Suggested Severity

| Scenario | Severity | Action |
| --- | --- | --- |
| Pipeline failed after retries | High | Immediate notification and investigation |
| Critical check failed | High | Fail the pipeline and alert |
| Abnormal volume drop | Medium | Investigate and compare with previous executions |
| Missing or delayed execution | Medium | Check scheduler and external dependency |
| Moderate drift in nulls or duplicates | Low/Medium | Track the trend and open an incident if it persists |

## How I Would Implement This In Production

### GCP Path

1. Persist `ops/quality_results` and `ops/execution_events` in `Google Cloud Storage` or `BigQuery`.
2. Send structured logs from `Luigi` and `PySpark` to `Cloud Logging`.
3. Create metrics from logs and operational tables.
4. Configure alerts in `Cloud Monitoring`.
5. Send notifications through email, webhook, Slack, or PagerDuty.

### Concrete GCP Alerts

- `pipeline_failure_alert`
  - condition: the job ended in error or no success event exists inside the expected time window
- `critical_quality_alert`
  - condition: a row with `status = fail` exists for a critical check
- `volume_drop_alert`
  - condition: `records_out / records_in < 0.70`
- `freshness_alert`
  - condition: the latest `event_timestamp_utc` is above the SLA threshold

## What The Evaluator Can Consider Implemented Today

- quality is measured and persisted in the `ops` layer
- execution is registered through operational events
- critical quality gate with optional automatic failure
- retries in the `Luigi` orchestrator
- dashboard showing latest execution status and quality checks

## What Remains As Production Last Mile

- real routing to notification channels
- integration with `Cloud Monitoring`
- execution history for anomaly-based alerting
- formal SLA and severity policies
