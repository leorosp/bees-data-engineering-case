# Implementation Backlog

## Phase 0 - Foundation

- [x] Define the final project name
- [x] Organize the GitHub repository
- [x] Define the medallion architecture
- [x] Create the initial documentation

## Phase 1 - Colab/PySpark Pipeline

- [x] Create `bronze` ingestion
- [x] Create `silver` transformation
- [x] Create `gold` aggregations
- [x] Create the `ops` layer

## Phase 2 - Data Quality

- [x] Define minimum contracts
- [x] Implement required field checks
- [x] Implement duplicate checks
- [x] Persist results in `ops`
- [x] Fail execution automatically when a critical rule is violated
- [x] Document MVP monitoring and alerting

## Phase 3 - Case Evidence

- [x] Validate a reference execution
- [x] Validate the quality gate with a problematic dataset
- [x] Document the evidence in the repository
- [x] Create an evaluator quickstart

## Phase 4 - Consumption

- [x] Create an executive and operational dashboard in Streamlit
- [x] Publish screenshots in the README
- [ ] Create a shorter presentation version

## Phase 5 - GCP Evolution

- [ ] Move persistence to `Google Cloud Storage`
- [ ] Execute the pipeline in `Dataproc Serverless`
- [ ] Publish the analytical layer in `BigQuery`
- [ ] Create a dashboard in `Looker Studio`
- [ ] Configure monitoring in `Cloud Monitoring`

## Phase 6 - Hardening

- [x] Create a GitHub Actions pipeline for automated tests
- [x] Expand tests to the full PySpark flow
- [ ] Automate visual dashboard validation
- [ ] Review cost and simplify operations
