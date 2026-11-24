# Scope, assumptions and limitations

## Delivered scope

- CPCB/data.gov.in API snapshot profiling and extraction.
- Immutable Bronze JSON storage with extraction metadata and manifests.
- PySpark normalization and within-extraction exact duplicate removal.
- Partitioned Silver Parquet storage in S3.
- Glue Data Catalog registration and Athena validation.
- dbt staging, intermediate, dimensions/facts/marts and tests.
- Historical snapshot comparison by derived station-pollutant entity key.
- Dedicated Airflow Docker deployment inside the same repository, independent of Airflow-Lab.
- End-to-end DAG with preflight, extraction, Bronze, Silver, Glue and dbt tasks.

## Evidence-based limitations

### Official AQI

The current deliverable does not calculate official CPCB AQI. AQI is not interchangeable with a single pollutant concentration or the source's average value. Official calculation requires pollutant-specific concentration units, prescribed averaging periods, eligible valid data coverage, breakpoints and minimum pollutant availability. Those semantics must be verified and implemented before describing any output as official AQI.

### Time-series continuity

The initial two snapshots had one source `last_update` value each, separated by approximately 94 hours. They prove that the API response changed between retrievals; they do not prove that every intermediate hourly measurement is recoverable. Do not label the two-snapshot comparison a continuous hourly trend.

### Station identity

Station name and geography-derived keys are candidates only. Renames, agency suffix changes, location corrections or spelling changes can appear as removed/new entities. Use official station IDs if a reliable source provides them.

### API reliability and cadence

The dataset catalog describes hourly granularity, but this alone does not prove that every API call returns a complete, stable hourly history or that a specific polling rate is allowed. Verify current API policy/rate limit and monitor returned timestamps before enabling a schedule.

### Local Airflow deployment

The Docker Compose deployment is a local development/demo environment. Production use requires durable hosted infrastructure, least-privilege IAM, secret management, backups, monitoring, alerting and secure network controls.

### dbt Windows/Linux cache

The host Windows dbt installation and container Linux dbt installation must use separate target directories. The Airflow DAG uses a Linux-only target path and disables partial parsing to prevent cross-platform manifest cache path errors.
