# India Air Quality Data Lake — Airflow orchestration

This is the project-specific Airflow deployment, isolated from the separate `Airflow-Lab` environment. It uses its own Compose project, PostgreSQL database, Redis broker, Airflow services and UI port `8081`.

## DAGs

- `india_air_quality_environment_check`: infrastructure smoke test; no data or AWS resources are modified.
- `india_air_quality_pipeline`: manual end-to-end orchestration: CPCB extraction -> immutable Bronze upload -> one-extraction PySpark Silver build -> incremental Silver upload -> Glue partition registration -> `dbt build`.

The production DAG has `schedule=None` intentionally. The API currently returns a changing snapshot and hourly historical replay has not been established. Keep ingestion manual until source refresh behavior, API quotas and desired cadence have been verified.

## Incremental Silver design

The original `build_historical_silver.py`, `upload_historical_silver.py` and `register_historical_silver_glue.py` form a one-time bootstrap flow for the two initial snapshots. Do not use the old bootstrap uploader for new runs. The orchestration DAG instead uses:

- `scripts/build_historical_silver.py --extraction-ids <one-id> --output-dir <run-specific-path>`
- `scripts/upload_silver_incremental.py` to append one immutable extraction to the existing Silver table prefix and write a per-extraction manifest last.
- `scripts/register_silver_partition.py` to add or verify the date partition in the existing Glue table.

Exact duplicates are removed only within one extraction. Cross-extraction observations are retained. The partition key remains `ingestion_date` and the extraction ID remains a data column.

## Setup / run

From `airflow/`:

```powershell
docker compose config --quiet
docker compose ps
```

On the Windows host, refresh the Identity Center session before triggering an AWS-backed DAG run:

```powershell
aws sso login --profile india-aqi-admin
```

Ensure `DATA_GOV_API_KEY` is populated in `airflow/.env`. Do not print it or commit `.env`.

Open `http://localhost:8081`, run `india_air_quality_environment_check` first, then manually trigger `india_air_quality_pipeline` after the smoke test succeeds.

## Local SSO limitation

The Compose deployment mounts the host AWS configuration read-only. The Airflow worker can read an existing SSO cache, but it cannot renew or rewrite it. Refresh with `aws sso login --profile india-aqi-admin` on the host when the cached session expires. For unattended scheduled operation, use an AWS execution environment with an IAM role rather than a developer's SSO profile.

## Operational boundaries

- Local Docker Compose is a development/demo environment, not a production-hardened deployment.
- The DAG uses the host-mounted project tree; the Airflow image does not copy the data lake project code into the image.
- `data/` is mounted from the host so extracted pages and run-specific Parquet are shared across Celery tasks.
- The pipeline does not calculate official CPCB AQI. It preserves source measurements and builds snapshot comparison analytics.
- The first run of the production DAG is a manual validation; no recurring schedule is enabled.
