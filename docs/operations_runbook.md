# Operations runbook

## Start the dedicated Airflow environment

```powershell
cd "C:\Data Engineer\india-air-quality-data-lake\airflow"
aws sso login --profile india-aqi-admin
docker compose config --quiet
docker compose up -d
docker compose ps
```

UI: `http://localhost:8081`.

First-time database initialization only:

```powershell
docker compose up airflow-init
```

## Run the pipeline

1. Confirm the host AWS SSO session is valid.
2. Confirm `DATA_GOV_API_KEY` is set in the ignored local `airflow/.env`.
3. Confirm Airflow services are healthy.
4. Trigger `india_air_quality_pipeline` manually in the UI.
5. Watch task order: `preflight` → `extract_cpcb_snapshot` → `upload_bronze` → `build_silver` → `upload_silver` → `register_glue_partition` → `run_dbt_build`.
6. Confirm every task is successful.
7. Verify the new extraction ID, S3 objects, Glue partition and dbt test summary.

The DAG is intentionally unscheduled until API policy, endpoint refresh behaviour and historical availability are validated.

## Useful commands

```powershell
# From airflow/
docker compose ps
docker compose logs --tail=150 airflow-worker
docker compose logs --tail=150 airflow-scheduler
docker compose logs --tail=150 airflow-dag-processor

docker compose exec airflow-dag-processor airflow dags list-import-errors
docker compose exec airflow-scheduler airflow tasks list india_air_quality_pipeline
```

## Validate dbt parser in Linux worker

```powershell
# From airflow/
docker compose exec airflow-worker /opt/dbt-venv/bin/dbt parse `
  --project-dir /opt/india-aqi/dbt `
  --profiles-dir /home/airflow/.dbt `
  --target dev `
  --target-path /tmp/dbt-target-manual `
  --no-partial-parse
```

Do not reuse the Windows-created `dbt/target` cache in Linux. Use an isolated Linux `--target-path` and disable partial parsing.

## Failure recovery

- Identify the first failed task and inspect its log.
- Record the DAG run ID and extraction ID.
- Verify whether the failed task already wrote output.
- Retry/clear only the failed task when upstream data is verified and the operation is idempotent.
- Do not clear successful extraction/Bronze/Silver tasks merely to retry dbt.
- Never use global Docker volume/prune commands to troubleshoot this deployment.

## Credential hygiene

- Keep `airflow/.env` out of Git.
- Refresh SSO on the host with `aws sso login --profile india-aqi-admin`.
- Never log or paste API key/password values.
- For unattended production, replace human SSO dependency with a workload identity/role and a managed secret mechanism.
