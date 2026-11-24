# India AQI Airflow - dbt cache compatibility fix

## Why
The dbt project is bind-mounted from Windows into a Linux Airflow worker. The
Windows-generated `dbt/target/partial_parse.msgpack` contains file IDs using
Windows separators. Reusing that cache on Linux triggered a dbt parser KeyError.

## Change
Replace `airflow/dags/india_air_quality_pipeline.py` with the file in this patch.
The dbt task now uses an isolated Linux-only target directory (`/tmp/dbt-target-airflow`)
and disables partial parsing, preventing host/container cache mixing. It also
uses the current Airflow 3 SDK exception import.

## Apply
Extract the ZIP at the repository root, then restart the DAG processor and scheduler:

```powershell
docker compose restart airflow-dag-processor airflow-scheduler
```

## Validate dbt parse in the worker
From the `airflow` directory:

```powershell
docker compose exec airflow-worker /opt/dbt-venv/bin/dbt parse `
  --project-dir /opt/india-aqi/dbt `
  --profiles-dir /home/airflow/.dbt `
  --target dev `
  --target-path /tmp/dbt-target-airflow `
  --no-partial-parse
```

If parse succeeds, clear **only** the failed `run_dbt_build` task instance in the
Airflow UI. Do not rerun the whole DAG: extraction, Bronze, Silver and Glue tasks
already succeeded and rerunning them would create another extraction.
