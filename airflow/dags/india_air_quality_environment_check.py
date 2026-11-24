"""Smoke-test the India Air Quality project's dedicated Airflow runtime.

This DAG performs read-only validation only. It does not call the CPCB API,
write to S3, modify Glue, or execute dbt models.
"""
from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

PROJECT_ROOT = Path(os.environ.get("INDIA_AQI_PROJECT_ROOT", "/opt/india-aqi"))
DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/home/airflow/.dbt"))

# Paths reflect the reorganised scripts/ directory. The former flat paths and
# legacy bronze_to_silver.py are intentionally not used by the active DAG.
REQUIRED_PROJECT_PATHS: dict[str, tuple[Path, str]] = {
    "CPCB extractor": (PROJECT_ROOT / "scripts/ingestion/profile_cpcb_full.py", "file"),
    "Bronze uploader": (PROJECT_ROOT / "scripts/ingestion/upload_bronze_dynamic.py", "file"),
    "Historical Silver builder": (PROJECT_ROOT / "scripts/transformation/build_historical_silver.py", "file"),
    "Incremental Silver uploader": (PROJECT_ROOT / "scripts/aws/upload_silver_incremental.py", "file"),
    "Glue partition registrar": (PROJECT_ROOT / "scripts/aws/register_silver_partition.py", "file"),
    "dbt project": (PROJECT_ROOT / "dbt/dbt_project.yml", "file"),
    "dbt models directory": (PROJECT_ROOT / "dbt/models", "dir"),
    "Project data directory": (PROJECT_ROOT / "data", "dir"),
    "dbt profiles": (DBT_PROFILES_DIR / "profiles.yml", "file"),
}


@dag(
    dag_id="india_air_quality_environment_check",
    description="Read-only validation of the India AQI Airflow runtime and project mounts.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["india-aqi", "infrastructure", "smoke-test"],
    default_args={"owner": "data-engineering", "retries": 0},
)
def india_air_quality_environment_check():
    @task(execution_timeout=timedelta(minutes=5))
    def validate_runtime() -> dict[str, Any]:
        import boto3
        import pyspark
        import requests
        from pyspark.sql import SparkSession

        java = subprocess.run(
            ["java", "-version"], check=True, capture_output=True, text=True
        )
        dbt = subprocess.run(
            [DBT_BIN, "--version"], check=True, capture_output=True, text=True
        )

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("india-aqi-airflow-smoke-test")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )
        try:
            spark_rows = spark.range(0, 5).count()
            if spark_rows != 5:
                raise AirflowFailException(
                    f"Unexpected Spark smoke-test count: {spark_rows}"
                )
            spark_version = spark.version
        finally:
            spark.stop()

        dbt_lines = [line.strip() for line in dbt.stdout.splitlines() if line.strip()]
        java_lines = (java.stderr or java.stdout).splitlines()
        return {
            "boto3_version": boto3.__version__,
            "requests_version": requests.__version__,
            "pyspark_version": pyspark.__version__,
            "spark_runtime_version": spark_version,
            "spark_smoke_test_rows": spark_rows,
            "dbt_cli": dbt_lines[-1] if dbt_lines else "available",
            "java": java_lines[0].strip() if java_lines else "available",
        }

    @task(execution_timeout=timedelta(minutes=2))
    def validate_project_mounts() -> dict[str, Any]:
        missing: list[str] = []
        validated: list[str] = []

        for description, (path, expected_type) in REQUIRED_PROJECT_PATHS.items():
            exists = path.is_file() if expected_type == "file" else path.is_dir()
            if not exists:
                missing.append(f"{description}: {path} (expected {expected_type})")
            else:
                validated.append(f"{description}: {path}")

        dbt_executable = Path(DBT_BIN)
        if not dbt_executable.is_file():
            missing.append(f"dbt executable: {dbt_executable}")
        else:
            validated.append(f"dbt executable: {dbt_executable}")

        if missing:
            raise AirflowFailException(
                "Project mount validation failed. Missing paths:\n - "
                + "\n - ".join(missing)
            )

        print("Validated project paths:\n - " + "\n - ".join(validated), flush=True)
        return {"project_root": str(PROJECT_ROOT), "validated_paths": len(validated)}

    @task(execution_timeout=timedelta(minutes=2))
    def validate_aws_sso() -> dict[str, Any]:
        import boto3

        profile = os.environ.get("AWS_PROFILE", "india-aqi-admin")
        region = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")
        session = boto3.Session(profile_name=profile, region_name=region)
        identity = session.client("sts").get_caller_identity()
        return {
            "aws_profile": profile,
            "region": region,
            "account": identity.get("Account"),
            "identity_check": "passed",
        }

    runtime = validate_runtime()
    mounts = validate_project_mounts()
    aws = validate_aws_sso()
    runtime >> mounts >> aws


india_air_quality_environment_check()
