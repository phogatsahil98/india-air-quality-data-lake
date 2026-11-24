"""End-to-end CPCB snapshot ingestion and analytics DAG.

This DAG is intentionally manual (schedule=None) until API polling frequency,
source refresh behavior and account/API quota are validated. It uses the
project's existing extraction + Bronze + PySpark build scripts, then the
incremental Silver uploader/Glue partition registrar and the existing dbt
project. Each task is retry-safe for the same extraction ID.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import dag, task

PROJECT_ROOT = Path("/opt/india-aqi")
AIRFLOW_ROOT = Path("/opt/airflow")
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
SCRIPTS = PROJECT_ROOT / "scripts"
SCRIPT_PATHS = {
    "extract": SCRIPTS / "ingestion" / "profile_cpcb_full.py",
    "upload_bronze": SCRIPTS / "ingestion" / "upload_bronze_dynamic.py",
    "build_silver": SCRIPTS / "transformation" / "build_historical_silver.py",
    "upload_silver": SCRIPTS / "aws" / "upload_silver_incremental.py",
    "register_glue_partition": SCRIPTS / "aws" / "register_silver_partition.py",
}
DBT_PROJECT = PROJECT_ROOT / "dbt"
DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/home/airflow/.dbt")
DBT_TARGET_PATH = os.environ.get("DBT_TARGET_PATH", "/tmp/dbt-target-airflow")
AWS_PROFILE = os.environ.get("AWS_PROFILE", "india-aqi-admin")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")
BUCKET = "india-aqi-lake-933139321496-ap-south-1"
DATABASE = "india_aqi_lake"


def _run_command(command: list[str], *, cwd: Path, timeout_seconds: int = 7200) -> str:
    """Run a project command, stream captured output into task logs, fail clearly."""
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        timeout=timeout_seconds,
        check=False,
    )
    output = completed.stdout or ""
    if output:
        print(output, flush=True)
    if completed.returncode != 0:
        tail = "\n".join(output.splitlines()[-40:])
        raise AirflowFailException(
            f"Command failed with exit code {completed.returncode}: {' '.join(command)}\n{tail}"
        )
    return output


def _safe_extraction_id(value: str) -> str:
    if not re.fullmatch(r"\d{8}T\d{6}Z", value):
        raise AirflowFailException(f"Invalid extraction ID detected: {value!r}")
    datetime.strptime(value, "%Y%m%dT%H%M%SZ")
    return value


@dag(
    dag_id="india_air_quality_pipeline",
    description="Extract CPCB data, land immutable Bronze, build historical Silver, register Glue partition and run dbt.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["india-aqi", "cpcb", "s3", "pyspark", "glue", "dbt"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
)
def india_air_quality_pipeline():
    @task(execution_timeout=timedelta(minutes=5), retries=0)
    def preflight() -> dict[str, Any]:
        """Fail fast before making API calls if secrets, mounts or AWS access are missing."""
        api_key = os.environ.get("DATA_GOV_API_KEY", "").strip()
        if not api_key:
            raise AirflowFailException(
                "DATA_GOV_API_KEY is missing. Add it to airflow/.env and recreate Airflow services."
            )

        required = [
            SCRIPT_PATHS["extract"],
            SCRIPT_PATHS["upload_bronze"],
            SCRIPT_PATHS["build_silver"],
            SCRIPT_PATHS["upload_silver"],
            SCRIPT_PATHS["register_glue_partition"],
            DBT_PROJECT / "dbt_project.yml",
            Path(DBT_PROFILES_DIR) / "profiles.yml",
        ]
        missing = [str(path) for path in required if not path.exists()]
        if missing:
            raise AirflowFailException("Required project files are not mounted: " + ", ".join(missing))
        if not Path(DBT_BIN).is_file():
            raise AirflowFailException(f"dbt executable not found: {DBT_BIN}")

        import boto3

        session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
        identity = session.client("sts").get_caller_identity()
        session.client("s3").head_bucket(Bucket=BUCKET)
        session.client("glue").get_database(Name=DATABASE)

        print(f"AWS identity validated for account {identity.get('Account')} in {AWS_REGION}.")
        print("CPCB API key is configured (value intentionally not logged).")
        print(f"Validated {len(required)} project/runtime paths.")
        return {"aws_account": identity.get("Account"), "region": AWS_REGION, "preflight": "passed"}

    @task(execution_timeout=timedelta(hours=1), retries=1, retry_delay=timedelta(minutes=5))
    def extract_cpcb_snapshot() -> str:
        """Run the existing full CPCB API extractor and return its new extraction ID."""
        extractor = SCRIPT_PATHS["extract"]
        RAW_ROOT.mkdir(parents=True, exist_ok=True)
        before = {path.name for path in RAW_ROOT.glob("source_profile_*") if path.is_dir()}

        _run_command([sys.executable, str(extractor)], cwd=PROJECT_ROOT, timeout_seconds=3600)

        after_paths = [
            path for path in RAW_ROOT.glob("source_profile_*")
            if path.is_dir() and path.name not in before
        ]
        if len(after_paths) != 1:
            names = [path.name for path in after_paths]
            raise AirflowFailException(
                "Expected exactly one new source_profile_<EXTRACTION_ID> directory from the extractor; "
                f"found {len(after_paths)}: {names}. Check the ingestion/profile_cpcb_full.py output and data/raw."
            )
        run_dir = after_paths[0]
        extraction_id = _safe_extraction_id(run_dir.name.removeprefix("source_profile_"))
        pages = list(run_dir.rglob("*.json"))
        if not pages:
            raise AirflowFailException(f"Extractor created {run_dir} but no JSON pages were found.")
        print(f"Extracted {len(pages)} JSON pages into {run_dir} (extraction_id={extraction_id}).")
        return extraction_id

    @task(execution_timeout=timedelta(minutes=45))
    def upload_bronze(extraction_id: str) -> str:
        _safe_extraction_id(extraction_id)
        _run_command(
            [
                sys.executable,
                str(SCRIPT_PATHS["upload_bronze"]),
                "--extraction-id", extraction_id,
                "--source-root", "data/raw",
                "--bucket", BUCKET,
                "--region", AWS_REGION,
                "--profile", AWS_PROFILE,
            ],
            cwd=PROJECT_ROOT,
            timeout_seconds=2700,
        )
        return extraction_id

    @task(execution_timeout=timedelta(hours=2))
    def build_silver(extraction_id: str) -> dict[str, str]:
        _safe_extraction_id(extraction_id)
        output_dir = (
            PROJECT_ROOT / "data" / "processed" / "airflow_runs" / extraction_id
            / "cpcb_observation_snapshots"
        )
        cache_dir = PROJECT_ROOT / "data" / "bronze_cache" / "airflow"
        _run_command(
            [
                sys.executable,
                str(SCRIPT_PATHS["build_silver"]),
                "--extraction-ids", extraction_id,
                "--bucket", BUCKET,
                "--region", AWS_REGION,
                "--profile", AWS_PROFILE,
                "--cache-dir", str(cache_dir),
                "--output-dir", str(output_dir),
                "--overwrite-output",
            ],
            cwd=PROJECT_ROOT,
            timeout_seconds=7200,
        )
        report = output_dir.parent / "cpcb_observation_snapshots_quality_report.json"
        if not output_dir.exists() or not report.is_file():
            raise AirflowFailException("PySpark did not produce the expected Silver output and quality report.")
        return {"dataset_dir": str(output_dir), "quality_report": str(report)}

    @task(execution_timeout=timedelta(minutes=60))
    def upload_silver(extraction_id: str, paths: dict[str, str]) -> str:
        _safe_extraction_id(extraction_id)
        _run_command(
            [
                sys.executable,
                str(SCRIPT_PATHS["upload_silver"]),
                "--extraction-id", extraction_id,
                "--dataset-dir", paths["dataset_dir"],
                "--quality-report", paths["quality_report"],
                "--bucket", BUCKET,
                "--region", AWS_REGION,
                "--profile", AWS_PROFILE,
            ],
            cwd=PROJECT_ROOT,
            timeout_seconds=3600,
        )
        return extraction_id

    @task(execution_timeout=timedelta(minutes=15))
    def register_glue_partition(extraction_id: str) -> str:
        _safe_extraction_id(extraction_id)
        ingestion_date = datetime.strptime(extraction_id, "%Y%m%dT%H%M%SZ").date().isoformat()
        _run_command(
            [
                sys.executable,
                str(SCRIPT_PATHS["register_glue_partition"]),
                "--ingestion-date", ingestion_date,
                "--bucket", BUCKET,
                "--region", AWS_REGION,
                "--profile", AWS_PROFILE,
                "--database", DATABASE,
                "--table", "cpcb_observation_snapshots",
            ],
            cwd=PROJECT_ROOT,
            timeout_seconds=900,
        )
        return ingestion_date

    @task(execution_timeout=timedelta(hours=1))
    def run_dbt_build(ingestion_date: str) -> dict[str, str]:
        # Keep dbt's generated state inside the Linux worker container. The dbt
        # project itself is bind-mounted from Windows, so sharing dbt/target
        # would mix Windows and Linux file IDs in partial_parse.msgpack.
        output = _run_command(
            [
                DBT_BIN,
                "build",
                "--project-dir", str(DBT_PROJECT),
                "--profiles-dir", DBT_PROFILES_DIR,
                "--target", "dev",
                "--target-path", DBT_TARGET_PATH,
                "--no-partial-parse",
                "--fail-fast",
            ],
            cwd=DBT_PROJECT,
            timeout_seconds=3600,
        )
        print(f"dbt build completed after registering ingestion_date={ingestion_date}.")
        return {"status": "success", "ingestion_date": ingestion_date, "last_output_line": output.splitlines()[-1] if output.splitlines() else "dbt build completed"}

    ready = preflight()
    extraction_id = extract_cpcb_snapshot()
    ready >> extraction_id
    bronze_id = upload_bronze(extraction_id)
    silver_paths = build_silver(bronze_id)
    uploaded_id = upload_silver(bronze_id, silver_paths)
    ingestion_date = register_glue_partition(uploaded_id)
    run_dbt_build(ingestion_date)


india_air_quality_pipeline()
