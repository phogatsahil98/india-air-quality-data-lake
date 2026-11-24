"""Small, synchronous Amazon Athena client for the Streamlit dashboard."""
from __future__ import annotations

import os
import time
from typing import Any

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError, ProfileNotFound


class AthenaQueryError(RuntimeError):
    """Raised when an Athena query fails or times out."""


def _get_settings() -> dict[str, Any]:
    return {
        "profile": os.getenv("AWS_PROFILE", "india-aqi-admin").strip(),
        "region": os.getenv("AWS_REGION", "ap-south-1").strip(),
        "workgroup": os.getenv("ATHENA_WORKGROUP", "india-aqi-workgroup").strip(),
        "catalog": os.getenv("ATHENA_CATALOG", "AwsDataCatalog").strip(),
        "database": os.getenv("ATHENA_DATABASE", "india_aqi_dbt_dev").strip(),
        "timeout": int(os.getenv("ATHENA_QUERY_TIMEOUT_SECONDS", "240")),
        "poll_interval": float(os.getenv("ATHENA_POLL_INTERVAL_SECONDS", "2")),
    }


def _client():
    settings = _get_settings()
    try:
        session = boto3.Session(
            profile_name=settings["profile"],
            region_name=settings["region"],
        )
        return session.client("athena"), settings
    except ProfileNotFound as exc:
        raise AthenaQueryError(
            f"AWS profile '{settings['profile']}' was not found. "
            "Check your local AWS config and set AWS_PROFILE if needed."
        ) from exc
    except (BotoCoreError, ClientError) as exc:
        raise AthenaQueryError(f"Could not initialise the AWS Athena client: {exc}") from exc


def _cell_value(cell: dict[str, Any]) -> str | None:
    return cell.get("VarCharValue")


def _results_to_dataframe(client, query_execution_id: str) -> pd.DataFrame:
    paginator = client.get_paginator("get_query_results")
    column_names: list[str] | None = None
    rows: list[list[str | None]] = []

    for page_number, page in enumerate(
        paginator.paginate(QueryExecutionId=query_execution_id, PaginationConfig={"PageSize": 1000})
    ):
        result_set = page.get("ResultSet", {})
        if column_names is None:
            metadata = result_set.get("ResultSetMetadata", {}).get("ColumnInfo", [])
            column_names = [column.get("Name", f"column_{index}") for index, column in enumerate(metadata)]

        page_rows = result_set.get("Rows", [])
        if page_number == 0 and page_rows:
            # Athena returns the column header as the first row of the first page.
            page_rows = page_rows[1:]

        for row in page_rows:
            data = row.get("Data", [])
            values = [_cell_value(cell) for cell in data]
            if len(values) < len(column_names or []):
                values.extend([None] * (len(column_names or []) - len(values)))
            rows.append(values[: len(column_names or [])])

    return pd.DataFrame(rows, columns=column_names or [])


def run_athena_query(sql: str) -> tuple[pd.DataFrame, str]:
    """Execute SQL in the configured workgroup and return its DataFrame and query ID."""
    if not sql or not sql.strip():
        raise AthenaQueryError("The dashboard received an empty SQL query.")

    client, settings = _client()
    try:
        started = client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={
                "Catalog": settings["catalog"],
                "Database": settings["database"],
            },
            WorkGroup=settings["workgroup"],
        )
        query_id = started["QueryExecutionId"]

        deadline = time.monotonic() + settings["timeout"]
        while time.monotonic() < deadline:
            response = client.get_query_execution(QueryExecutionId=query_id)
            status = response["QueryExecution"]["Status"]
            state = status.get("State")

            if state == "SUCCEEDED":
                return _results_to_dataframe(client, query_id), query_id
            if state in {"FAILED", "CANCELLED"}:
                reason = status.get("StateChangeReason", "No failure reason was returned.")
                raise AthenaQueryError(f"Athena query {state.lower()}: {reason}\nQuery ID: {query_id}")

            time.sleep(settings["poll_interval"])

        try:
            client.stop_query_execution(QueryExecutionId=query_id)
        except (BotoCoreError, ClientError):
            pass
        raise AthenaQueryError(
            f"Athena query exceeded the {settings['timeout']} second timeout. Query ID: {query_id}"
        )
    except AthenaQueryError:
        raise
    except (BotoCoreError, ClientError, KeyError) as exc:
        raise AthenaQueryError(
            f"Athena request failed: {exc}. If your AWS IAM Identity Center session expired, "
            "run 'aws sso login --profile india-aqi-admin' in PowerShell and refresh the dashboard."
        ) from exc
