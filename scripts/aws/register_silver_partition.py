#!/usr/bin/env python3
"""Idempotently register a CPCB Silver ingestion_date partition in AWS Glue.

Before creating or reusing a partition, this script verifies that non-empty
Parquet objects exist at the expected S3 partition prefix and that every object
uses SSE-S3 encryption with the SHA-256 metadata written by the Silver uploader.

The script never uploads, deletes, or rewrites S3 data.
"""
from __future__ import annotations

import argparse
import copy
import re
import sys
from datetime import date
from typing import Any

import boto3
from botocore.exceptions import ClientError, ProfileNotFound

DEFAULT_BUCKET = "india-aqi-lake-933139321496-ap-south-1"
DEFAULT_REGION = "ap-south-1"
DEFAULT_PROFILE = "india-aqi-admin"
DEFAULT_DATABASE = "india_aqi_lake"
DEFAULT_TABLE = "cpcb_observation_snapshots"
PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify S3 Silver files and idempotently register one Glue partition."
    )
    parser.add_argument("--ingestion-date", required=True, help="Partition date in YYYY-MM-DD format.")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def validate_ingestion_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("--ingestion-date must be a valid YYYY-MM-DD date.") from exc
    if parsed.isoformat() != value:
        raise ValueError("--ingestion-date must use canonical YYYY-MM-DD format.")
    return value


def inspect_partition_objects(s3: Any, bucket: str, prefix: str) -> tuple[int, int]:
    """Require a non-empty, checksum-tagged Parquet inventory for the partition."""
    paginator = s3.get_paginator("list_objects_v2")
    parquet_objects: list[dict[str, Any]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item.get("Key", ""))
            if key.lower().endswith(".parquet"):
                parquet_objects.append(item)

    if not parquet_objects:
        raise RuntimeError(
            f"No Parquet files found under s3://{bucket}/{prefix}. "
            "Upload and verify Silver before registering the Glue partition."
        )

    total_bytes = 0
    for item in parquet_objects:
        key = str(item["Key"])
        size = int(item.get("Size", 0))
        if size <= 0:
            raise RuntimeError(f"Empty Parquet object found: s3://{bucket}/{key}.")

        head = s3.head_object(Bucket=bucket, Key=key)
        if head.get("ContentLength") != size:
            raise RuntimeError(f"S3 size changed while inspecting s3://{bucket}/{key}.")
        if head.get("ServerSideEncryption") != "AES256":
            raise RuntimeError(f"Expected SSE-S3 encryption on s3://{bucket}/{key}.")

        metadata = {str(k).lower(): str(v) for k, v in (head.get("Metadata") or {}).items()}
        if not re.fullmatch(r"[0-9a-f]{64}", metadata.get("sha256", "")):
            raise RuntimeError(f"Missing valid SHA-256 object metadata on s3://{bucket}/{key}.")

        total_bytes += size

    return len(parquet_objects), total_bytes


def validate_glue_table(glue: Any, database: str, table_name: str, expected_location: str) -> dict[str, Any]:
    table = glue.get_table(DatabaseName=database, Name=table_name)["Table"]
    if table.get("TableType") != "EXTERNAL_TABLE":
        raise RuntimeError(f"{database}.{table_name} is not an EXTERNAL_TABLE.")

    descriptor = table.get("StorageDescriptor") or {}
    actual_location = str(descriptor.get("Location", "")).rstrip("/") + "/"
    if actual_location != expected_location:
        raise RuntimeError(
            f"Glue table location mismatch: expected {expected_location!r}, got {actual_location!r}."
        )

    partition_keys = table.get("PartitionKeys") or []
    if len(partition_keys) != 1 or str(partition_keys[0].get("Name", "")).lower() != "ingestion_date":
        raise RuntimeError(
            f"{database}.{table_name} must have exactly one partition key named ingestion_date."
        )

    if descriptor.get("InputFormat") != PARQUET_INPUT_FORMAT:
        raise RuntimeError("Glue table input format is not the expected Parquet input format.")
    if descriptor.get("OutputFormat") != PARQUET_OUTPUT_FORMAT:
        raise RuntimeError("Glue table output format is not the expected Parquet output format.")
    if not descriptor.get("Columns"):
        raise RuntimeError("Glue table has no data columns in its storage descriptor.")
    return table


def validate_partition_location(
    glue: Any,
    database: str,
    table_name: str,
    ingestion_date: str,
    expected_location: str,
    expected_table_descriptor: dict[str, Any],
) -> bool:
    try:
        partition = glue.get_partition(
            DatabaseName=database,
            TableName=table_name,
            PartitionValues=[ingestion_date],
        )["Partition"]
    except glue.exceptions.EntityNotFoundException:
        return False

    actual_location = str(
        (partition.get("StorageDescriptor") or {}).get("Location", "")
    ).rstrip("/") + "/"
    if actual_location != expected_location:
        raise RuntimeError(
            f"Partition {ingestion_date} points to {actual_location!r}; "
            f"expected {expected_location!r}."
        )

    partition_sd = partition.get("StorageDescriptor") or {}
    if partition_sd.get("InputFormat") != expected_table_descriptor.get("InputFormat"):
        raise RuntimeError(f"Partition {ingestion_date} has an unexpected Parquet input format.")
    if partition_sd.get("OutputFormat") != expected_table_descriptor.get("OutputFormat"):
        raise RuntimeError(f"Partition {ingestion_date} has an unexpected Parquet output format.")
    expected_columns = [
        (str(col.get("Name", "")).lower(), str(col.get("Type", "")).lower())
        for col in expected_table_descriptor.get("Columns", [])
    ]
    actual_columns = [
        (str(col.get("Name", "")).lower(), str(col.get("Type", "")).lower())
        for col in partition_sd.get("Columns", [])
    ]
    if actual_columns != expected_columns:
        raise RuntimeError(f"Partition {ingestion_date} has a schema that differs from its Glue table.")
    return True


def main() -> int:
    args = parse_args()
    try:
        ingestion_date = validate_ingestion_date(args.ingestion_date)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    table_location = f"s3://{args.bucket}/silver/{args.table}/"
    partition_location = f"{table_location}ingestion_date={ingestion_date}/"
    partition_prefix = f"silver/{args.table}/ingestion_date={ingestion_date}/"

    print("=" * 76)
    print("CPCB SILVER — GLUE PARTITION REGISTRATION")
    print("=" * 76)
    print(f"Target table : {args.database}.{args.table}")
    print(f"S3 partition : s3://{args.bucket}/{partition_prefix}")

    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        s3 = session.client("s3")
        glue = session.client("glue")

        object_count, total_bytes = inspect_partition_objects(
            s3, args.bucket, partition_prefix
        )
        table = validate_glue_table(
            glue, args.database, args.table, table_location
        )
        exists = validate_partition_location(
            glue,
            args.database,
            args.table,
            ingestion_date,
            partition_location,
            table.get("StorageDescriptor") or {},
        )
    except ProfileNotFound:
        print(f"ERROR: AWS profile not found: {args.profile}", file=sys.stderr)
        return 2
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        message = exc.response.get("Error", {}).get("Message", str(exc))
        print(f"AWS ERROR ({code}): {message}", file=sys.stderr)
        if code in {
            "ExpiredToken",
            "UnauthorizedException",
            "UnrecognizedClientException",
            "InvalidClientTokenId",
        }:
            print(f"Refresh the host session: aws sso login --profile {args.profile}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR during Glue pre-registration validation: {exc}", file=sys.stderr)
        return 1

    print(f"Parquet objects: {object_count}")
    print(f"Parquet bytes  : {total_bytes:,}")
    print(f"Glue table     : {table['Name']} (external Parquet)")
    print(f"Partition      : {'EXISTS AND MATCHES' if exists else 'NOT REGISTERED'}")

    if exists:
        print("Glue partition already exists and matches the verified S3 location.")
        return 0

    if args.dry_run:
        print(
            f"DRY RUN: would register ingestion_date={ingestion_date} "
            f"at {partition_location}."
        )
        return 0

    descriptor = copy.deepcopy(table["StorageDescriptor"])
    descriptor["Location"] = partition_location
    try:
        try:
            glue.create_partition(
                DatabaseName=args.database,
                TableName=args.table,
                PartitionInput={
                    "Values": [ingestion_date],
                    "StorageDescriptor": descriptor,
                    "Parameters": {
                        "classification": "parquet",
                        "compressionType": "snappy",
                    },
                },
            )
            action = "CREATED"
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code not in {"AlreadyExistsException", "AlreadyExists"}:
                raise
            action = "EXISTS_AFTER_CONCURRENT_CREATE"

        # Read-after-write is also the idempotent race-condition check.
        if not validate_partition_location(
            glue,
            args.database,
            args.table,
            ingestion_date,
            partition_location,
            table.get("StorageDescriptor") or {},
        ):
            raise RuntimeError("Glue partition was not visible after registration.")

        print("\nGLUE PARTITION REGISTRATION COMPLETE")
        print(f"Action     : {action}")
        print(f"Table      : {args.database}.{args.table}")
        print(f"Partition  : ingestion_date={ingestion_date}")
        print(f"S3 objects : {object_count}")
        print(f"Data size  : {total_bytes:,} bytes")
        print("S3 data    : NOT MODIFIED")
        return 0

    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        message = exc.response.get("Error", {}).get("Message", str(exc))
        print(f"AWS GLUE ERROR ({code}): {message}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR during Glue partition registration: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
