#!/usr/bin/env python3
"""Register the CPCB historical Silver Parquet dataset in AWS Glue.

Creates a separate external Glue table:
  india_aqi_lake.cpcb_observation_snapshots

It does not modify the existing cpcb_observations table or any S3 data.
It validates the uploaded manifest, S3 Parquet inventory and checksums stored
in object metadata before creating the table and its ingestion_date partitions.

The operation is idempotent: a matching table/partition is left unchanged;
a conflicting definition or unexpected Parquet object causes a safe failure.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import boto3
from botocore.exceptions import ClientError, ProfileNotFound

DEFAULT_BUCKET = "india-aqi-lake-933139321496-ap-south-1"
DEFAULT_REGION = "ap-south-1"
DEFAULT_PROFILE = "india-aqi-admin"
DEFAULT_DATABASE = "india_aqi_lake"
DEFAULT_TABLE = "cpcb_observation_snapshots"
DEFAULT_BUILD_ID = "sha256-b40e4b49351ef553"
DATA_PREFIX = f"silver/{DEFAULT_TABLE}/"
METADATA_PREFIX = f"metadata/silver_runs/dataset={DEFAULT_TABLE}"

# Spark schema from build_historical_silver.py. ingestion_date is a Hive
# partition column, so it is deliberately excluded from StorageDescriptor.Columns.
COLUMNS = [
    {"Name": "extraction_id", "Type": "string"},
    {"Name": "retrieved_at_utc", "Type": "timestamp"},
    {"Name": "source_last_update_raw", "Type": "string"},
    {"Name": "country", "Type": "string"},
    {"Name": "state", "Type": "string"},
    {"Name": "city", "Type": "string"},
    {"Name": "station", "Type": "string"},
    {"Name": "candidate_station_key", "Type": "string"},
    {"Name": "candidate_entity_key", "Type": "string"},
    {"Name": "pollutant_id", "Type": "string"},
    {"Name": "latitude_raw", "Type": "string"},
    {"Name": "longitude_raw", "Type": "string"},
    {"Name": "latitude", "Type": "double"},
    {"Name": "longitude", "Type": "double"},
    {"Name": "coordinate_status", "Type": "string"},
    {"Name": "min_value_raw", "Type": "string"},
    {"Name": "max_value_raw", "Type": "string"},
    {"Name": "avg_value_raw", "Type": "string"},
    {"Name": "min_value", "Type": "double"},
    {"Name": "max_value", "Type": "double"},
    {"Name": "avg_value", "Type": "double"},
    {"Name": "measurement_status", "Type": "string"},
    {"Name": "source_record_hash", "Type": "string"},
    {"Name": "source_occurrence_count", "Type": "bigint"},
    {"Name": "duplicate_excess_count", "Type": "bigint"},
    {"Name": "source_pages", "Type": "array<string>"},
    {"Name": "source_page_count", "Type": "int"},
    {"Name": "ingested_at_utc", "Type": "timestamp"},
]
PARTITION_KEYS = [{"Name": "ingestion_date", "Type": "string"}]
INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Safely register the CPCB historical Silver Parquet dataset in AWS Glue."
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--build-id", default=DEFAULT_BUILD_ID)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate S3 and Glue state, but do not create the table or partitions.",
    )
    return parser.parse_args()


def s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def read_s3_json(s3: Any, bucket: str, key: str) -> dict[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        obj = json.loads(body.read().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid JSON at {s3_uri(bucket, key)}: {exc}") from exc
    finally:
        body.close()
    if not isinstance(obj, dict):
        raise RuntimeError(f"Expected a JSON object at {s3_uri(bucket, key)}.")
    return obj


def list_parquet_keys(s3: Any, bucket: str, prefix: str) -> set[str]:
    paginator = s3.get_paginator("list_objects_v2")
    result: set[str] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.lower().endswith(".parquet"):
                result.add(key)
    return result


def validate_manifest_and_objects(s3: Any, args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    manifest_key = f"{METADATA_PREFIX}/build_id={args.build_id}/manifest.json"
    manifest = read_s3_json(s3, args.bucket, manifest_key)

    if manifest.get("dataset") != args.table:
        raise RuntimeError(
            f"Manifest dataset mismatch: expected {args.table!r}, got {manifest.get('dataset')!r}."
        )
    if manifest.get("build_id") != args.build_id:
        raise RuntimeError(
            f"Manifest build_id mismatch: expected {args.build_id!r}, got {manifest.get('build_id')!r}."
        )

    expected_prefix = f"silver/{args.table}/"
    if manifest.get("data_prefix") != expected_prefix:
        raise RuntimeError(
            f"Manifest data_prefix mismatch: expected {expected_prefix!r}, got {manifest.get('data_prefix')!r}."
        )

    summary = manifest.get("summary") or {}
    row_count = summary.get("combined_unique_record_count")
    if row_count != 7324:
        raise RuntimeError(f"Unexpected manifest row count: expected 7,324, found {row_count!r}.")
    if summary.get("cross_extraction_deduplication_performed") is not False:
        raise RuntimeError("Manifest does not confirm cross-extraction deduplication was disabled.")

    files = manifest.get("files")
    if not isinstance(files, list) or len(files) != 2:
        raise RuntimeError(f"Expected exactly two Parquet files in manifest; found {len(files) if isinstance(files, list) else 'invalid inventory'}.")

    expected_keys: set[str] = set()
    partitions: dict[str, dict[str, Any]] = {}
    for item in files:
        if not isinstance(item, dict):
            raise RuntimeError("Manifest files inventory contains a non-object entry.")
        key = item.get("s3_key")
        digest = item.get("sha256")
        size = item.get("size_bytes")
        part = item.get("partition") or {}
        partition_value = part.get("ingestion_date")
        if not isinstance(key, str) or not key.startswith(expected_prefix) or not key.endswith(".parquet"):
            raise RuntimeError(f"Invalid Parquet key in manifest: {key!r}.")
        if not isinstance(digest, str) or len(digest) != 64:
            raise RuntimeError(f"Missing/invalid SHA-256 in manifest for {key}.")
        if not isinstance(size, int) or size <= 0:
            raise RuntimeError(f"Missing/invalid size_bytes in manifest for {key}.")
        if not isinstance(partition_value, str) or len(partition_value) != 10:
            raise RuntimeError(f"Missing/invalid ingestion_date partition for {key}.")
        if f"ingestion_date={partition_value}/" not in key:
            raise RuntimeError(f"Partition path does not match manifest for {key}.")
        expected_keys.add(key)
        partitions.setdefault(partition_value, {"file_count": 0, "location": f"s3://{args.bucket}/{expected_prefix}ingestion_date={partition_value}/"})
        partitions[partition_value]["file_count"] += 1

        head = s3.head_object(Bucket=args.bucket, Key=key)
        if head.get("ContentLength") != size:
            raise RuntimeError(f"S3 object size differs from manifest for {s3_uri(args.bucket, key)}.")
        metadata = {str(k).lower(): v for k, v in (head.get("Metadata") or {}).items()}
        if metadata.get("sha256") != digest:
            raise RuntimeError(
                f"S3 object SHA-256 metadata differs from manifest for {s3_uri(args.bucket, key)}."
            )
        if head.get("ServerSideEncryption") != "AES256":
            raise RuntimeError(f"Expected SSE-S3 encryption on {s3_uri(args.bucket, key)}.")

    if sorted(partitions) != ["2026-11-16", "2026-11-20"]:
        raise RuntimeError(f"Unexpected partition values: {sorted(partitions)}")

    actual_keys = list_parquet_keys(s3, args.bucket, expected_prefix)
    if actual_keys != expected_keys:
        missing = sorted(expected_keys - actual_keys)
        extra = sorted(actual_keys - expected_keys)
        raise RuntimeError(
            "S3 Parquet inventory does not match the manifest. "
            f"Missing={missing[:5]}, extra={extra[:5]}"
        )

    return manifest, [partitions[key] | {"value": key} for key in sorted(partitions)], manifest_key


def storage_descriptor(location: str) -> dict[str, Any]:
    return {
        "Columns": [dict(column) for column in COLUMNS],
        "Location": location,
        "InputFormat": INPUT_FORMAT,
        "OutputFormat": OUTPUT_FORMAT,
        "Compressed": True,
        "NumberOfBuckets": -1,
        "SerdeInfo": {
            "SerializationLibrary": SERDE,
            "Parameters": {"serialization.format": "1"},
        },
        "StoredAsSubDirectories": False,
        "Parameters": {"classification": "parquet", "compressionType": "snappy"},
    }


def normalize_columns(columns: list[dict[str, Any]] | None) -> list[tuple[str, str]]:
    return [(str(col.get("Name", "")).lower(), str(col.get("Type", "")).lower()) for col in (columns or [])]


def validate_existing_table(glue: Any, database: str, table: str, expected_location: str) -> bool:
    try:
        response = glue.get_table(DatabaseName=database, Name=table)
    except glue.exceptions.EntityNotFoundException:
        return False

    existing = response["Table"]
    sd = existing.get("StorageDescriptor") or {}
    if existing.get("TableType") != "EXTERNAL_TABLE":
        raise RuntimeError(f"Existing Glue table {database}.{table} is not an EXTERNAL_TABLE.")
    if sd.get("Location", "").rstrip("/") != expected_location.rstrip("/"):
        raise RuntimeError(
            f"Existing Glue table location conflicts with expected location: {sd.get('Location')!r}."
        )
    if normalize_columns(sd.get("Columns")) != normalize_columns(COLUMNS):
        raise RuntimeError("Existing Glue table columns/types differ from the expected Silver schema; refusing to alter it.")
    if normalize_columns(existing.get("PartitionKeys")) != normalize_columns(PARTITION_KEYS):
        raise RuntimeError("Existing Glue table partition keys differ from expected ingestion_date; refusing to alter it.")
    if sd.get("InputFormat") != INPUT_FORMAT or sd.get("OutputFormat") != OUTPUT_FORMAT:
        raise RuntimeError("Existing Glue table is not configured with the expected Parquet formats.")
    return True


def table_input(args: argparse.Namespace) -> dict[str, Any]:
    location = f"s3://{args.bucket}/silver/{args.table}/"
    return {
        "Name": args.table,
        "Description": "Extraction-aware CPCB historical Silver observations; one row per extraction and exact source record.",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "EXTERNAL": "TRUE",
            "classification": "parquet",
            "compressionType": "snappy",
            "source": "CPCB / data.gov.in real-time air quality observations",
            "historical_bootstrap_build_id": args.build_id,
        },
        "StorageDescriptor": storage_descriptor(location),
        "PartitionKeys": [dict(key) for key in PARTITION_KEYS],
    }


def ensure_partition(glue: Any, args: argparse.Namespace, partition: dict[str, Any], dry_run: bool) -> str:
    value = partition["value"]
    expected_location = partition["location"]
    try:
        existing = glue.get_partition(
            DatabaseName=args.database,
            TableName=args.table,
            PartitionValues=[value],
        )["Partition"]
    except glue.exceptions.EntityNotFoundException:
        if dry_run:
            return "WOULD_CREATE"
        glue.create_partition(
            DatabaseName=args.database,
            TableName=args.table,
            PartitionInput={
                "Values": [value],
                "StorageDescriptor": storage_descriptor(expected_location),
                "Parameters": {"classification": "parquet", "compressionType": "snappy"},
            },
        )
        return "CREATED"

    actual_location = (existing.get("StorageDescriptor") or {}).get("Location", "")
    if actual_location.rstrip("/") != expected_location.rstrip("/"):
        raise RuntimeError(
            f"Existing partition {value} points to {actual_location!r}, expected {expected_location!r}."
        )
    if normalize_columns((existing.get("StorageDescriptor") or {}).get("Columns")) != normalize_columns(COLUMNS):
        raise RuntimeError(f"Existing partition {value} has a different Parquet schema.")
    return "EXISTS_MATCHING"


def main() -> int:
    args = parse_args()
    manifest_key = f"{METADATA_PREFIX}/build_id={args.build_id}/manifest.json"
    expected_table_location = f"s3://{args.bucket}/silver/{args.table}/"

    print("=" * 78)
    print("CPCB HISTORICAL SILVER — GLUE REGISTRATION")
    print("=" * 78)
    print(f"AWS profile : {args.profile}")
    print(f"AWS region  : {args.region}")
    print(f"Glue target : {args.database}.{args.table}")
    print(f"S3 location : {expected_table_location}")
    print(f"Build ID    : {args.build_id}")

    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        s3 = session.client("s3")
        glue = session.client("glue")
        s3.head_bucket(Bucket=args.bucket)
        glue.get_database(Name=args.database)
        manifest, partitions, manifest_key = validate_manifest_and_objects(s3, args)
    except ProfileNotFound:
        print(f"ERROR: AWS profile not found: {args.profile}", file=sys.stderr)
        return 2
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        message = exc.response.get("Error", {}).get("Message", str(exc))
        print(f"AWS ERROR ({code}): {message}", file=sys.stderr)
        if code in {"ExpiredToken", "UnauthorizedException", "UnrecognizedClientException"}:
            print(f"Run: aws sso login --profile {args.profile}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"ERROR during pre-registration validation: {exc}", file=sys.stderr)
        return 2

    print(f"Manifest    : s3://{args.bucket}/{manifest_key}")
    print(f"Rows        : {manifest['summary']['combined_unique_record_count']:,}")
    print(f"Parquet files: {len(manifest['files'])}")
    print("S3 inventory: exact match; object sizes, SHA-256 metadata and SSE-S3 verified")
    print("Partitions  : " + ", ".join(p["value"] for p in partitions))

    try:
        table_exists = validate_existing_table(glue, args.database, args.table, expected_table_location)
        print(f"Glue table  : {'EXISTS AND MATCHES' if table_exists else 'NOT YET CREATED'}")
        for part in partitions:
            status = ensure_partition(glue, args, part, dry_run=True)
            print(f"Partition {part['value']}: {status}")
    except Exception as exc:
        print(f"ERROR: Existing Glue catalog state is conflicting: {exc}", file=sys.stderr)
        return 2

    if args.dry_run:
        print("DRY RUN: Glue table and partitions were not modified.")
        if not table_exists:
            print(f"Would create {args.database}.{args.table} as an external Parquet table.")
        return 0

    try:
        if not table_exists:
            glue.create_table(DatabaseName=args.database, TableInput=table_input(args))
            print(f"Created Glue table: {args.database}.{args.table}")
        else:
            print(f"Reusing matching Glue table: {args.database}.{args.table}")

        created = existing = 0
        for partition in partitions:
            result = ensure_partition(glue, args, partition, dry_run=False)
            if result == "CREATED":
                created += 1
            else:
                existing += 1
            print(f"Partition {partition['value']}: {result}")

        # Final read-after-write validation of table and all expected partitions.
        validate_existing_table(glue, args.database, args.table, expected_table_location)
        for partition in partitions:
            result = ensure_partition(glue, args, partition, dry_run=True)
            if result not in {"EXISTS_MATCHING"}:
                raise RuntimeError(f"Partition {partition['value']} was not visible after registration.")

        print("\n" + "=" * 78)
        print("GLUE REGISTRATION COMPLETE")
        print("=" * 78)
        print(f"Table       : {args.database}.{args.table}")
        print(f"Table type  : EXTERNAL_TABLE (Parquet / Snappy)")
        print(f"Rows in S3  : {manifest['summary']['combined_unique_record_count']:,}")
        print(f"Partitions created : {created}")
        print(f"Partitions reused  : {existing}")
        print("Existing cpcb_observations table: NOT MODIFIED")
        print("S3 Parquet data: NOT MODIFIED")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        message = exc.response.get("Error", {}).get("Message", str(exc))
        print(f"AWS ERROR during Glue registration ({code}): {message}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR during Glue registration: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
