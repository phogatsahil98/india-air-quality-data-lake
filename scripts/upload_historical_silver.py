#!/usr/bin/env python3
"""Upload the local extraction-aware CPCB historical Silver Parquet dataset to S3.

This is a guarded bootstrap uploader for:
  data/processed/historical_silver/cpcb_observation_snapshots/

Data files are uploaded under:
  s3://<bucket>/silver/cpcb_observation_snapshots/ingestion_date=YYYY-MM-DD/...

The Glue table location should point at the Silver dataset prefix above. Build
manifest and quality report are stored OUTSIDE that table location under:
  s3://<bucket>/metadata/silver_runs/dataset=cpcb_observation_snapshots/build_id=<hash>/

Safety properties:
- validates the quality report and local Parquet row count using Spark;
- uploads only Parquet data files (not _SUCCESS or local metadata);
- calculates SHA-256 locally and re-reads each uploaded object to verify it;
- uses SSE-S3 encryption;
- skips identical existing files and refuses conflicting objects;
- refuses unexpected existing Parquet objects in the target prefix;
- writes the manifest last, after every Parquet file is verified;
- does not create/modify Glue tables or touch the existing cpcb_observations table.

This bootstrap script intentionally refuses a different existing manifest. A
separate incremental uploader should be designed before adding future runs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError, ProfileNotFound

DEFAULT_BUCKET = "india-aqi-lake-933139321496-ap-south-1"
DEFAULT_REGION = "ap-south-1"
DEFAULT_PROFILE = "india-aqi-admin"
DATASET_NAME = "cpcb_observation_snapshots"
DEFAULT_LOCAL_DATASET = Path("data/processed/historical_silver/cpcb_observation_snapshots")
DEFAULT_QUALITY_REPORT = Path(
    "data/processed/historical_silver/cpcb_observation_snapshots_quality_report.json"
)
DATA_PREFIX = f"silver/{DATASET_NAME}/"
METADATA_ROOT = f"metadata/silver_runs/dataset={DATASET_NAME}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Upload validated historical CPCB Silver Parquet data to S3.")
    p.add_argument("--dataset-dir", type=Path, default=DEFAULT_LOCAL_DATASET)
    p.add_argument("--quality-report", type=Path, default=DEFAULT_QUALITY_REPORT)
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--region", default=DEFAULT_REGION)
    p.add_argument("--profile", default=DEFAULT_PROFILE)
    p.add_argument("--dry-run", action="store_true", help="Validate and display planned S3 uploads without writing.")
    p.add_argument(
        "--skip-spark-validation",
        action="store_true",
        help="Skip Spark row-count/schema validation (not recommended).",
    )
    return p.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read valid JSON from {path}: {exc}") from exc
    if not isinstance(obj, dict):
        raise RuntimeError(f"Expected a JSON object in {path}.")
    return obj


def is_not_found(exc: ClientError) -> bool:
    code = str(exc.response.get("Error", {}).get("Code", ""))
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


def object_exists(s3: Any, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if is_not_found(exc):
            return False
        raise


def remote_sha256(s3: Any, bucket: str, key: str) -> str:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    h = hashlib.sha256()
    try:
        for chunk in body.iter_chunks(chunk_size=1024 * 1024):
            if chunk:
                h.update(chunk)
    finally:
        body.close()
    return h.hexdigest()


def read_remote_json(s3: Any, bucket: str, key: str) -> dict[str, Any] | None:
    if not object_exists(s3, bucket, key):
        return None
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        payload = json.loads(body.read().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unreadable remote JSON: s3://{bucket}/{key}: {exc}") from exc
    finally:
        body.close()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Remote JSON is not an object: s3://{bucket}/{key}")
    return payload


def validate_local(args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    if not args.dataset_dir.is_dir():
        raise RuntimeError(f"Silver dataset directory not found: {args.dataset_dir}")
    if not args.quality_report.is_file():
        raise RuntimeError(f"Silver quality report not found: {args.quality_report}")

    report = load_json(args.quality_report)
    if report.get("dataset") != DATASET_NAME:
        raise RuntimeError(
            f"Quality report dataset mismatch: expected {DATASET_NAME!r}, got {report.get('dataset')!r}."
        )
    if report.get("cross_extraction_deduplication_performed") is not False:
        raise RuntimeError("Quality report does not confirm cross-extraction deduplication was disabled.")

    parquet_files = sorted(p for p in args.dataset_dir.rglob("*.parquet") if p.is_file())
    if not parquet_files:
        raise RuntimeError(f"No Parquet files found under {args.dataset_dir}.")

    inventory: list[dict[str, Any]] = []
    seen_partitions: set[str] = set()
    for path in parquet_files:
        rel = path.relative_to(args.dataset_dir).as_posix()
        match = re.search(r"(?:^|/)ingestion_date=(\d{4}-\d{2}-\d{2})/", rel)
        if not match:
            raise RuntimeError(
                f"Parquet file is not under an ingestion_date=YYYY-MM-DD partition: {rel}"
            )
        partition_value = match.group(1)
        seen_partitions.add(partition_value)
        digest = sha256_file(path)
        inventory.append({
            "local_relative_path": rel,
            "s3_key": DATA_PREFIX + rel,
            "partition": {"ingestion_date": partition_value},
            "size_bytes": path.stat().st_size,
            "sha256": digest,
        })

    total_bytes = sum(item["size_bytes"] for item in inventory)
    expected_rows = int(report.get("combined_unique_record_count", -1))
    if expected_rows < 0:
        raise RuntimeError("Quality report is missing combined_unique_record_count.")

    spark_validation: dict[str, Any] = {"performed": False}
    if not args.skip_spark_validation:
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise RuntimeError(
                "PySpark is unavailable in the active environment. Activate .venv or use --skip-spark-validation."
            ) from exc
        spark = (
            SparkSession.builder.appName("ValidateCPCBHistoricalSilverBeforeUpload")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        try:
            spark.sparkContext.setLogLevel("WARN")
            frame = spark.read.parquet(str(args.dataset_dir))
            actual_rows = frame.count()
            if actual_rows != expected_rows:
                raise RuntimeError(
                    f"Parquet row count mismatch: quality report={expected_rows:,}, actual={actual_rows:,}."
                )
            required_columns = {
                "extraction_id", "retrieved_at_utc", "ingestion_date",
                "source_last_update_raw", "candidate_station_key",
                "candidate_entity_key", "pollutant_id", "measurement_status",
                "source_record_hash", "duplicate_excess_count",
            }
            missing = sorted(required_columns - set(frame.columns))
            if missing:
                raise RuntimeError(f"Silver Parquet is missing expected columns: {missing}")
            spark_validation = {
                "performed": True,
                "row_count": actual_rows,
                "columns": frame.columns,
                "schema_json": frame.schema.jsonValue(),
            }
        finally:
            spark.stop()

    summary = {
        "dataset": DATASET_NAME,
        "grain": report.get("grain"),
        "combined_unique_record_count": expected_rows,
        "combined_duplicate_excess_count": report.get("combined_duplicate_excess_count"),
        "conflicting_timestamped_entity_groups": report.get("conflicting_timestamped_entity_groups"),
        "cross_extraction_deduplication_performed": report.get("cross_extraction_deduplication_performed"),
        "source_extraction_ids": [
            item.get("extraction_id") for item in report.get("source_extractions", [])
            if isinstance(item, dict) and item.get("extraction_id")
        ],
        "measurement_status_counts": {
            item.get("extraction_id"): item.get("measurement_status_counts")
            for item in report.get("source_extractions", [])
            if isinstance(item, dict) and item.get("extraction_id")
        },
        "partitions": sorted(seen_partitions),
        "parquet_file_count": len(inventory),
        "total_parquet_bytes": total_bytes,
        "spark_validation": spark_validation,
    }
    if len(summary["source_extraction_ids"]) < 2:
        raise RuntimeError("Expected at least two source extractions in this historical bootstrap report.")
    return inventory, report, summary


def list_remote_parquet_keys(s3: Any, bucket: str, prefix: str) -> set[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys: set[str] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".parquet"):
                keys.add(key)
    return keys


def upload_and_verify(s3: Any, bucket: str, item: dict[str, Any], local_root: Path) -> str:
    key = item["s3_key"]
    expected = item["sha256"]
    if object_exists(s3, bucket, key):
        actual = remote_sha256(s3, bucket, key)
        if actual != expected:
            raise RuntimeError(
                f"Refusing to overwrite different existing object: s3://{bucket}/{key}"
            )
        return "SKIPPED_IDENTICAL"

    local_path = local_root / Path(item["local_relative_path"])
    s3.upload_file(
        str(local_path), bucket, key,
        ExtraArgs={
            "ContentType": "application/vnd.apache.parquet",
            "ServerSideEncryption": "AES256",
            "Metadata": {
                "sha256": expected,
                "dataset": DATASET_NAME,
            },
        },
    )
    actual = remote_sha256(s3, bucket, key)
    if actual != expected:
        raise RuntimeError(f"Checksum verification failed after upload: s3://{bucket}/{key}")
    return "UPLOADED_VERIFIED"


def main() -> int:
    args = parse_args()
    try:
        inventory, quality_report, summary = validate_local(args)
    except Exception as exc:
        print(f"ERROR: Local Silver validation failed: {exc}", file=sys.stderr)
        return 2

    # Hash the deterministic content inventory to produce a stable build identifier.
    inventory_for_hash = [
        {k: item[k] for k in ("local_relative_path", "s3_key", "size_bytes", "sha256")}
        for item in inventory
    ]
    build_digest = hashlib.sha256(
        json.dumps(inventory_for_hash, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    build_id = f"sha256-{build_digest[:16]}"
    metadata_prefix = f"{METADATA_ROOT}/build_id={build_id}"
    manifest_key = f"{metadata_prefix}/manifest.json"
    quality_key = f"{metadata_prefix}/quality_report.json"

    manifest = {
        "manifest_version": 1,
        "dataset": DATASET_NAME,
        "build_id": build_id,
        "created_at_utc": utc_now(),
        "bucket": args.bucket,
        "data_prefix": DATA_PREFIX,
        "glue_table_location": f"s3://{args.bucket}/{DATA_PREFIX}",
        "metadata_prefix": metadata_prefix,
        "quality_report_key": quality_key,
        "summary": summary,
        "files": inventory,
        "notes": [
            "Only Parquet data files are stored under the Glue table location.",
            "The quality report and manifest are stored outside the Glue table location.",
            "This is a guarded bootstrap upload; use a separate incremental process for future appends.",
            "The existing silver/cpcb_observations dataset is not modified.",
        ],
    }
    manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False, default=str).encode("utf-8")
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()

    print("=" * 78)
    print("CPCB HISTORICAL SILVER UPLOAD VALIDATION")
    print("=" * 78)
    print(f"Local dataset       : {args.dataset_dir}")
    print(f"Quality report      : {args.quality_report}")
    print(f"Source extractions  : {', '.join(summary['source_extraction_ids'])}")
    print(f"Expected rows       : {summary['combined_unique_record_count']:,}")
    print(f"Parquet files       : {len(inventory)}")
    print(f"Parquet size        : {summary['total_parquet_bytes']:,} bytes")
    print(f"Partitions          : {', '.join(summary['partitions'])}")
    print(f"S3 data prefix      : s3://{args.bucket}/{DATA_PREFIX}")
    print(f"S3 manifest         : s3://{args.bucket}/{manifest_key}")
    print(f"Build ID            : {build_id}")

    if args.dry_run:
        print("DRY RUN: no S3 objects were written.")
        for item in inventory:
            print(f"  {item['local_relative_path']} -> s3://{args.bucket}/{item['s3_key']}")
        return 0

    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        s3 = session.client("s3")
        s3.head_bucket(Bucket=args.bucket)
    except ProfileNotFound:
        print(f"ERROR: AWS profile not found: {args.profile}", file=sys.stderr)
        return 2
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        print(
            f"ERROR: Cannot access bucket {args.bucket} (AWS error: {code}). "
            f"For an expired SSO session run: aws sso login --profile {args.profile}",
            file=sys.stderr,
        )
        return 2

    try:
        # If this exact content has already been uploaded, verify and return safely.
        existing_manifest = read_remote_json(s3, args.bucket, manifest_key)
        if existing_manifest is not None:
            existing_files = existing_manifest.get("files", [])
            existing_sig = sorted((x.get("s3_key"), x.get("sha256"), x.get("size_bytes")) for x in existing_files if isinstance(x, dict))
            new_sig = sorted((x["s3_key"], x["sha256"], x["size_bytes"]) for x in inventory)
            if existing_sig != new_sig:
                raise RuntimeError(
                    "A manifest already exists for this build ID but its file inventory differs. "
                    "No overwrite performed. Investigate the existing S3 metadata."
                )
            print("Existing matching manifest found; verifying its Parquet files.")

        existing_data_keys = list_remote_parquet_keys(s3, args.bucket, DATA_PREFIX)
        expected_data_keys = {item["s3_key"] for item in inventory}
        unexpected = sorted(existing_data_keys - expected_data_keys)
        if unexpected:
            preview = "\n".join(f"  s3://{args.bucket}/{key}" for key in unexpected[:10])
            raise RuntimeError(
                "Unexpected Parquet objects already exist under the target table prefix. "
                "Refusing to mix unrelated/stale datasets. First objects:\n" + preview
            )

        uploaded = skipped = 0
        for index, item in enumerate(inventory, start=1):
            status = upload_and_verify(s3, args.bucket, item, args.dataset_dir)
            if status == "UPLOADED_VERIFIED":
                uploaded += 1
            else:
                skipped += 1
            print(f"[{index}/{len(inventory)}] {status}: {item['local_relative_path']}")

        # Upload quality report outside the Glue data location.
        report_bytes = args.quality_report.read_bytes()
        report_sha = hashlib.sha256(report_bytes).hexdigest()
        if object_exists(s3, args.bucket, quality_key):
            if remote_sha256(s3, args.bucket, quality_key) != report_sha:
                raise RuntimeError(f"Refusing to overwrite different quality report at s3://{args.bucket}/{quality_key}")
        else:
            s3.put_object(
                Bucket=args.bucket,
                Key=quality_key,
                Body=report_bytes,
                ContentType="application/json",
                ServerSideEncryption="AES256",
                Metadata={"sha256": report_sha, "dataset": DATASET_NAME},
            )
            if remote_sha256(s3, args.bucket, quality_key) != report_sha:
                raise RuntimeError("Quality report checksum verification failed.")

        # Manifest is the final commit marker: it is created only after all data/report objects verify.
        if existing_manifest is None:
            s3.put_object(
                Bucket=args.bucket,
                Key=manifest_key,
                Body=manifest_bytes,
                ContentType="application/json",
                ServerSideEncryption="AES256",
                Metadata={"sha256": manifest_sha, "dataset": DATASET_NAME, "build-id": build_id},
            )
            if remote_sha256(s3, args.bucket, manifest_key) != manifest_sha:
                raise RuntimeError("Manifest checksum verification failed after upload.")

        print("\n" + "=" * 78)
        print("HISTORICAL SILVER UPLOAD COMPLETE")
        print("=" * 78)
        print(f"New Parquet files uploaded : {uploaded}")
        print(f"Identical files skipped    : {skipped}")
        print(f"Rows in dataset             : {summary['combined_unique_record_count']:,}")
        print(f"Glue table location         : s3://{args.bucket}/{DATA_PREFIX}")
        print(f"Manifest                    : s3://{args.bucket}/{manifest_key}")
        print("Glue Catalog                : NOT MODIFIED")
        print("Existing cpcb_observations  : NOT MODIFIED")

    except (ClientError, OSError, RuntimeError, ValueError) as exc:
        print(f"\nERROR during S3 upload: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
