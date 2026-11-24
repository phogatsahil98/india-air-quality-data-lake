#!/usr/bin/env python3
"""Idempotently append one extraction's Parquet files to CPCB historical Silver.

This is the incremental counterpart to upload_historical_silver.py, which is a
one-time bootstrap uploader for the two initial snapshots.

Data layout:
  silver/cpcb_observation_snapshots/ingestion_date=YYYY-MM-DD/<EXTRACTION_ID>__*.parquet

Metadata layout:
  metadata/silver_runs/dataset=cpcb_observation_snapshots/
      extraction_id=<ID>/{quality_report.json,manifest.json}

The manifest is written last and is the commit marker for one extraction.
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
DATASET = "cpcb_observation_snapshots"
DATA_PREFIX = f"silver/{DATASET}/"
METADATA_PREFIX = f"metadata/silver_runs/dataset={DATASET}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append one CPCB extraction to historical Silver.")
    parser.add_argument("--extraction-id", required=True)
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--quality-report", type=Path, required=True)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def parse_extraction_id(value: str) -> tuple[datetime, str]:
    if not re.fullmatch(r"\d{8}T\d{6}Z", value):
        raise ValueError("extraction_id must use YYYYMMDDTHHMMSSZ format")
    timestamp = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return timestamp, timestamp.date().isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_remote(s3: Any, bucket: str, key: str) -> str:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    digest = hashlib.sha256()
    try:
        for chunk in body.iter_chunks(chunk_size=1024 * 1024):
            if chunk:
                digest.update(chunk)
    finally:
        body.close()
    return digest.hexdigest()


def object_exists(s3: Any, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return False
        raise


def read_remote_json(s3: Any, bucket: str, key: str) -> dict[str, Any] | None:
    if not object_exists(s3, bucket, key):
        return None
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        value = json.loads(body.read().decode("utf-8"))
    finally:
        body.close()
    if not isinstance(value, dict):
        raise RuntimeError(f"Remote metadata is not a JSON object: s3://{bucket}/{key}")
    return value



def canonical_json_hash(value: Any, *, ignored_top_level_keys: tuple[str, ...] = ()) -> str:
    """Hash JSON content deterministically, optionally ignoring run-time metadata."""
    if isinstance(value, dict):
        value = {key: item for key, item in value.items() if key not in ignored_top_level_keys}
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_quality_report(
    report: dict[str, Any],
    extraction_id: str,
    expected_retrieved_at: datetime,
) -> int:
    """Validate that a quality report describes exactly the requested extraction."""
    if report.get("dataset") != DATASET:
        raise RuntimeError(f"Quality report dataset must be {DATASET!r}.")
    source_runs = report.get("source_extractions")
    if not isinstance(source_runs, list) or len(source_runs) != 1:
        raise RuntimeError("Expected a quality report containing exactly one source extraction.")
    source_run = source_runs[0]
    if not isinstance(source_run, dict) or source_run.get("extraction_id") != extraction_id:
        raise RuntimeError("Quality report extraction_id does not match the requested extraction.")
    if report.get("conflicting_timestamped_entity_groups") != 0:
        raise RuntimeError("Silver report contains conflicting timestamped entity groups.")
    if report.get("cross_extraction_deduplication_performed") is not False:
        raise RuntimeError("Silver report does not confirm within-extraction-only deduplication.")

    unique_rows = report.get("combined_unique_record_count")
    run_unique_rows = source_run.get("spark_unique_record_count")
    if not isinstance(unique_rows, int) or unique_rows <= 0:
        raise RuntimeError(f"Invalid unique row count in quality report: {unique_rows!r}.")
    if run_unique_rows != unique_rows:
        raise RuntimeError(
            f"Per-extraction unique row count ({run_unique_rows!r}) does not match "
            f"the combined unique row count ({unique_rows})."
        )

    try:
        expected_text = expected_retrieved_at.isoformat().replace("+00:00", "Z")
        actual_text = str(source_run.get("retrieved_at_utc", ""))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Invalid retrieved_at_utc in quality report.") from exc
    if actual_text != expected_text:
        raise RuntimeError(
            f"Quality report retrieved_at_utc mismatch: expected {expected_text}, got {actual_text!r}."
        )

    if source_run.get("conflicting_timestamped_entity_groups", 0) not in (0, None):
        raise RuntimeError("Extraction-level quality report contains timestamped entity conflicts.")
    return unique_rows

def load_and_validate_local(args: argparse.Namespace, extraction_id: str, ingestion_date: str, retrieved_at: datetime) -> tuple[list[dict[str, Any]], dict[str, Any], int]:
    if not args.dataset_dir.is_dir():
        raise FileNotFoundError(f"Silver dataset directory does not exist: {args.dataset_dir}")
    if not args.quality_report.is_file():
        raise FileNotFoundError(f"Silver quality report does not exist: {args.quality_report}")

    try:
        report = json.loads(args.quality_report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Could not read valid Silver quality report: {exc}") from exc
    if not isinstance(report, dict):
        raise RuntimeError("Quality report must contain a JSON object.")
    unique_rows = validate_quality_report(report, extraction_id, retrieved_at)

    partition_dir = args.dataset_dir / f"ingestion_date={ingestion_date}"
    parquet_files = sorted(path for path in partition_dir.rglob("*.parquet") if path.is_file())
    if not parquet_files:
        raise RuntimeError(f"No Parquet files found under expected partition: {partition_dir}")

    # Refuse extra date partitions: this upload is intentionally for one run only.
    other_partitions = [
        path.name for path in args.dataset_dir.glob("ingestion_date=*")
        if path.name != f"ingestion_date={ingestion_date}"
    ]
    if other_partitions:
        raise RuntimeError(f"Expected one ingestion-date partition; found {other_partitions}")

    inventory: list[dict[str, Any]] = []
    for path in parquet_files:
        if path.stat().st_size <= 0:
            raise RuntimeError(f"Empty Parquet file: {path}")
        digest = sha256_file(path)
        remote_name = f"{extraction_id}__{path.name}"
        key = f"{DATA_PREFIX}ingestion_date={ingestion_date}/{remote_name}"
        inventory.append({
            "local_relative_path": path.relative_to(args.dataset_dir).as_posix(),
            "s3_key": key,
            "size_bytes": path.stat().st_size,
            "sha256": digest,
        })
    return inventory, report, unique_rows


def verify_remote_object(s3: Any, bucket: str, item: dict[str, Any]) -> None:
    head = s3.head_object(Bucket=bucket, Key=item["s3_key"])
    if head.get("ContentLength") != item["size_bytes"]:
        raise RuntimeError(f"S3 object size mismatch: s3://{bucket}/{item['s3_key']}")
    metadata = {str(k).lower(): v for k, v in (head.get("Metadata") or {}).items()}
    if metadata.get("sha256") != item["sha256"]:
        raise RuntimeError(f"S3 SHA-256 metadata mismatch: s3://{bucket}/{item['s3_key']}")
    if head.get("ServerSideEncryption") != "AES256":
        raise RuntimeError(f"Expected SSE-S3 encryption: s3://{bucket}/{item['s3_key']}")
    if sha256_remote(s3, bucket, item["s3_key"]) != item["sha256"]:
        raise RuntimeError(f"S3 content checksum mismatch: s3://{bucket}/{item['s3_key']}")


def main() -> int:
    args = parse_args()
    try:
        retrieved_at, ingestion_date = parse_extraction_id(args.extraction_id)
        inventory, report, row_count = load_and_validate_local(args, args.extraction_id, ingestion_date, retrieved_at)
    except Exception as exc:
        print(f"ERROR: local Silver validation failed: {exc}", file=sys.stderr)
        return 2

    run_prefix = f"{METADATA_PREFIX}/extraction_id={args.extraction_id}"
    manifest_key = f"{run_prefix}/manifest.json"
    report_key = f"{run_prefix}/quality_report.json"
    manifest = {
        "manifest_version": 1,
        "dataset": DATASET,
        "extraction_id": args.extraction_id,
        "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
        "ingestion_date": ingestion_date,
        "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "bucket": args.bucket,
        "data_prefix": DATA_PREFIX,
        "quality_report_key": report_key,
        "quality_report_sha256": hashlib.sha256(args.quality_report.read_bytes()).hexdigest(),
        "quality_report_size_bytes": args.quality_report.stat().st_size,
        "row_count": row_count,
        "duplicate_excess_count": sum(int(run.get("duplicate_excess_count") or 0) for run in report.get("source_extractions", [])),
        "files": inventory,
        "notes": [
            "One manifest represents one immutable source extraction.",
            "Exact duplicates are removed within this extraction only.",
            "No deduplication is performed across different extraction IDs.",
            "source_last_update_raw is retained as supplied by CPCB; no timezone is inferred.",
        ],
    }

    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        s3 = session.client("s3")
        s3.head_bucket(Bucket=args.bucket)

        existing = read_remote_json(s3, args.bucket, manifest_key)
        local_report_bytes = args.quality_report.read_bytes()
        if existing is not None:
            old_files = existing.get("files")
            if not isinstance(old_files, list) or not old_files:
                raise RuntimeError("Existing extraction manifest has no valid file inventory.")
            old_sig = sorted((x.get("sha256"), x.get("size_bytes")) for x in old_files if isinstance(x, dict))
            new_sig = sorted((x["sha256"], x["size_bytes"]) for x in inventory)
            if (
                existing.get("extraction_id") != args.extraction_id
                or old_sig != new_sig
                or existing.get("row_count") != row_count
            ):
                raise RuntimeError(
                    "An existing extraction manifest conflicts with this local build; refusing overwrite."
                )

            # Compare stored Parquet objects by content, not generated Spark part
            # filenames. This permits a deterministic rebuild to use new part names.
            for old_item in old_files:
                key = old_item.get("s3_key")
                if not isinstance(key, str) or not key.startswith(
                    f"{DATA_PREFIX}ingestion_date={ingestion_date}/"
                ):
                    raise RuntimeError("Existing manifest contains a Parquet key outside the expected partition.")
                verify_remote_object(
                    s3,
                    args.bucket,
                    {
                        "s3_key": key,
                        "sha256": old_item["sha256"],
                        "size_bytes": old_item["size_bytes"],
                    },
                )

            stored_report_key = existing.get("quality_report_key", report_key)
            if stored_report_key != report_key or not object_exists(s3, args.bucket, report_key):
                raise RuntimeError("Existing extraction manifest points to a missing or unexpected quality report.")
            remote_report = read_remote_json(s3, args.bucket, report_key)
            if remote_report is None:
                raise RuntimeError("Existing quality report is missing.")
            local_report = json.loads(local_report_bytes.decode("utf-8"))
            if canonical_json_hash(remote_report, ignored_top_level_keys=("build_created_at_utc",)) != canonical_json_hash(
                local_report, ignored_top_level_keys=("build_created_at_utc",)
            ):
                raise RuntimeError("Existing quality report content differs from this build.")
            expected_report_sha = existing.get("quality_report_sha256")
            actual_report_sha = sha256_remote(s3, args.bucket, report_key)
            if expected_report_sha and actual_report_sha != expected_report_sha:
                raise RuntimeError("Existing quality report checksum differs from its manifest.")

            print(f"Existing matching Silver extraction verified: {args.extraction_id}")
            print(f"Rows: {row_count:,}; Parquet files: {len(inventory)}")
            print("Parquet objects and quality report verified against the existing manifest.")
            return 0

        print("CPCB INCREMENTAL SILVER UPLOAD")
        print(f"Extraction ID : {args.extraction_id}")
        print(f"Ingestion date: {ingestion_date}")
        print(f"Rows          : {row_count:,}")
        print(f"Parquet files : {len(inventory)}")
        print(f"S3 prefix     : s3://{args.bucket}/{DATA_PREFIX}ingestion_date={ingestion_date}/")

        if args.dry_run:
            for item in inventory:
                print(f"WOULD UPLOAD s3://{args.bucket}/{item['s3_key']} ({item['size_bytes']:,} bytes)")
            print("DRY RUN: no S3 objects written")
            return 0

        for item in inventory:
            local_path = args.dataset_dir / Path(item["local_relative_path"])
            if object_exists(s3, args.bucket, item["s3_key"]):
                verify_remote_object(s3, args.bucket, item)
                print(f"SKIPPED_IDENTICAL {item['s3_key']}")
                continue
            s3.upload_file(
                str(local_path), args.bucket, item["s3_key"],
                ExtraArgs={
                    "ContentType": "application/vnd.apache.parquet",
                    "ServerSideEncryption": "AES256",
                    "Metadata": {"sha256": item["sha256"], "extraction-id": args.extraction_id},
                },
            )
            verify_remote_object(s3, args.bucket, item)
            print(f"UPLOADED_VERIFIED {item['s3_key']}")

        report_bytes = args.quality_report.read_bytes()
        report_sha = hashlib.sha256(report_bytes).hexdigest()
        if object_exists(s3, args.bucket, report_key):
            existing_sha = sha256_remote(s3, args.bucket, report_key)
            if existing_sha != report_sha:
                raise RuntimeError("Existing quality report differs; refusing to overwrite")
        else:
            s3.put_object(
                Bucket=args.bucket, Key=report_key, Body=report_bytes,
                ContentType="application/json", ServerSideEncryption="AES256",
                Metadata={"sha256": report_sha, "extraction-id": args.extraction_id},
            )
            if sha256_remote(s3, args.bucket, report_key) != report_sha:
                raise RuntimeError("Quality report checksum verification failed")

        manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False, default=str).encode("utf-8")
        s3.put_object(
            Bucket=args.bucket, Key=manifest_key, Body=manifest_bytes,
            ContentType="application/json", ServerSideEncryption="AES256",
            Metadata={"sha256": hashlib.sha256(manifest_bytes).hexdigest(), "extraction-id": args.extraction_id},
        )
        if sha256_remote(s3, args.bucket, manifest_key) != hashlib.sha256(manifest_bytes).hexdigest():
            raise RuntimeError("Final Silver manifest checksum verification failed")

        print("INCREMENTAL SILVER UPLOAD COMPLETE")
        print(f"Manifest: s3://{args.bucket}/{manifest_key}")
        print("Glue Catalog: not modified by this script")
        return 0
    except ProfileNotFound:
        print(f"ERROR: AWS profile not found: {args.profile}", file=sys.stderr)
        return 2
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        message = exc.response.get("Error", {}).get("Message", str(exc))
        print(f"AWS ERROR ({code}): {message}", file=sys.stderr)
        if code in {"ExpiredToken", "UnauthorizedException", "UnrecognizedClientException"}:
            print(f"Refresh SSO on host: aws sso login --profile {args.profile}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR during incremental Silver upload: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
