#!/usr/bin/env python3
"""Upload one CPCB raw extraction to an immutable S3 Bronze prefix.

Expected local layout (created by profile_cpcb_full.py):
    data/raw/source_profile_<EXTRACTION_ID>/*.json

S3 layout:
    s3://<bucket>/bronze/cpcb/snapshots/extraction_id=<EXTRACTION_ID>/
        data/<relative-json-path>
        _metadata/manifest.json

The script is deliberately append-only. It skips already-uploaded identical
objects, refuses conflicting objects, and writes the manifest last.
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
DATASET_NAME = "cpcb_real_time_air_quality"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload a CPCB extraction to its own immutable S3 Bronze prefix."
    )
    parser.add_argument(
        "--extraction-id",
        required=True,
        help="Run ID, for example 20261120T170816Z (without source_profile_).",
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("data/raw"),
        help="Parent directory containing source_profile_<EXTRACTION_ID> (default: data/raw).",
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help=f"S3 bucket (default: {DEFAULT_BUCKET}).")
    parser.add_argument("--region", default=DEFAULT_REGION, help=f"AWS region (default: {DEFAULT_REGION}).")
    parser.add_argument("--profile", default=DEFAULT_PROFILE, help=f"AWS CLI profile (default: {DEFAULT_PROFILE}).")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate local files and print the planned S3 keys without uploading.",
    )
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def extraction_time(extraction_id: str) -> str:
    try:
        parsed = datetime.strptime(extraction_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(
            "Invalid extraction ID. Expected format YYYYMMDDTHHMMSSZ, "
            "for example 20261120T170816Z."
        ) from exc
    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_page_metadata(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read valid JSON page: {path} ({exc})") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"Expected an API response object in {path.name}, got {type(payload).__name__}.")

    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError(
            f"{path.name} does not contain a top-level 'records' list. "
            "No files have been uploaded. Check the raw API response format."
        )

    def optional_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    return {
        "record_count": len(records),
        "offset": optional_int(payload.get("offset")),
        "limit": optional_int(payload.get("limit")),
        "api_total": optional_int(payload.get("total")),
    }


def is_not_found(error: ClientError) -> bool:
    code = str(error.response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NotFound"}


def object_exists(s3_client: Any, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if is_not_found(exc):
            return False
        raise


def remote_sha256(s3_client: Any, bucket: str, key: str) -> str:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    digest = hashlib.sha256()
    body = response["Body"]
    try:
        for chunk in body.iter_chunks(chunk_size=1024 * 1024):
            if chunk:
                digest.update(chunk)
    finally:
        body.close()
    return digest.hexdigest()


def read_existing_manifest(s3_client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    if not object_exists(s3_client, bucket, key):
        return None
    response = s3_client.get_object(Bucket=bucket, Key=key)
    try:
        payload = json.loads(response["Body"].read().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"An existing manifest is unreadable: s3://{bucket}/{key}") from exc
    finally:
        response["Body"].close()
    if not isinstance(payload, dict):
        raise RuntimeError(f"An existing manifest has an unexpected structure: s3://{bucket}/{key}")
    return payload


def make_local_inventory(run_dir: Path, extraction_id: str, prefix: str) -> tuple[list[dict[str, Any]], int, list[int]]:
    json_files = sorted(path for path in run_dir.rglob("*.json") if path.is_file())
    if not json_files:
        raise FileNotFoundError(f"No .json files found under {run_dir}")

    inventory: list[dict[str, Any]] = []
    total_records = 0
    api_totals: set[int] = set()

    for path in json_files:
        relative_path = path.relative_to(run_dir).as_posix()
        page_info = read_page_metadata(path)
        file_hash = sha256_file(path)
        size_bytes = path.stat().st_size
        s3_key = f"{prefix}/data/{relative_path}"
        item = {
            "extraction_id": extraction_id,
            "local_relative_path": relative_path,
            "s3_key": s3_key,
            "size_bytes": size_bytes,
            "sha256": file_hash,
            **page_info,
        }
        inventory.append(item)
        total_records += page_info["record_count"]
        if page_info["api_total"] is not None:
            api_totals.add(page_info["api_total"])

    return inventory, total_records, sorted(api_totals)



def validate_page_inventory(
    inventory: list[dict[str, Any]],
    total_records: int,
    api_totals: list[int],
) -> dict[str, Any]:
    """Fail closed when page totals or offsets indicate an incomplete extraction."""
    if not inventory:
        raise RuntimeError("The extraction contains no API JSON pages.")

    if len(api_totals) > 1:
        raise RuntimeError(
            "CPCB reported inconsistent total counts across pages: "
            f"{api_totals}. Refusing to publish an immutable Bronze manifest."
        )

    api_total = api_totals[0] if api_totals else None
    if api_total is not None and total_records != api_total:
        raise RuntimeError(
            f"Incomplete extraction: API total is {api_total}, "
            f"but local pages contain {total_records} records."
        )

    # Prefer the explicit offset returned by the API. If it is absent, the
    # extractor's deterministic page_offset_<offset>.json filename is used.
    offsets: list[tuple[int, int, str]] = []
    for item in inventory:
        offset = item.get("offset")
        if offset is None:
            match = re.fullmatch(
                r"page_offset_(\d+)\.json",
                Path(item["local_relative_path"]).name,
            )
            if not match:
                raise RuntimeError(
                    "Could not validate page order: an API page has no returned offset "
                    "and does not use the expected page_offset_<offset>.json filename."
                )
            offset = int(match.group(1))
        offsets.append((int(offset), int(item["record_count"]), item["local_relative_path"]))

    offsets_validated = False
    if offsets:
        offsets.sort(key=lambda value: value[0])
        expected_offset = 0
        for actual_offset, record_count, filename in offsets:
            if actual_offset != expected_offset:
                raise RuntimeError(
                    f"Non-contiguous API pagination at {filename}: expected offset "
                    f"{expected_offset}, found {actual_offset}."
                )
            expected_offset += record_count
        offsets_validated = True
        if expected_offset != total_records:
            raise RuntimeError(
                f"Page offsets cover {expected_offset} records, but inventory contains "
                f"{total_records}."
            )

    if api_total is None:
        raise RuntimeError(
            "The API total was not present in page responses. Refusing to mark this "
            "extraction complete; verify the source response before uploading."
        )

    return {
        "status": "PASSED",
        "api_total": api_total,
        "raw_record_count": total_records,
        "record_count_matches_api_total": total_records == api_total,
        "page_offsets_validated": offsets_validated,
        "page_count": len(inventory),
    }

def compare_inventory(existing_manifest: dict[str, Any], inventory: list[dict[str, Any]], extraction_id: str) -> bool:
    if existing_manifest.get("extraction_id") != extraction_id:
        return False
    existing_files = existing_manifest.get("files")
    if not isinstance(existing_files, list):
        return False

    def signature(items: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
        return sorted(
            (
                item.get("local_relative_path"),
                item.get("s3_key"),
                item.get("size_bytes"),
                item.get("sha256"),
                item.get("record_count"),
            )
            for item in items
        )

    return signature(existing_files) == signature(inventory)


def upload_one_file(s3_client: Any, bucket: str, item: dict[str, Any], local_path: Path) -> str:
    key = item["s3_key"]
    expected_hash = item["sha256"]

    if object_exists(s3_client, bucket, key):
        actual_hash = remote_sha256(s3_client, bucket, key)
        if actual_hash != expected_hash:
            raise RuntimeError(
                f"Refusing to overwrite different data already stored at s3://{bucket}/{key}. "
                "Use a new extraction ID after checking the source run."
            )
        return "SKIPPED_IDENTICAL"

    s3_client.upload_file(
        str(local_path),
        bucket,
        key,
        ExtraArgs={
            "ContentType": "application/json",
            "ServerSideEncryption": "AES256",
            "Metadata": {"sha256": expected_hash, "extraction-id": item.get("extraction_id", "")},
        },
    )

    actual_hash = remote_sha256(s3_client, bucket, key)
    if actual_hash != expected_hash:
        raise RuntimeError(f"Checksum verification failed for s3://{bucket}/{key}.")
    return "UPLOADED_VERIFIED"


def main() -> int:
    args = parse_args()
    try:
        retrieved_at = extraction_time(args.extraction_id)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    run_dir = args.source_root / f"source_profile_{args.extraction_id}"
    if not run_dir.is_dir():
        print(f"ERROR: Source extraction folder does not exist: {run_dir}", file=sys.stderr)
        return 2

    prefix = f"bronze/cpcb/snapshots/extraction_id={args.extraction_id}"
    manifest_key = f"{prefix}/_metadata/manifest.json"

    try:
        inventory, total_records, api_totals = make_local_inventory(run_dir, args.extraction_id, prefix)
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    try:
        completeness = validate_page_inventory(inventory, total_records, api_totals)
    except RuntimeError as exc:
        print(f"ERROR: Extraction completeness validation failed: {exc}", file=sys.stderr)
        return 2

    print("=" * 76)
    print("CPCB DYNAMIC BRONZE UPLOAD")
    print("=" * 76)
    print(f"Extraction ID : {args.extraction_id}")
    print(f"Retrieved at  : {retrieved_at}")
    print(f"Local folder  : {run_dir}")
    print(f"JSON pages    : {len(inventory)}")
    print(f"Raw records   : {total_records}")
    print(f"API total(s)  : {api_totals}")
    print(f"Completeness  : {completeness['status']}")
    print(f"Offsets valid : {completeness['page_offsets_validated']}")
    print(f"S3 destination: s3://{args.bucket}/{prefix}/")

    for item in inventory:
        print(f"  {item['local_relative_path']} -> {item['s3_key']} ({item['record_count']} records)")

    if args.dry_run:
        print("\nDRY RUN: validation completed; no AWS calls or uploads were performed.")
        return 0

    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        s3_client = session.client("s3")
        s3_client.head_bucket(Bucket=args.bucket)
    except ProfileNotFound:
        print(
            f"ERROR: AWS profile '{args.profile}' was not found. Check your AWS config.",
            file=sys.stderr,
        )
        return 2
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        print(
            f"ERROR: Could not access bucket '{args.bucket}' using profile '{args.profile}' "
            f"in region '{args.region}' (AWS error: {code}).\n"
            f"For an expired IAM Identity Center session, run: aws sso login --profile {args.profile}",
            file=sys.stderr,
        )
        return 2

    # If the manifest already exists, only accept a byte-for-byte equivalent
    # file inventory. Never silently merge a different extraction into a run.
    try:
        existing_manifest = read_existing_manifest(s3_client, args.bucket, manifest_key)
        if existing_manifest is not None and not compare_inventory(existing_manifest, inventory, args.extraction_id):
            raise RuntimeError(
                f"A manifest already exists for this extraction ID but the local file inventory differs: "
                f"s3://{args.bucket}/{manifest_key}. Refusing to modify an immutable extraction."
            )

        uploaded = 0
        skipped = 0
        for item in inventory:
            local_path = run_dir / Path(item["local_relative_path"])
            status = upload_one_file(s3_client, args.bucket, item, local_path)
            if status == "UPLOADED_VERIFIED":
                uploaded += 1
            else:
                skipped += 1
            print(f"{status}: {item['local_relative_path']}")

        manifest = {
            "manifest_version": 1,
            "dataset": DATASET_NAME,
            "source": "CPCB real-time air-quality observations via data.gov.in API",
            "extraction_id": args.extraction_id,
            "retrieved_at_utc": retrieved_at,
            "uploaded_at_utc": utc_now(),
            "aws_region": args.region,
            "bucket": args.bucket,
            "s3_prefix": prefix,
            "page_count": len(inventory),
            "raw_record_count": total_records,
            "api_total_values_reported": api_totals,
            "completeness_validation": completeness,
            "files": inventory,
        }

        if existing_manifest is None:
            manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
            manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
            s3_client.put_object(
                Bucket=args.bucket,
                Key=manifest_key,
                Body=manifest_bytes,
                ContentType="application/json",
                ServerSideEncryption="AES256",
                Metadata={"sha256": manifest_sha, "extraction-id": args.extraction_id},
            )
            if remote_sha256(s3_client, args.bucket, manifest_key) != manifest_sha:
                raise RuntimeError(
                    f"Manifest checksum verification failed for s3://{args.bucket}/{manifest_key}."
                )
            manifest_action = "CREATED_AND_VERIFIED"
        else:
            manifest_action = "EXISTING_VALIDATED"

    except (ClientError, RuntimeError, OSError) as exc:
        print(f"\nERROR: Bronze upload did not complete: {exc}", file=sys.stderr)
        return 1

    print("\n" + "=" * 76)
    print("BRONZE UPLOAD COMPLETE")
    print("=" * 76)
    print(f"Files uploaded and checksum-verified: {uploaded}")
    print(f"Identical files already present     : {skipped}")
    print(f"Manifest status                     : {manifest_action}")
    print(f"Manifest                            : s3://{args.bucket}/{manifest_key}")
    print("The source extraction folder was not modified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
