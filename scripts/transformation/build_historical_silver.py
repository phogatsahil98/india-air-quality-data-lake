#!/usr/bin/env python3
"""Build a consolidated, extraction-aware CPCB historical Silver dataset.

Build an extraction-aware CPCB Silver dataset for one or more explicit source runs.
It downloads immutable Bronze JSON pages from S3, validates them, removes exact
record duplicates *within each extraction only*, and writes a local Parquet
Silver dataset partitioned by our ingestion date.

It does NOT upload the Parquet files to S3 or alter any existing Silver data.

S3 Bronze layout:
    s3://<bucket>/bronze/cpcb/snapshots/extraction_id=<ID>/data/*.json
    s3://<bucket>/bronze/cpcb/snapshots/extraction_id=<ID>/_metadata/manifest.json

Local output:
    data/processed/<chosen-build-directory>/

Safety:
    Recursive output deletion is restricted to a descendant of data/processed.
    This script requires explicit extraction IDs; it has no stale snapshot defaults.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError, ProfileNotFound

DEFAULT_BUCKET = "india-aqi-lake-933139321496-ap-south-1"
DEFAULT_REGION = "ap-south-1"
DEFAULT_PROFILE = "india-aqi-admin"
BRONZE_ROOT = "bronze/cpcb/snapshots"
EXPECTED_SOURCE_FIELDS = [
    "country",
    "state",
    "city",
    "station",
    "last_update",
    "latitude",
    "longitude",
    "pollutant_id",
    "min_value",
    "max_value",
    "avg_value",
]
MEASUREMENT_FIELDS = ["min_value", "max_value", "avg_value"]
NULL_TOKENS = {"", "na", "n/a", "null", "none", "-"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download selected CPCB Bronze extractions from S3 and build a "
            "consolidated, extraction-aware local Silver Parquet dataset."
        )
    )
    parser.add_argument(
        "--extraction-ids",
        nargs="+",
        required=True,
        help=(
            "One or more explicit extraction IDs to process. "
            "Example: 20261116T194631Z 20261120T170816Z"
        ),
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("data/bronze_cache"),
        help="Local cache for downloaded immutable Bronze JSON pages.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/processed/historical_silver/cpcb_observation_snapshots"),
        help="Local Parquet output directory under data/processed.",
    )
    parser.add_argument(
        "--overwrite-output",
        action="store_true",
        help="Delete and rebuild only the specified generated output directory under data/processed.",
    )
    parser.add_argument(
        "--output-partitions",
        type=int,
        default=4,
        help="Number of Spark output partitions before writing Parquet (default: 4).",
    )
    return parser.parse_args()



def prepare_output_directory(output_dir: Path, *, overwrite: bool) -> Path:
    """Validate the generated output path and optionally remove a prior build.

    Deletion is limited to descendants of this repository's data/processed
    directory. The root itself, paths outside it, and symlink targets are never
    eligible for recursive deletion.
    """
    repo_root = Path(__file__).resolve().parents[2]
    allowed_roots = (
        (repo_root / "data" / "processed" / "historical_silver").resolve(),
        (repo_root / "data" / "processed" / "airflow_runs").resolve(),
    )

    if output_dir.is_symlink():
        raise ValueError("Output directory must not be a symbolic link.")

    target = output_dir.resolve()
    matched_root = None
    for allowed_root in allowed_roots:
        try:
            relative = target.relative_to(allowed_root)
            if relative.parts:
                matched_root = allowed_root
                break
        except ValueError:
            continue

    if matched_root is None:
        roots = ", ".join(str(path) for path in allowed_roots)
        raise ValueError(
            f"Output path must be a child of one of the generated build roots ({roots}); "
            f"refusing unsafe path {target}."
        )

    # Refuse symlinked build roots so a repository-local path cannot redirect
    # recursive deletion to an external directory.
    raw_allowed_roots = (
        repo_root / "data" / "processed" / "historical_silver",
        repo_root / "data" / "processed" / "airflow_runs",
    )
    if any(path.is_symlink() for path in raw_allowed_roots):
        raise ValueError("Generated build roots must not be symbolic links.")

    if target.exists():
        if not target.is_dir():
            raise ValueError(f"Output path exists but is not a directory: {target}")
        if not overwrite:
            raise FileExistsError(
                f"Output directory already exists: {target}. Choose a new path or "
                "use --overwrite-output for a generated directory under data/processed."
            )
        shutil.rmtree(target)

    # Create only the parent. Spark's errorifexists write mode must see a
    # non-existent target directory or it will correctly refuse the write.
    target.parent.mkdir(parents=True, exist_ok=True)
    return target

def parse_extraction_time(extraction_id: str) -> datetime:
    if not re.fullmatch(r"\d{8}T\d{6}Z", extraction_id):
        raise ValueError(
            f"Invalid extraction ID {extraction_id!r}; expected YYYYMMDDTHHMMSSZ."
        )
    return datetime.strptime(extraction_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json_object(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read valid JSON from {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected a JSON object in {path}, got {type(payload).__name__}.")
    return payload


def validate_extraction_id(extraction_id: str) -> None:
    parse_extraction_time(extraction_id)


def s3_object_exists(s3: Any, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def read_s3_json(s3: Any, bucket: str, key: str) -> dict[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=key)
    try:
        payload = json.loads(response["Body"].read().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unreadable S3 JSON object s3://{bucket}/{key}: {exc}") from exc
    finally:
        response["Body"].close()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected a JSON object in s3://{bucket}/{key}.")
    return payload


def list_data_keys(s3: Any, bucket: str, prefix: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.lower().endswith(".json"):
                keys.append(key)
    return sorted(keys)


def validate_page(path: Path) -> int:
    payload = read_json_object(path)
    records = payload.get("records")
    if not isinstance(records, list):
        raise RuntimeError(f"{path.name} does not contain a top-level 'records' array.")
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise RuntimeError(f"{path.name}: records[{index}] is not a JSON object.")
        missing = [field for field in EXPECTED_SOURCE_FIELDS if field not in record]
        if missing:
            raise RuntimeError(
                f"{path.name}: records[{index}] is missing expected source fields: {missing}"
            )
    return len(records)


def download_extraction(
    s3: Any,
    bucket: str,
    extraction_id: str,
    cache_root: Path,
) -> tuple[Path, dict[str, Any]]:
    base_prefix = f"{BRONZE_ROOT}/extraction_id={extraction_id}/"
    manifest_key = f"{base_prefix}_metadata/manifest.json"
    data_prefix = f"{base_prefix}data/"

    if not s3_object_exists(s3, bucket, manifest_key):
        raise RuntimeError(
            f"Bronze manifest not found: s3://{bucket}/{manifest_key}. "
            "The extraction must be uploaded successfully before Silver processing."
        )

    manifest = read_s3_json(s3, bucket, manifest_key)
    if manifest.get("extraction_id") not in (None, extraction_id):
        raise RuntimeError(
            f"Manifest extraction_id mismatch for {extraction_id}: "
            f"found {manifest.get('extraction_id')!r}."
        )
    completeness = manifest.get("completeness_validation")
    if completeness is not None:
        if not isinstance(completeness, dict) or completeness.get("status") != "PASSED":
            raise RuntimeError(
                f"Bronze extraction {extraction_id} does not have a passed completeness validation."
            )
        if completeness.get("record_count_matches_api_total") is not True:
            raise RuntimeError(
                f"Bronze extraction {extraction_id} record count does not match the API total."
            )

    keys = list_data_keys(s3, bucket, data_prefix)
    if not keys:
        raise RuntimeError(f"No JSON page files found at s3://{bucket}/{data_prefix}")

    manifest_files = manifest.get("files")
    if not isinstance(manifest_files, list) or not manifest_files:
        raise RuntimeError(
            f"Bronze manifest for {extraction_id} has no valid files inventory."
        )
    manifest_keys = {
        str(item.get("s3_key"))
        for item in manifest_files
        if isinstance(item, dict) and item.get("s3_key")
    }
    if len(manifest_keys) != len(manifest_files) or manifest_keys != set(keys):
        missing = sorted(set(keys) - manifest_keys)
        stale = sorted(manifest_keys - set(keys))
        raise RuntimeError(
            f"Bronze manifest inventory mismatch for {extraction_id}. "
            f"Missing from manifest={missing[:5]}, missing from S3={stale[:5]}."
        )

    expected_pages = manifest.get("page_count")
    if expected_pages is not None and int(expected_pages) != len(keys):
        raise RuntimeError(
            f"Page-count mismatch for {extraction_id}: manifest={expected_pages}, S3={len(keys)}."
        )

    # Both the original bootstrap manifest and the dynamic uploader record a
    # per-page S3 key and SHA-256 digest. Every downloaded page must be covered.
    expected_hashes: dict[str, str] = {}
    expected_sizes: dict[str, int] = {}
    for item in manifest_files:
        key = str(item["s3_key"])
        digest = str(item.get("sha256") or "")
        size_value = item.get("size_bytes")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", digest):
            raise RuntimeError(f"Bronze manifest has an invalid SHA-256 for {key}.")
        try:
            size_bytes = int(size_value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Bronze manifest has an invalid size for {key}.") from exc
        if size_bytes <= 0:
            raise RuntimeError(f"Bronze manifest reports an empty object: {key}.")
        expected_hashes[key] = digest.lower()
        expected_sizes[key] = size_bytes

    for key in keys:
        head = s3.head_object(Bucket=bucket, Key=key)
        if head.get("ContentLength") != expected_sizes[key]:
            raise RuntimeError(f"S3 page size does not match the manifest: s3://{bucket}/{key}.")

    run_dir = cache_root / f"extraction_id={extraction_id}" / "data"
    run_dir.mkdir(parents=True, exist_ok=True)
    raw_count = 0

    expected_filenames = {Path(key).name for key in keys}
    for stale_file in run_dir.glob("*.json"):
        if stale_file.name not in expected_filenames:
            stale_file.unlink()

    for key in keys:
        filename = Path(key).name
        local_path = run_dir / filename
        # Download afresh so the local cache cannot silently diverge from Bronze.
        s3.download_file(bucket, key, str(local_path))

        expected_hash = expected_hashes.get(key)
        actual_hash = sha256_file(local_path)
        if expected_hash and actual_hash.lower() != expected_hash.lower():
            raise RuntimeError(
                f"Local cache checksum mismatch for s3://{bucket}/{key}. "
                "Delete that cached file and rerun the builder."
            )

        raw_count += validate_page(local_path)

    expected_records = manifest.get("raw_record_count")
    if expected_records is None:
        expected_records = manifest.get("records_collected")
    if expected_records is not None and int(expected_records) != raw_count:
        raise RuntimeError(
            f"Raw-record count mismatch for {extraction_id}: "
            f"manifest={expected_records}, downloaded pages={raw_count}."
        )

    reported_total = manifest.get("api_reported_total")
    if reported_total is None:
        reported_totals = manifest.get("api_total_values_reported")
        if isinstance(reported_totals, list) and len(reported_totals) == 1:
            reported_total = reported_totals[0]
    if reported_total is not None and int(reported_total) != raw_count:
        raise RuntimeError(
            f"API-reported total mismatch for {extraction_id}: "
            f"manifest={reported_total}, downloaded pages={raw_count}."
        )

    retrieved_at_utc = parse_extraction_time(extraction_id).isoformat().replace("+00:00", "Z")
    bronze_uploaded_at_utc = manifest.get("uploaded_at_utc") or retrieved_at_utc
    if not isinstance(bronze_uploaded_at_utc, str):
        raise RuntimeError(f"Invalid uploaded_at_utc in Bronze manifest for {extraction_id}.")
    try:
        parsed_upload_time = datetime.fromisoformat(
            bronze_uploaded_at_utc.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid uploaded_at_utc in Bronze manifest for {extraction_id}: "
            f"{bronze_uploaded_at_utc!r}."
        ) from exc
    if parsed_upload_time.tzinfo is None:
        raise RuntimeError(
            f"Bronze uploaded_at_utc must include a timezone for {extraction_id}."
        )
    bronze_uploaded_at_utc = parsed_upload_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    summary = {
        "extraction_id": extraction_id,
        "retrieved_at_utc": retrieved_at_utc,
        "bronze_uploaded_at_utc": bronze_uploaded_at_utc,
        "page_count": len(keys),
        "raw_record_count": raw_count,
        "s3_manifest_key": manifest_key,
        "local_data_dir": str(run_dir),
        "manifest_reported_raw_record_count": expected_records,
    }
    return run_dir, summary


def build_spark_dataframe(
    spark: Any,
    extraction_id: str,
    page_files: list[Path],
    ingested_at_utc: str,
) -> Any:
    from pyspark.sql import functions as F

    try:
        ingested_at_value = datetime.fromisoformat(
            ingested_at_utc.replace("Z", "+00:00")
        ).astimezone(timezone.utc).replace(tzinfo=None)
    except (AttributeError, ValueError) as exc:
        raise RuntimeError(
            f"Invalid UTC ingestion timestamp for extraction {extraction_id}: {ingested_at_utc!r}."
        ) from exc

    if not page_files:
        raise RuntimeError(f"No cached JSON pages available for extraction {extraction_id}.")

    page_paths = [str(path.resolve()) for path in page_files]
    envelopes = (
        spark.read.option("multiLine", "true")
        .option("mode", "FAILFAST")
        .json(page_paths)
    )

    if "records" not in envelopes.columns:
        raise RuntimeError(
            f"Spark did not find a top-level 'records' field for extraction {extraction_id}."
        )

    raw = (
        envelopes.withColumn("_source_path", F.input_file_name())
        .select(
            F.explode_outer(F.col("records")).alias("_record"),
            F.regexp_extract(F.col("_source_path"), r"([^/]+)$", 1).alias("source_page_file"),
        )
        .filter(F.col("_record").isNotNull())
    )

    selected = [F.col(f"_record.{field}").cast("string").alias(field) for field in EXPECTED_SOURCE_FIELDS]
    raw = raw.select("source_page_file", *selected)

    # Hash the complete raw source record before converting NA/blank tokens to null.
    # This keeps the source-level duplicate definition separate from cleaned values.
    raw_json = F.to_json(
        F.struct(*[F.col(field).alias(field) for field in EXPECTED_SOURCE_FIELDS]),
        options={"ignoreNullFields": "false"},
    )
    raw = raw.withColumn("source_record_hash", F.sha2(raw_json, 256))
    raw = raw.withColumn("extraction_id", F.lit(extraction_id))

    # Deduplicate only within this extraction. Never collapse records across runs.
    aggregate_exprs = [F.first(F.col(field), ignorenulls=False).alias(field) for field in EXPECTED_SOURCE_FIELDS]
    aggregate_exprs.extend(
        [
            F.count(F.lit(1)).cast("long").alias("source_occurrence_count"),
            F.sort_array(F.collect_set("source_page_file")).alias("source_pages"),
        ]
    )
    deduplicated = raw.groupBy("extraction_id", "source_record_hash").agg(*aggregate_exprs)
    deduplicated = deduplicated.withColumn(
        "duplicate_excess_count", (F.col("source_occurrence_count") - F.lit(1)).cast("long")
    )

    retrieved_at = F.to_timestamp(F.lit(extraction_id), "yyyyMMdd'T'HHmmss'Z'")
    deduplicated = (
        deduplicated.withColumn("retrieved_at_utc", retrieved_at)
        .withColumn("ingestion_date", F.date_format(F.col("retrieved_at_utc"), "yyyy-MM-dd"))
        .withColumn("source_last_update_raw", F.col("last_update"))
        .drop("last_update")
        .withColumn("latitude_raw", F.col("latitude"))
        .withColumn("longitude_raw", F.col("longitude"))
    )

    for field in MEASUREMENT_FIELDS:
        raw_field = f"{field}_raw"
        deduplicated = deduplicated.withColumnRenamed(field, raw_field)
        cleaned = F.when(
            F.col(raw_field).isNull()
            | F.lower(F.trim(F.col(raw_field))).isin(*sorted(NULL_TOKENS)),
            F.lit(None).cast("string"),
        ).otherwise(F.trim(F.col(raw_field)))
        deduplicated = deduplicated.withColumn(field, F.expr(f"try_cast({raw_field} as double)"))
        deduplicated = deduplicated.withColumn(f"_{field}_clean", cleaned)

    invalid_measurement = None
    valid_measure_count = None
    for field in MEASUREMENT_FIELDS:
        invalid_this = F.col(f"_{field}_clean").isNotNull() & F.col(field).isNull()
        valid_this = F.col(field).isNotNull()
        invalid_measurement = invalid_this if invalid_measurement is None else (invalid_measurement | invalid_this)
        valid_measure_count = (
            F.when(valid_this, F.lit(1)).otherwise(F.lit(0))
            if valid_measure_count is None
            else valid_measure_count + F.when(valid_this, F.lit(1)).otherwise(F.lit(0))
        )

    deduplicated = deduplicated.withColumn("_valid_measure_count", valid_measure_count)
    deduplicated = deduplicated.withColumn(
        "measurement_status",
        F.when(invalid_measurement, F.lit("INVALID_NUMERIC"))
        .when(F.col("_valid_measure_count") == 0, F.lit("UNAVAILABLE"))
        .when(F.col("_valid_measure_count") == len(MEASUREMENT_FIELDS), F.lit("AVAILABLE"))
        .otherwise(F.lit("PARTIAL")),
    )

    deduplicated = (
        deduplicated.withColumn("latitude", F.expr("try_cast(latitude_raw as double)"))
        .withColumn("longitude", F.expr("try_cast(longitude_raw as double)"))
        .withColumn(
            "coordinate_status",
            F.when(
                F.col("latitude").between(-90.0, 90.0)
                & F.col("longitude").between(-180.0, 180.0),
                F.lit("VALID"),
            ).otherwise(F.lit("INVALID")),
        )
    )

    norm_country = F.lower(F.trim(F.coalesce(F.col("country"), F.lit(""))))
    norm_state = F.lower(F.trim(F.coalesce(F.col("state"), F.lit(""))))
    norm_city = F.lower(F.trim(F.coalesce(F.col("city"), F.lit(""))))
    norm_station = F.lower(F.trim(F.coalesce(F.col("station"), F.lit(""))))
    norm_pollutant = F.upper(F.trim(F.coalesce(F.col("pollutant_id"), F.lit(""))))
    station_identity = F.concat_ws("||", norm_country, norm_state, norm_city, norm_station)
    entity_identity = F.concat_ws("||", station_identity, norm_pollutant)

    deduplicated = (
        deduplicated.withColumn("candidate_station_key", F.sha2(station_identity, 256))
        .withColumn("candidate_entity_key", F.sha2(entity_identity, 256))
        .withColumn("source_page_count", F.size(F.col("source_pages")))
        # Use the immutable Bronze manifest timestamp, not Spark's current time,
        # so replaying the same extraction produces the same record values.
        .withColumn("ingested_at_utc", F.lit(ingested_at_value).cast("timestamp"))
        .drop("_valid_measure_count", "_min_value_clean", "_max_value_clean", "_avg_value_clean")
    )

    # Reorder columns into a stable documented schema. ingestion_date is the
    # partition column and will be stored in the Parquet directory path.
    return deduplicated.select(
        "extraction_id",
        "retrieved_at_utc",
        "ingestion_date",
        "source_last_update_raw",
        "country",
        "state",
        "city",
        "station",
        "candidate_station_key",
        "candidate_entity_key",
        "pollutant_id",
        "latitude_raw",
        "longitude_raw",
        "latitude",
        "longitude",
        "coordinate_status",
        "min_value_raw",
        "max_value_raw",
        "avg_value_raw",
        "min_value",
        "max_value",
        "avg_value",
        "measurement_status",
        "source_record_hash",
        "source_occurrence_count",
        "duplicate_excess_count",
        "source_pages",
        "source_page_count",
        "ingested_at_utc",
    )


def dataframe_run_summary(df: Any) -> dict[str, Any]:
    from pyspark.sql import functions as F

    summary_row = df.agg(
        F.count(F.lit(1)).alias("unique_record_count"),
        F.sum("duplicate_excess_count").alias("duplicate_excess_count"),
        F.countDistinct("candidate_station_key").alias("station_count"),
        F.countDistinct("city").alias("city_count"),
        F.countDistinct("pollutant_id").alias("pollutant_count"),
        F.sum(F.when(F.col("measurement_status") == "AVAILABLE", 1).otherwise(0)).alias("available_count"),
        F.sum(F.when(F.col("measurement_status") == "UNAVAILABLE", 1).otherwise(0)).alias("unavailable_count"),
        F.sum(F.when(F.col("measurement_status") == "PARTIAL", 1).otherwise(0)).alias("partial_count"),
        F.sum(F.when(F.col("measurement_status") == "INVALID_NUMERIC", 1).otherwise(0)).alias("invalid_numeric_count"),
        F.sum(F.when(F.col("coordinate_status") == "VALID", 1).otherwise(0)).alias("valid_coordinate_count"),
        F.sum(F.when(F.col("coordinate_status") == "INVALID", 1).otherwise(0)).alias("invalid_coordinate_count"),
    ).first()

    result = {key: summary_row[key] for key in summary_row.asDict()}
    status_rows = df.groupBy("measurement_status").count().collect()
    result["measurement_status_counts"] = {row["measurement_status"]: row["count"] for row in status_rows}
    return result


def main() -> int:
    args = parse_args()

    try:
        extraction_ids = list(dict.fromkeys(args.extraction_ids))
        for extraction_id in extraction_ids:
            validate_extraction_id(extraction_id)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.output_partitions <= 0:
        print("ERROR: --output-partitions must be greater than zero.", file=sys.stderr)
        return 2

    try:
        args.output_dir = prepare_output_directory(
            args.output_dir,
            overwrite=args.overwrite_output,
        )
    except (OSError, ValueError) as exc:
        print(f"ERROR: unsafe or unavailable output directory: {exc}", file=sys.stderr)
        return 2

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
            f"ERROR: Cannot access S3 bucket {args.bucket} (AWS error: {code}).\n"
            f"For an expired SSO session run: aws sso login --profile {args.profile}",
            file=sys.stderr,
        )
        return 2

    downloaded: list[tuple[str, Path, dict[str, Any]]] = []
    try:
        for extraction_id in extraction_ids:
            print(f"\nDownloading and validating Bronze extraction: {extraction_id}")
            run_dir, run_info = download_extraction(s3, args.bucket, extraction_id, args.cache_dir)
            page_files = sorted(run_dir.glob("*.json"))
            downloaded.append((extraction_id, run_dir, run_info))
            print(f"  Pages downloaded : {len(page_files)}")
            print(f"  Raw records      : {run_info['raw_record_count']:,}")
            print(f"  Retrieved at UTC: {run_info['retrieved_at_utc']}")
    except (ClientError, OSError, RuntimeError, ValueError) as exc:
        print(f"\nERROR during Bronze download/validation: {exc}", file=sys.stderr)
        return 1

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        print(
            "ERROR: PySpark is not installed in the active environment. Activate your project .venv.",
            file=sys.stderr,
        )
        return 2

    spark = (
        SparkSession.builder.appName("CPCBHistoricalSilver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.speculation", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    run_frames: list[Any] = []
    run_reports: list[dict[str, Any]] = []
    try:
        for extraction_id, run_dir, run_info in downloaded:
            page_files = sorted(run_dir.glob("*.json"))
            raw_df = build_spark_dataframe(
                spark,
                extraction_id,
                page_files,
                run_info["bronze_uploaded_at_utc"],
            )
            raw_df.cache()
            unique_count = raw_df.count()
            run_report = dataframe_run_summary(raw_df)
            run_report.update(run_info)
            run_report["spark_unique_record_count"] = unique_count
            run_reports.append(run_report)
            run_frames.append(raw_df)
            print(f"\nExtraction validation: {extraction_id}")
            print(f"  Raw records                  : {run_info['raw_record_count']:,}")
            print(f"  Unique exact records         : {unique_count:,}")
            print(f"  Exact duplicate excess       : {int(run_report['duplicate_excess_count'] or 0):,}")
            print(f"  Cities / stations / pollutants: {run_report['city_count']} / {run_report['station_count']} / {run_report['pollutant_count']}")
            print(f"  Measurement statuses         : {run_report['measurement_status_counts']}")

        historical = run_frames[0]
        for frame in run_frames[1:]:
            historical = historical.unionByName(frame)

        # This is a quality check, not a deduplication step: source records from
        # separate extraction IDs must remain separate even when entities match.
        conflict_groups = (
            historical.groupBy(
                "extraction_id",
                "candidate_entity_key",
                "source_last_update_raw",
            )
            .agg(F.countDistinct("source_record_hash").alias("distinct_reading_records"))
            .filter(F.col("distinct_reading_records") > 1)
        )
        conflict_count = conflict_groups.count()

        overall_report = {
            "build_created_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "dataset": "cpcb_observation_snapshots",
            "grain": "one extraction x candidate station-pollutant entity x source record",
            "deduplication_scope": "exact source records within each extraction_id only",
            "source_extractions": run_reports,
            "combined_unique_record_count": historical.count(),
            "combined_duplicate_excess_count": sum(int(r["duplicate_excess_count"] or 0) for r in run_reports),
            "conflicting_timestamped_entity_groups": conflict_count,
            "cross_extraction_deduplication_performed": False,
            "source_last_update_timezone_assigned": False,
            "notes": [
                "retrieved_at_utc is derived from the UTC extraction ID, not from CPCB last_update.",
                "ingested_at_utc is sourced from the immutable Bronze manifest upload timestamp; legacy manifests without that field fall back to retrieved_at_utc.",
                "source_last_update_raw is retained as supplied; no timezone is inferred.",
                "candidate station keys are built from source names and are not official CPCB station IDs.",
                "exact duplicates are collapsed only within the same extraction ID.",
                "this build does not calculate official CPCB AQI.",
            ],
        }

        # Prevent small local Windows writes from producing excess output files.
        args.output_dir.parent.mkdir(parents=True, exist_ok=True)
        (
            historical.repartition(args.output_partitions, "candidate_entity_key")
            .sortWithinPartitions("candidate_entity_key", "source_record_hash")
            .write.mode("errorifexists")
            .partitionBy("ingestion_date")
            .parquet(str(args.output_dir))
        )

        report_path = args.output_dir.parent / f"{args.output_dir.name}_quality_report.json"
        report_path.write_text(json.dumps(overall_report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

        print("\n" + "=" * 76)
        print("HISTORICAL SILVER BUILD COMPLETE (LOCAL ONLY)")
        print("=" * 76)
        print(f"Output dataset : {args.output_dir}")
        print(f"Quality report : {report_path}")
        print(f"Combined rows  : {overall_report['combined_unique_record_count']:,}")
        print(f"Duplicate rows removed within runs: {overall_report['combined_duplicate_excess_count']:,}")
        print(f"Conflicting timestamped entity groups: {conflict_count}")
        print("Cross-extraction deduplication: NONE")
        print("S3 upload: NOT PERFORMED")
        print("Existing Silver table/data: NOT MODIFIED")

        print(f"Bronze download cache retained: {args.cache_dir}")

    except Exception as exc:
        print(f"\nERROR during historical Silver build: {exc}", file=sys.stderr)
        return 1
    finally:
        for frame in run_frames:
            try:
                frame.unpersist()
            except Exception:
                pass
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
