
import hashlib
import json
import sys

from datetime import datetime, timezone
from pathlib import Path

import boto3

from botocore.exceptions import ClientError, BotoCoreError


# ==========================================
# 1. CONFIGURATION
# ==========================================

PROFILE = "india-aqi-admin"
REGION = "ap-south-1"

EXTRACTION_ID = "20261116T194631Z"

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RUN_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "runs"
    / EXTRACTION_ID
)

PARQUET_DIR = (
    RUN_DIR
    / "silver"
    / "cpcb_observations"
)

QUALITY_REPORT = (
    RUN_DIR
    / "metadata"
    / "quality_report.json"
)

SILVER_PREFIX = "silver/cpcb_observations/"

METADATA_PREFIX = (
    f"metadata/silver_runs/"
    f"extraction_id={EXTRACTION_ID}/"
)


# ==========================================
# 2. AWS SESSION
# ==========================================

session = boto3.Session(
    profile_name=PROFILE,
    region_name=REGION
)

identity = session.client("sts").get_caller_identity()

ACCOUNT_ID = identity["Account"]

BUCKET_NAME = (
    f"india-aqi-lake-{ACCOUNT_ID}-{REGION}"
)

s3 = session.client("s3", region_name=REGION)


# ==========================================
# 3. SHA-256 CHECKSUM
# ==========================================

def calculate_sha256(file_path):

    digest = hashlib.sha256()

    with file_path.open("rb") as file:

        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b""
        ):
            digest.update(chunk)

    return digest.hexdigest()


# ==========================================
# 4. VERIFY REMOTE OBJECT
# ==========================================

def verify_remote_object(key, expected_checksum, expected_size):

    response = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=key
    )

    remote_hash = hashlib.sha256()

    try:

        for chunk in response["Body"].iter_chunks(
            chunk_size=1024 * 1024
        ):

            if chunk:
                remote_hash.update(chunk)

    finally:

        response["Body"].close()

    actual_checksum = remote_hash.hexdigest()

    actual_size = response["ContentLength"]

    if actual_checksum != expected_checksum:
        raise RuntimeError(
            f"SHA-256 mismatch: {key}"
        )

    if actual_size != expected_size:
        raise RuntimeError(
            f"File size mismatch: {key}"
        )

    return True


# ==========================================
# 5. UPLOAD AND VERIFY
# ==========================================

def upload_and_verify(file_path, key, content_type):

    local_checksum = calculate_sha256(file_path)

    local_size = file_path.stat().st_size

    try:

        existing = s3.head_object(
            Bucket=BUCKET_NAME,
            Key=key
        )

    except ClientError as exc:

        code = exc.response["Error"]["Code"]

        if code not in ("404", "NoSuchKey", "NotFound"):
            raise

        existing = None

    if existing:

        metadata = existing.get("Metadata", {})

        if (
            metadata.get("sha256") != local_checksum
            or existing["ContentLength"] != local_size
        ):

            raise RuntimeError(
                f"Existing object differs. "
                f"Refusing to overwrite: {key}"
            )

        print("EXISTING OBJECT:", key)

    else:

        s3.upload_file(
            str(file_path),
            BUCKET_NAME,
            key,
            ExtraArgs={
                "ContentType": content_type,
                "ServerSideEncryption": "AES256",
                "Metadata": {
                    "sha256": local_checksum,
                    "extraction-id": EXTRACTION_ID,
                    "project": "india-aqi-data-lake"
                }
            }
        )

        print("UPLOADED:", key)

    verify_remote_object(
        key,
        local_checksum,
        local_size
    )

    print("CHECKSUM VERIFIED:", file_path.name)

    return {
        "s3_key": key,
        "size_bytes": local_size,
        "sha256": local_checksum
    }


# ==========================================
# 6. MAIN EXECUTION
# ==========================================

def main():

    if not PARQUET_DIR.exists():
        raise FileNotFoundError(PARQUET_DIR)

    if not QUALITY_REPORT.exists():
        raise FileNotFoundError(QUALITY_REPORT)

    # Ensure Spark completed its write operation.

    if not (PARQUET_DIR / "_SUCCESS").exists():
        raise RuntimeError(
            "Spark _SUCCESS marker is missing."
        )

    parquet_files = sorted(
        PARQUET_DIR.rglob("*.parquet")
    )

    if not parquet_files:
        raise RuntimeError("No Parquet files found.")

    with QUALITY_REPORT.open(
        encoding="utf-8"
    ) as file:
        report = json.load(file)

    expected_count = report["silver_records_written"]

    if expected_count != 3676:
        raise RuntimeError(
            f"Unexpected Silver count: {expected_count}"
        )

    print("\n" + "=" * 55)
    print("INDIA AQI - SILVER S3 UPLOAD")
    print("=" * 55)

    print("Bucket:", BUCKET_NAME)
    print("Parquet files:", len(parquet_files))
    print("Expected records:", expected_count)

    uploaded_files = []

    # Upload Parquet files while preserving
    # ingestion_date partition directories.

    for file_path in parquet_files:

        relative_path = file_path.relative_to(
            PARQUET_DIR
        ).as_posix()

        key = SILVER_PREFIX + relative_path

        result = upload_and_verify(
            file_path,
            key,
            "application/octet-stream"
        )

        uploaded_files.append(result)

    # Upload quality report outside the table location.

    quality_key = (
        METADATA_PREFIX + "quality_report.json"
    )

    quality_result = upload_and_verify(
        QUALITY_REPORT,
        quality_key,
        "application/json"
    )

    # Build upload manifest.

    manifest = {
        "project": "India Air Quality Data Lake",
        "extraction_id": EXTRACTION_ID,
        "uploaded_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "bucket": BUCKET_NAME,
        "region": REGION,

        "silver_prefix": SILVER_PREFIX,

        "source_records": report["source_records_read"],
        "silver_records": expected_count,

        "duplicate_excess_rows_removed": (
            report["exact_duplicate_excess_rows_removed"]
        ),

        "measurement_status_counts": (
            report["measurement_status_counts"]
        ),

        "parquet_files": uploaded_files,

        "quality_report": quality_result
    }

    local_manifest = (
        RUN_DIR
        / "metadata"
        / "silver_upload_manifest.json"
    )

    local_manifest.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with local_manifest.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            manifest,
            file,
            indent=2,
            ensure_ascii=False
        )

    manifest_key = (
        METADATA_PREFIX + "manifest.json"
    )

    manifest_result = upload_and_verify(
        local_manifest,
        manifest_key,
        "application/json"
    )

    # Final verification.

    print("\n" + "=" * 55)
    print("SILVER UPLOAD VERIFICATION")
    print("=" * 55)

    print("Parquet files uploaded:", len(uploaded_files))
    print("Records represented:", expected_count)
    print("Quality report:", "Verified")
    print("Upload manifest:", "Verified")

    total_bytes = sum(
        item["size_bytes"]
        for item in uploaded_files
    )

    print("Total Parquet bytes:", total_bytes)

    print("\nSILVER LAYER UPLOAD COMPLETED!")

    print(
        f"s3://{BUCKET_NAME}/{SILVER_PREFIX}"
    )


if __name__ == "__main__":

    try:
        main()

    except (ClientError, BotoCoreError) as exc:

        print(
            "\nAWS ERROR:",
            type(exc).__name__
        )

        sys.exit(1)

    except Exception as exc:

        print("\nUPLOAD ERROR:", str(exc))

        sys.exit(1)
