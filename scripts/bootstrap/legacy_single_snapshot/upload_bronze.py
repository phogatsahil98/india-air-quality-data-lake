
import hashlib
import json
import sys

from pathlib import Path

import boto3

from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError


# ==================================================
# 1. CONFIGURATION
# ==================================================

PROFILE = "india-aqi-admin"
REGION = "ap-south-1"

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "source_profile_20261116T194631Z"
)

RESOURCE_ID = (
    "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
)

# ==================================================
# 2. AWS SESSION
# ==================================================

session = boto3.Session(
    profile_name=PROFILE,
    region_name=REGION
)

sts = session.client("sts")

identity = sts.get_caller_identity()

account_id = identity["Account"]

BUCKET_NAME = (
    f"india-aqi-lake-{account_id}-{REGION}"
)

s3 = session.client(
    "s3",
    region_name=REGION,
    config=Config(
        retries={
            "max_attempts": 5,
            "mode": "standard"
        }
    )
)


# ==================================================
# 3. S3 PREFIX CONFIGURATION
# ==================================================

EXTRACTION_ID = SOURCE_DIR.name.replace(
    "source_profile_", ""
)

SNAPSHOT_PREFIX = (
    f"bronze/cpcb/snapshots/"
    f"extraction_id={EXTRACTION_ID}/"
)

DATA_PREFIX = SNAPSHOT_PREFIX + "data/"

MANIFEST_KEY = (
    SNAPSHOT_PREFIX + "_metadata/manifest.json"
)


# ==================================================
# 4. SHA-256 CHECKSUM
# ==================================================

def calculate_sha256(file_path):

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:

        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b""
        ):
            sha256.update(chunk)

    return sha256.hexdigest()


# ==================================================
# 5. CHECK EXISTING S3 OBJECT
# ==================================================

def get_existing_object(key):

    try:

        return s3.head_object(
            Bucket=BUCKET_NAME,
            Key=key
        )

    except ClientError as exc:

        code = exc.response["Error"]["Code"]

        if code in ("404", "NoSuchKey", "NotFound"):

            return None

        raise


# ==================================================
# 6. UPLOAD ONE RAW FILE
# ==================================================

def upload_file(file_path, key):

    checksum = calculate_sha256(file_path)

    existing = get_existing_object(key)

    if existing:

        existing_checksum = (
            existing.get("Metadata", {}).get("sha256")
        )

        if existing_checksum == checksum:

            print(
                f"SKIPPED (already verified): "
                f"{file_path.name}"
            )

            return checksum

        raise RuntimeError(
            f"Existing S3 object differs from local file: {key}. "
            "Refusing to overwrite the object."
        )

    s3.upload_file(
        str(file_path),
        BUCKET_NAME,
        key,
        ExtraArgs={
            "ContentType": "application/json",
            "ServerSideEncryption": "AES256",
            "Metadata": {
                "sha256": checksum,
                "source": "cpcb-data-gov-in",
                "extraction-id": EXTRACTION_ID
            }
        }
    )

    # Verify uploaded object.
    uploaded = s3.head_object(
        Bucket=BUCKET_NAME,
        Key=key
    )

    remote_checksum = (
        uploaded.get("Metadata", {}).get("sha256")
    )

    if remote_checksum != checksum:

        raise RuntimeError(
            f"Checksum verification failed: {key}"
        )

    if uploaded["ContentLength"] != file_path.stat().st_size:

        raise RuntimeError(
            f"File size mismatch: {key}"
        )

    print(f"UPLOADED: {file_path.name}")

    return checksum


# ==================================================
# 7. BUILD EXTRACTION MANIFEST
# ==================================================

def create_manifest(files, page_details):

    total_records = sum(
        item["records"] for item in page_details
    )

    manifest = {
        "project": "India Air Quality Data Lake",
        "source": "CPCB via data.gov.in",
        "resource_id": RESOURCE_ID,
        "bucket": BUCKET_NAME,
        "region": REGION,
        "extraction_id": EXTRACTION_ID,
        "snapshot_prefix": SNAPSHOT_PREFIX,
        "api_reported_total": (
            page_details[0]["api_reported_total"]
        ),
        "records_collected": total_records,
        "page_count": len(files),
        "files": page_details
    }

    return manifest


# ==================================================
# 8. UPLOAD MANIFEST
# ==================================================

def upload_manifest(manifest):

    manifest_bytes = json.dumps(
        manifest,
        indent=2,
        ensure_ascii=False
    ).encode("utf-8")

    checksum = hashlib.sha256(
        manifest_bytes
    ).hexdigest()

    existing = get_existing_object(MANIFEST_KEY)

    if existing:

        existing_checksum = (
            existing.get("Metadata", {}).get("sha256")
        )

        if existing_checksum == checksum:

            print("\nManifest already verified.")

            return

        raise RuntimeError(
            "Existing manifest differs. "
            "Refusing to overwrite it."
        )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=MANIFEST_KEY,
        Body=manifest_bytes,
        ContentType="application/json",
        ServerSideEncryption="AES256",
        Metadata={
            "sha256": checksum
        }
    )

    print("\nManifest uploaded successfully.")


# ==================================================
# 9. VERIFY S3 OBJECT COUNT
# ==================================================

def verify_upload(expected_count):

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    uploaded_objects = []

    for page in paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=DATA_PREFIX
    ):

        uploaded_objects.extend(
            page.get("Contents", [])
        )

    actual_count = len(uploaded_objects)

    print("\n" + "=" * 55)
    print("BRONZE UPLOAD VERIFICATION")
    print("=" * 55)

    print("Bucket:", BUCKET_NAME)
    print("S3 data prefix:", DATA_PREFIX)

    print("Expected files:", expected_count)
    print("Uploaded files:", actual_count)

    print(
        "Total data size:",
        sum(obj["Size"] for obj in uploaded_objects),
        "bytes"
    )

    if actual_count != expected_count:

        raise RuntimeError(
            "S3 object count does not match local file count."
        )

    print("\nALL RAW FILES VERIFIED SUCCESSFULLY!")


# ==================================================
# 10. MAIN EXECUTION
# ==================================================

def main():

    if not SOURCE_DIR.exists():

        raise FileNotFoundError(
            f"Source directory not found: {SOURCE_DIR}"
        )

    files = sorted(
        SOURCE_DIR.glob("page_offset_*.json")
    )

    if not files:

        raise RuntimeError(
            "No raw JSON pages found."
        )

    print("\n" + "=" * 55)
    print("INDIA AQI - BRONZE INGESTION")
    print("=" * 55)

    print("AWS account verified:", bool(account_id))
    print("Bucket:", BUCKET_NAME)
    print("Source files:", len(files))

    page_details = []

    # Upload each original JSON page.
    for index, file_path in enumerate(files, start=1):

        with file_path.open(
            encoding="utf-8"
        ) as file:

            payload = json.load(file)

        records = payload.get("records", [])

        key = DATA_PREFIX + file_path.name

        checksum = upload_file(
            file_path,
            key
        )

        page_details.append({
            "file_name": file_path.name,
            "s3_key": key,
            "records": len(records),
            "size_bytes": file_path.stat().st_size,
            "sha256": checksum,
            "api_reported_total": int(
                payload.get("total") or 0
            )
        })

        print(
            f"Progress: {index}/{len(files)}"
        )

    # Build and upload extraction manifest.
    manifest = create_manifest(
        files,
        page_details
    )

    upload_manifest(manifest)

    # Verify all source data objects.
    verify_upload(len(files))

    print("\nBRONZE LAYER INGESTION COMPLETED.")
    print(
        f"s3://{BUCKET_NAME}/{SNAPSHOT_PREFIX}"
    )


# ==================================================
# 11. ENTRY POINT
# ==================================================

if __name__ == "__main__":

    try:

        main()

    except (ClientError, BotoCoreError) as exc:

        print("\nAWS ERROR:", type(exc).__name__)
        print("Check SSO session and S3 permissions.")

        sys.exit(1)

    except Exception as exc:

        print("\nINGESTION ERROR:", str(exc))

        sys.exit(1)
