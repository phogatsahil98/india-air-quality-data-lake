
import os
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter

import requests

from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ================================================
# 1. CONFIGURATION
# ================================================

load_dotenv()

API_KEY = os.getenv("DATA_GOV_API_KEY")

BASE_URL = (
    "https://api.data.gov.in/resource/"
    "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
)

PAGE_SIZE = 100
MAX_PAGES = 1000

NUMERIC_FIELDS = [
    "latitude",
    "longitude",
    "min_value",
    "max_value",
    "avg_value",
]

NATURAL_KEY_FIELDS = [
    "country",
    "state",
    "city",
    "station",
    "pollutant_id",
    "last_update",
]


# ================================================
# 2. VALIDATE ENVIRONMENT
# ================================================

if not API_KEY:
    raise ValueError(
        "DATA_GOV_API_KEY not found in .env"
    )


# ================================================
# 3. HTTP SESSION AND RETRIES
# ================================================

retry_strategy = Retry(
    total=4,
    connect=4,
    read=4,
    status=4,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET"]),
    respect_retry_after_header=True,
)

adapter = HTTPAdapter(
    max_retries=retry_strategy
)

session = requests.Session()

session.mount("https://", adapter)


# ================================================
# 4. FETCH ONE PAGE
# ================================================

def fetch_page(offset):

    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": PAGE_SIZE,
        "offset": offset,
    }

    response = session.get(
        BASE_URL,
        params=params,
        timeout=(15, 90),
        headers={
            "Accept": "application/json",
            "User-Agent": "India-AQI-Data-Lake/1.0",
        },
    )

    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("Unexpected API response.")

    if payload.get("error"):
        raise RuntimeError("API reported an error.")

    records = payload.get("records")

    if not isinstance(records, list):
        raise ValueError("Records array missing.")

    return payload, records


# ================================================
# 5. FETCH ALL PAGES AND PRESERVE RAW RESPONSES
# ================================================

def fetch_all_pages():

    run_id = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    output_dir = Path(
        "data/raw"
    ) / f"source_profile_{run_id}"

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    all_records = []

    offset = 0
    page_number = 0
    reported_total = None

    print("\nStarting full dataset extraction...")
    print("Page size:", PAGE_SIZE)

    while page_number < MAX_PAGES:

        payload, records = fetch_page(offset)

        if reported_total is None:
            reported_total = int(
                payload.get("total") or 0
            )

            print("API reported total:", reported_total)

        # Save the complete original API envelope.
        output_file = output_dir / (
            f"page_offset_{offset:06d}.json"
        )

        with output_file.open(
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                payload,
                file,
                indent=2,
                ensure_ascii=False
            )

        if not records:
            print("No additional records.")
            break

        all_records.extend(records)

        print(
            f"Page {page_number + 1}: "
            f"Offset={offset}, "
            f"Records={len(records)}, "
            f"Collected={len(all_records)}"
        )

        offset += len(records)
        page_number += 1

        # Stop when we've reached the initially reported total.
        if reported_total and offset >= reported_total:
            break

        # For an API without a usable total, a short page
        # indicates the end of the result set.
        if not reported_total and len(records) < PAGE_SIZE:
            break

        # Avoid sending rapid consecutive requests.
        time.sleep(0.5)

    else:
        raise RuntimeError("Maximum page safety limit reached.")

    print("\nExtraction completed.")
    print("Pages fetched:", page_number)
    print("Records collected:", len(all_records))
    print("Raw pages saved in:", output_dir)

    return all_records


# ================================================
# 6. DATA QUALITY HELPERS
# ================================================

def is_missing(value):

    return (
        value is None
        or (
            isinstance(value, str)
            and not value.strip()
        )
    )


def is_valid_numeric(value):

    if is_missing(value):
        return False

    try:
        float(value)
        return True

    except (ValueError, TypeError):
        return False


def unique_values(records, field):

    return sorted({
        str(record[field]).strip()
        for record in records
        if not is_missing(record.get(field))
    })


# ================================================
# 7. PROFILE COMPLETE DATASET
# ================================================

def profile_dataset(records):

    print("\n" + "=" * 60)
    print("FULL CPCB DATASET PROFILE")
    print("=" * 60)

    if not records:
        print("No records available.")
        return

    print("\nTOTAL RECORDS:", len(records))

    fields = sorted({
        field
        for record in records
        for field in record.keys()
    })

    print("\nFIELDS:", fields)

    dimensions = [
        "country",
        "state",
        "city",
        "station",
        "pollutant_id",
    ]

    print("\nGEOGRAPHICAL AND POLLUTANT COVERAGE")

    for field in dimensions:

        values = unique_values(records, field)

        print(
            f"{field}: {len(values)} distinct values"
        )

        if field in [
            "state",
            "city",
            "pollutant_id",
        ]:
            print(" ", values)

    # Pollutant observation counts
    print("\nPOLLUTANT DISTRIBUTION")

    pollutant_counts = Counter(
        record.get("pollutant_id")
        for record in records
        if not is_missing(record.get("pollutant_id"))
    )

    for pollutant, count in sorted(
        pollutant_counts.items()
    ):
        print(f"{pollutant}: {count}")

    # Missing values
    print("\nMISSING VALUE PROFILE")

    for field in fields:

        missing = sum(
            is_missing(record.get(field))
            for record in records
        )

        print(
            f"{field}: {missing}/{len(records)}"
        )

    # Invalid numeric values
    print("\nNUMERIC VALIDATION")

    for field in NUMERIC_FIELDS:

        invalid = sum(
            not is_valid_numeric(record.get(field))
            for record in records
        )

        print(
            f"{field}: {invalid} invalid/missing values"
        )

    # Candidate duplicate detection
    keys = [
        tuple(
            str(record.get(field) or "").strip()
            for field in NATURAL_KEY_FIELDS
        )
        for record in records
    ]

    duplicates = len(keys) - len(set(keys))

    print("\nCANDIDATE DUPLICATE RECORDS:", duplicates)

    print("\nSAMPLE RECORD:")

    print(
        json.dumps(
            records[0],
            indent=2,
            ensure_ascii=False
        )
    )


# ================================================
# 8. EXECUTION
# ================================================

if __name__ == "__main__":

    try:

        records = fetch_all_pages()

        profile_dataset(records)

    except requests.exceptions.RequestException as exc:

        # Do not print the full request URL:
        # it contains the API key.
        print(
            "\nAPI request failed:",
            type(exc).__name__
        )

        raise SystemExit(1)

    except (ValueError, RuntimeError) as exc:

        print("\nExtraction error:", str(exc))
        raise SystemExit(1)
