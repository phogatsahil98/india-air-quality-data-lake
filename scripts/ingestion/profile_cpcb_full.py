#!/usr/bin/env python3
"""Extract and profile the CPCB real-time air-quality API snapshot.

The script saves every original API response page under:
    data/raw/source_profile_<EXTRACTION_ID>/

The extraction ID is a UTC timestamp. The source's ``last_update`` field is
preserved inside raw JSON and is not interpreted as a UTC timestamp.

This script is an extraction utility; uploading to S3 is handled separately by
``upload_bronze_dynamic.py``.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger("india_aqi.ingestion")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data" / "raw"
BASE_URL = (
    "https://api.data.gov.in/resource/"
    "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
)

PAGE_SIZE = 100
MAX_PAGES = 1_000
REQUEST_TIMEOUT = (15, 90)
REQUEST_DELAY_SECONDS = 0.5

NUMERIC_FIELDS = (
    "latitude",
    "longitude",
    "min_value",
    "max_value",
    "avg_value",
)
DIMENSION_FIELDS = ("country", "state", "city", "station", "pollutant_id")
NATURAL_KEY_FIELDS = (
    "country",
    "state",
    "city",
    "station",
    "pollutant_id",
    "last_update",
)


def configure_logging() -> None:
    """Configure predictable console logging for local and Airflow execution."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_api_key() -> str:
    """Load the API key without printing or exposing it in exception messages."""
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    api_key = os.getenv("DATA_GOV_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "DATA_GOV_API_KEY is not configured. Set it in the environment "
            "or in the project-root .env file."
        )
    return api_key


def build_http_session() -> requests.Session:
    """Create a reusable HTTP session with bounded retries for idempotent GETs."""
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "India-AQI-Data-Lake/1.1",
        }
    )
    return session


def parse_api_total(value: Any, *, offset: int) -> int:
    """Validate the API-reported total record count."""
    try:
        total = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"CPCB response at offset {offset} has a missing or invalid total."
        ) from exc
    if total < 0:
        raise ValueError(f"CPCB response at offset {offset} has a negative total.")
    return total


def parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def create_extraction_directory(raw_root: Path) -> tuple[str, Path]:
    """Create a unique second-resolution extraction directory.

    If two runs start in the same UTC second, wait briefly rather than reuse or
    overwrite an existing extraction directory.
    """
    raw_root.mkdir(parents=True, exist_ok=True)
    for _ in range(12):
        extraction_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_dir = raw_root / f"source_profile_{extraction_id}"
        try:
            output_dir.mkdir(parents=False, exist_ok=False)
            return extraction_id, output_dir
        except FileExistsError:
            time.sleep(0.25)
    raise RuntimeError(
        "Could not allocate a unique extraction directory after 12 attempts. "
        "Check whether another extraction is running."
    )


def fetch_page(
    session: requests.Session,
    api_key: str,
    offset: int,
    *,
    page_size: int = PAGE_SIZE,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Fetch and validate one API page.

    Do not include the full request URL in errors because the API key is a
    query-string parameter.
    """
    params = {
        "api-key": api_key,
        "format": "json",
        "limit": page_size,
        "offset": offset,
    }
    response = session.get(
        BASE_URL,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    try:
        payload = response.json()
    except requests.exceptions.JSONDecodeError as exc:
        raise ValueError(f"CPCB returned invalid JSON at offset {offset}.") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected CPCB response type at offset {offset}.")
    if payload.get("error"):
        # Do not echo the full payload; it may include request context.
        raise RuntimeError(f"CPCB API reported an error at offset {offset}.")

    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError(f"CPCB response at offset {offset} has no records array.")
    if any(not isinstance(record, dict) for record in records):
        raise ValueError(f"CPCB response at offset {offset} contains a non-object record.")
    if len(records) > page_size:
        raise ValueError(
            f"CPCB returned {len(records)} records for a requested page size of {page_size}."
        )

    returned_offset = parse_optional_int(payload.get("offset"))
    if returned_offset is not None and returned_offset != offset:
        raise RuntimeError(
            f"CPCB offset mismatch: requested {offset}, response reported {returned_offset}."
        )
    return payload, records


def save_raw_page(payload: dict[str, Any], output_file: Path) -> None:
    """Atomically preserve the full, unmodified API response envelope."""
    temporary_file = output_file.with_suffix(output_file.suffix + ".tmp")
    with temporary_file.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    temporary_file.replace(output_file)


def fetch_all_pages(
    *,
    api_key: str,
    raw_root: Path = DEFAULT_RAW_ROOT,
    session: requests.Session | None = None,
    page_size: int = PAGE_SIZE,
    max_pages: int = MAX_PAGES,
    request_delay: float = REQUEST_DELAY_SECONDS,
) -> tuple[list[dict[str, Any]], str, Path]:
    """Fetch all pages and require a complete, internally consistent result set."""
    if page_size <= 0:
        raise ValueError("page_size must be greater than zero.")
    if max_pages <= 0:
        raise ValueError("max_pages must be greater than zero.")

    http = session or build_http_session()
    extraction_id, output_dir = create_extraction_directory(raw_root)
    all_records: list[dict[str, Any]] = []
    offset = 0
    page_number = 0
    first_total: int | None = None
    totals_seen: set[int] = set()

    LOGGER.info("Starting CPCB extraction %s with page size %s.", extraction_id, page_size)

    try:
        while page_number < max_pages:
            payload, records = fetch_page(
                http,
                api_key,
                offset,
                page_size=page_size,
            )
            page_total = parse_api_total(payload.get("total"), offset=offset)
            totals_seen.add(page_total)

            if first_total is None:
                first_total = page_total
                LOGGER.info("CPCB reports %s total records.", first_total)
            elif page_total != first_total:
                raise RuntimeError(
                    "CPCB reported a changing total during pagination "
                    f"({first_total} initially, {page_total} at offset {offset}). "
                    "The source may have changed while pages were being fetched; "
                    "this extraction is not committed as a complete snapshot."
                )

            if not records:
                if first_total is not None and offset < first_total:
                    raise RuntimeError(
                        f"CPCB returned an empty page at offset {offset} before the "
                        f"reported total ({first_total}) was collected."
                    )
                break

            output_file = output_dir / f"page_offset_{offset:06d}.json"
            save_raw_page(payload, output_file)

            all_records.extend(records)
            page_number += 1
            LOGGER.info(
                "Page %s: offset=%s, records=%s, collected=%s/%s.",
                page_number,
                offset,
                len(records),
                len(all_records),
                first_total,
            )

            offset += len(records)
            if first_total is not None and offset >= first_total:
                break

            if request_delay > 0:
                time.sleep(request_delay)
        else:
            raise RuntimeError(
                f"Maximum page safety limit ({max_pages}) reached before extraction completed."
            )

        if first_total is None:
            raise RuntimeError("CPCB did not report a total record count.")
        if len(all_records) != first_total:
            raise RuntimeError(
                f"Incomplete extraction: API reported {first_total} records, "
                f"but {len(all_records)} were collected."
            )

        LOGGER.info(
            "Extraction complete: %s pages, %s records, output=%s.",
            page_number,
            len(all_records),
            output_dir,
        )
        return all_records, extraction_id, output_dir
    finally:
        if session is None:
            http.close()


def is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def is_valid_numeric(value: Any) -> bool:
    if is_missing(value):
        return False
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        return False
    return math.isfinite(number)


def unique_values(records: list[dict[str, Any]], field: str) -> list[str]:
    values = {
        str(record[field]).strip()
        for record in records
        if field in record and not is_missing(record[field])
    }
    return sorted(values)


def profile_dataset(records: list[dict[str, Any]]) -> None:
    """Print profiling metrics for the collected source snapshot."""
    print("\n" + "=" * 60)
    print("FULL CPCB DATASET PROFILE")
    print("=" * 60)

    if not records:
        print("No records available.")
        return

    fields = sorted({field for record in records for field in record})
    print("\nTOTAL RECORDS:", len(records))
    print("\nFIELDS:", fields)
    print("\nGEOGRAPHICAL AND POLLUTANT COVERAGE")

    for field in DIMENSION_FIELDS:
        values = unique_values(records, field)
        print(f"{field}: {len(values)} distinct values")
        if field in {"state", "city", "pollutant_id"}:
            print(" ", values)

    print("\nPOLLUTANT DISTRIBUTION")
    pollutant_counts = Counter(
        str(record.get("pollutant_id")).strip()
        for record in records
        if not is_missing(record.get("pollutant_id"))
    )
    for pollutant, count in sorted(pollutant_counts.items()):
        print(f"{pollutant}: {count}")

    print("\nMISSING VALUE PROFILE")
    for field in fields:
        missing = sum(is_missing(record.get(field)) for record in records)
        print(f"{field}: {missing}/{len(records)}")

    print("\nNUMERIC VALIDATION")
    for field in NUMERIC_FIELDS:
        invalid = sum(not is_valid_numeric(record.get(field)) for record in records)
        print(f"{field}: {invalid} invalid/missing values")

    keys = [
        tuple(
            "" if is_missing(record.get(field)) else str(record.get(field)).strip()
            for field in NATURAL_KEY_FIELDS
        )
        for record in records
    ]
    print("\nCANDIDATE DUPLICATE RECORDS:", len(keys) - len(set(keys)))
    print("\nSAMPLE RECORD:")
    print(json.dumps(records[0], indent=2, ensure_ascii=False))


def main() -> int:
    configure_logging()
    try:
        api_key = load_api_key()
        records, extraction_id, output_dir = fetch_all_pages(api_key=api_key)
        profile_dataset(records)
        print("\nExtraction ID:", extraction_id)
        print("Raw pages saved in:", output_dir)
        return 0
    except requests.exceptions.RequestException as exc:
        # Never log the exception text: Requests may include the URL, and the
        # API key is supplied as a query-string parameter.
        LOGGER.error("CPCB API request failed (%s).", type(exc).__name__)
        return 1
    except (OSError, RuntimeError, ValueError) as exc:
        LOGGER.error("CPCB extraction failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
