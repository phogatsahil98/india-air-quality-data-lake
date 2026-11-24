
import json
import os
import sys

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

load_dotenv()

API_KEY = os.getenv("DATA_GOV_API_KEY")

BASE_URL = (
    "https://api.data.gov.in/resource"
    "/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
)

PARAMS = {
    "api-key": API_KEY,
    "format": "json",
    "limit": 100,
    "offset": 0,
}

OUTPUT_DIR = Path("data/raw")


# --------------------------------------------------
# 2. Validate configuration
# --------------------------------------------------

if not API_KEY:
    sys.exit(
        "ERROR: DATA_GOV_API_KEY is missing. "
        "Check your .env file."
    )


# --------------------------------------------------
# 3. Configure HTTP retries
# --------------------------------------------------

retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)

adapter = HTTPAdapter(max_retries=retry_strategy)

session = requests.Session()
session.mount("https://", adapter)


# --------------------------------------------------
# 4. Fetch API data
# --------------------------------------------------

def fetch_data():

    print("\nConnecting to CPCB API...")

    test_params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": 5,
        "offset": 0,
    }

    try:
        response = requests.get(
            BASE_URL,
            params=test_params,
            timeout=(15, 90),
            headers={
                "Accept": "application/json",
                "User-Agent": "India-AQI-Data-Lake/1.0",
            },
        )

        print("\nHTTP STATUS:", response.status_code)

        print("\nRESPONSE HEADERS:")

        important_headers = [
            "Server",
            "Content-Type",
            "Date",
            "Via",
            "Retry-After",
            "X-Cache",
            "CF-Ray",
        ]

        for header in important_headers:
            value = response.headers.get(header)

            if value:
                print(f"{header}: {value}")

        print("\nRESPONSE BODY:")

        body = response.text[:1500]

        if API_KEY:
            body = body.replace(API_KEY, "<REDACTED>")

        print(body if body else "<Empty response body>")

        if response.status_code != 200:
            print("\nAPI request unsuccessful.")
            sys.exit(1)

        payload = response.json()

        if not isinstance(payload, dict):
            sys.exit("Unexpected JSON structure.")

        records = payload.get("records")

        if not isinstance(records, list):
            sys.exit("Records array missing from response.")

        print("\nAPI CONNECTION SUCCESSFUL!")
        print("Records received:", len(records))

        return payload, records

    except requests.exceptions.RequestException as exc:

        print("\nREQUEST FAILED:", type(exc).__name__)
        print(str(exc).replace(API_KEY, "<REDACTED>"))

        sys.exit(1)


# --------------------------------------------------
# 5. Save original API response
# --------------------------------------------------

def save_raw_response(payload):

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(
        ZoneInfo("Asia/Kolkata")
    ).strftime("%Y%m%d_%H%M%S")

    output_file = OUTPUT_DIR / (
        f"cpcb_api_sample_{timestamp}.json"
    )

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(
            payload,
            file,
            indent=2,
            ensure_ascii=False,
        )

    print(f"\nRaw response saved: {output_file}")


# --------------------------------------------------
# 6. Profile source data
# --------------------------------------------------

def profile_data(payload, records):

    print("\n" + "=" * 55)
    print("CPCB SOURCE DATA PROFILE")
    print("=" * 55)

    print(f"API reported total : {payload.get('total')}")
    print(f"Records returned   : {len(records)}")
    print(f"API reported count : {payload.get('count')}")

    if not records:
        print("\nNo records returned.")
        return

    # Discover actual API schema
    fields = sorted({
        field
        for record in records
        for field in record.keys()
    })

    print("\nFIELDS AVAILABLE:")
    for field in fields:
        print(f" - {field}")

    # Discover geographic coverage
    dimensions = [
        "state",
        "city",
        "station",
        "pollutant_id",
    ]

    print("\nDISTINCT VALUES:")

    for dimension in dimensions:

        values = {
            str(record.get(dimension)).strip()
            for record in records
            if record.get(dimension) not in (None, "")
        }

        if values:
            print(
                f"{dimension}: {len(values)} unique values"
            )

            if dimension in ("city", "pollutant_id"):
                print(f"  {sorted(values)}")

    # Missing-value analysis
    print("\nNULL / MISSING VALUE PROFILE:")

    for field in fields:

        missing = sum(
            record.get(field) in (None, "")
            for record in records
        )

        print(
            f"{field}: {missing}/{len(records)} missing"
        )

    # Display one actual record
    print("\nSAMPLE RECORD:")
    print(
        json.dumps(
            records[0],
            indent=2,
            ensure_ascii=False
        )
    )


# --------------------------------------------------
# 7. Main execution
# --------------------------------------------------

if __name__ == "__main__":

    payload, records = fetch_data()

    save_raw_response(payload)

    profile_data(payload, records)

    print("\nProfiling completed successfully.")
