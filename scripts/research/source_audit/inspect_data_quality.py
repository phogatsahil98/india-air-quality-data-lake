
import json
import math

from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime


# ==========================================
# 1. FIND LATEST RAW EXTRACTION
# ==========================================

DATA_DIR = Path("data/raw")

runs = sorted(
    DATA_DIR.glob("source_profile_*"),
    key=lambda path: path.stat().st_mtime,
    reverse=True
)

if not runs:
    raise FileNotFoundError("No source profile directory found.")

RUN_DIR = runs[0]

page_files = sorted(
    RUN_DIR.glob("page_offset_*.json")
)

print("Reading extraction:", RUN_DIR)
print("Raw pages found:", len(page_files))


# ==========================================
# 2. LOAD ALL RAW RECORDS
# ==========================================

rows = []

for page_file in page_files:

    with page_file.open(
        encoding="utf-8"
    ) as file:

        payload = json.load(file)

    for row_number, record in enumerate(
        payload.get("records", [])
    ):

        rows.append({
            "page": page_file.name,
            "row_number": row_number,
            "record": record
        })


print("\nTOTAL RECORDS:", len(rows))


# ==========================================
# 3. NUMERIC VALIDATION
# ==========================================

NUMERIC_FIELDS = [
    "latitude",
    "longitude",
    "min_value",
    "max_value",
    "avg_value"
]


def is_valid_number(value):

    if value is None:
        return False

    if isinstance(value, str) and not value.strip():
        return False

    try:

        number = float(value)

        return math.isfinite(number)

    except (ValueError, TypeError):

        return False


print("\n" + "=" * 60)
print("INVALID NUMERIC TOKENS")
print("=" * 60)

for field in NUMERIC_FIELDS:

    invalid_tokens = Counter()

    for item in rows:

        value = item["record"].get(field)

        if not is_valid_number(value):

            if value is None:
                token = "<NULL>"

            elif not str(value).strip():
                token = "<BLANK>"

            else:
                token = str(value).strip()

            invalid_tokens[token] += 1

    print(f"\n{field}")

    for token, count in invalid_tokens.most_common(15):

        print(f"  {token!r}: {count}")


# ==========================================
# 4. DISPLAY INVALID RECORD EXAMPLES
# ==========================================

print("\n" + "=" * 60)
print("INVALID RECORD EXAMPLES")
print("=" * 60)

shown = 0

for item in rows:

    record = item["record"]

    invalid_fields = [
        field
        for field in NUMERIC_FIELDS
        if not is_valid_number(record.get(field))
    ]

    if invalid_fields:

        print({
            "city": record.get("city"),
            "station": record.get("station"),
            "pollutant": record.get("pollutant_id"),
            "timestamp": record.get("last_update"),
            "invalid_fields": invalid_fields,
            "values": {
                field: record.get(field)
                for field in invalid_fields
            },
            "source_page": item["page"]
        })

        shown += 1

        if shown >= 10:
            break


# ==========================================
# 5. CANDIDATE NATURAL KEY
# ==========================================

KEY_FIELDS = [
    "country",
    "state",
    "city",
    "station",
    "pollutant_id",
    "last_update"
]


def normalise(value):

    if value is None:
        return ""

    return str(value).strip().casefold()


key_groups = defaultdict(list)

for item in rows:

    record = item["record"]

    key = tuple(
        normalise(record.get(field))
        for field in KEY_FIELDS
    )

    key_groups[key].append(item)


duplicate_groups = {
    key: group
    for key, group in key_groups.items()
    if len(group) > 1
}


# ==========================================
# 6. CLASSIFY DUPLICATES
# ==========================================

extra_key_rows = sum(
    len(group) - 1
    for group in duplicate_groups.values()
)

exact_duplicate_rows = 0
cross_page_groups = 0
conflicting_reading_groups = 0

for group in duplicate_groups.values():

    signatures = Counter(
        json.dumps(
            item["record"],
            sort_keys=True,
            ensure_ascii=False
        )
        for item in group
    )

    exact_duplicate_rows += sum(
        count - 1
        for count in signatures.values()
    )

    pages = {
        item["page"]
        for item in group
    }

    if len(pages) > 1:
        cross_page_groups += 1

    readings = {
        tuple(
            str(item["record"].get(field))
            for field in [
                "min_value",
                "max_value",
                "avg_value"
            ]
        )
        for item in group
    }

    if len(readings) > 1:
        conflicting_reading_groups += 1


print("\n" + "=" * 60)
print("DUPLICATE ANALYSIS")
print("=" * 60)

print("Duplicate key groups:", len(duplicate_groups))
print("Extra rows with repeated keys:", extra_key_rows)
print("Exact duplicate excess rows:", exact_duplicate_rows)
print("Groups appearing across multiple pages:", cross_page_groups)
print("Groups with conflicting readings:", conflicting_reading_groups)


# ==========================================
# 7. SHOW DUPLICATE GROUP EXAMPLES
# ==========================================

print("\nDUPLICATE EXAMPLES")

for key, group in list(duplicate_groups.items())[:10]:

    print("\nKey:", key)

    for item in group:

        record = item["record"]

        print({
            "page": item["page"],
            "min": record.get("min_value"),
            "max": record.get("max_value"),
            "avg": record.get("avg_value"),
            "station": record.get("station")
        })


# ==========================================
# 8. TIMESTAMP ANALYSIS
# ==========================================

print("\n" + "=" * 60)
print("SOURCE TIMESTAMP ANALYSIS")
print("=" * 60)

timestamps = []
invalid_timestamps = 0

for item in rows:

    value = item["record"].get("last_update")

    try:

        parsed = datetime.strptime(
            value,
            "%d-%m-%Y %H:%M:%S"
        )

        timestamps.append(parsed)

    except (ValueError, TypeError):

        invalid_timestamps += 1


if timestamps:

    print("Distinct timestamps:", len(set(timestamps)))
    print("Earliest timestamp:", min(timestamps))
    print("Latest timestamp:", max(timestamps))

print("Invalid timestamps:", invalid_timestamps)


# ==========================================
# 9. STATION / POLLUTANT COVERAGE
# ==========================================

print("\n" + "=" * 60)
print("STATION / POLLUTANT COVERAGE")
print("=" * 60)

station_groups = defaultdict(list)

for item in rows:

    record = item["record"]

    station_key = tuple(
        normalise(record.get(field))
        for field in [
            "country",
            "state",
            "city",
            "station"
        ]
    )

    station_groups[station_key].append(
        record.get("pollutant_id")
    )


coverage = Counter(
    len(set(pollutants))
    for pollutants in station_groups.values()
)

print("Composite stations:", len(station_groups))
print("Pollutant coverage distribution:", dict(sorted(coverage.items())))

incomplete_stations = [
    {
        "city": key[2],
        "station": key[3],
        "pollutants": sorted(set(pollutants))
    }
    for key, pollutants in station_groups.items()
    if len(set(pollutants)) < 7
]

print("Stations reporting fewer than 7 pollutants:",
      len(incomplete_stations))

for station in incomplete_stations[:10]:
    print(station)


print("\nDATA QUALITY INVESTIGATION COMPLETED.")
