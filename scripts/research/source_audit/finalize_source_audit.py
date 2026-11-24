
import json
from pathlib import Path
from collections import Counter, defaultdict
from itertools import combinations

DATA_DIR = Path("data/raw")

runs = sorted(
    DATA_DIR.glob("source_profile_*"),
    key=lambda p: p.stat().st_mtime,
    reverse=True
)

if not runs:
    raise FileNotFoundError("No extraction found.")

run_dir = runs[0]

pages = sorted(run_dir.glob("page_offset_*.json"))

NA_FIELDS = ["min_value", "max_value", "avg_value"]

KEY_FIELDS = [
    "country",
    "state",
    "city",
    "station",
    "pollutant_id",
    "last_update"
]


def normalise(value):
    return str(value or "").strip().casefold()


def is_na(value):
    return (
        value is None
        or str(value).strip().upper() in ("", "NA")
    )


all_records = []
page_keys = {}
key_groups = defaultdict(list)

# ------------------------------------------
# 1. Load records
# ------------------------------------------

for page in pages:

    with page.open(encoding="utf-8") as file:
        payload = json.load(file)

    records = payload.get("records", [])

    keys = set()

    for record in records:

        key = tuple(
            normalise(record.get(field))
            for field in KEY_FIELDS
        )

        keys.add(key)

        key_groups[key].append({
            "page": page.name,
            "record": record
        })

        all_records.append(record)

    page_keys[page.name] = keys


print("\nTOTAL RECORDS:", len(all_records))


# ------------------------------------------
# 2. NA co-occurrence analysis
# ------------------------------------------

all_na = 0
partial_na = 0
valid_numeric = 0

na_by_pollutant = Counter()
na_by_city = Counter()

for record in all_records:

    na_count = sum(
        is_na(record.get(field))
        for field in NA_FIELDS
    )

    if na_count == 3:

        all_na += 1

    elif na_count > 0:

        partial_na += 1

    else:

        valid_numeric += 1

    if na_count > 0:

        na_by_pollutant[
            record.get("pollutant_id")
        ] += 1

        na_by_city[
            record.get("city")
        ] += 1


print("\nNA CO-OCCURRENCE")
print("All three fields NA:", all_na)
print("Partially unavailable:", partial_na)
print("All three available:", valid_numeric)

print("\nNA BY POLLUTANT:")
for pollutant, count in na_by_pollutant.most_common():
    print(pollutant, count)

print("\nTOP CITIES WITH NA:")
for city, count in na_by_city.most_common(15):
    print(city, count)


# ------------------------------------------
# 3. Duplicate group size distribution
# ------------------------------------------

duplicate_groups = {
    key: group
    for key, group in key_groups.items()
    if len(group) > 1
}

group_sizes = Counter(
    len(group)
    for group in duplicate_groups.values()
)

print("\nDUPLICATE GROUP SIZES:", dict(group_sizes))

print(
    "PROVISIONAL UNIQUE NATURAL KEYS:",
    len(key_groups)
)


# ------------------------------------------
# 4. Page-pair overlap
# ------------------------------------------

page_pair_overlap = Counter()

for key, group in duplicate_groups.items():

    page_names = sorted({
        item["page"]
        for item in group
    })

    for pair in combinations(page_names, 2):
        page_pair_overlap[pair] += 1


print("\nPAGE PAIR OVERLAPS")

for pair, count in page_pair_overlap.most_common(20):

    print(
        f"{pair[0]} <-> {pair[1]}: {count} repeated keys"
    )


# ------------------------------------------
# 5. Pollutant eligibility coverage
# ------------------------------------------

station_pollutants = defaultdict(set)

for record in all_records:

    station_key = tuple(
        normalise(record.get(field))
        for field in [
            "country",
            "state",
            "city",
            "station"
        ]
    )

    pollutant = normalise(
        record.get("pollutant_id")
    )

    if pollutant:
        station_pollutants[station_key].add(pollutant)


pm_pollutants = {"pm10", "pm2.5"}

basic_eligible = 0
insufficient_pollutants = 0
missing_particulate = 0

for pollutants in station_pollutants.values():

    has_pm = bool(pollutants & pm_pollutants)

    if len(pollutants) >= 3 and has_pm:

        basic_eligible += 1

    elif len(pollutants) < 3:

        insufficient_pollutants += 1

    elif not has_pm:

        missing_particulate += 1


print("\nAQI STRUCTURAL COVERAGE")
print("Total stations:", len(station_pollutants))
print("At least 3 pollutants + PM:", basic_eligible)
print("Fewer than 3 pollutants:", insufficient_pollutants)
print("At least 3 but no PM:", missing_particulate)

print(
    "\nNOTE: These are structural coverage counts, "
    "not AQI eligibility. The 16-hour requirement "
    "has not been validated."
)

print("\nSOURCE AUDIT COMPLETED.")
