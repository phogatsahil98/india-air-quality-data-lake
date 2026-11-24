from __future__ import annotations

import argparse
import json
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

MISSING_TOKENS = {"", "na", "n/a", "null", "none", "nan"}
MEASUREMENT_FIELDS = ("min_value", "max_value", "avg_value")
KEY_FIELDS = (
    "country", "state", "city", "station", "last_update", "pollutant_id"
)


def norm(value: Any) -> str:
    """Normalize source text for comparisons without changing stored source data."""
    if value is None:
        return ""
    return str(value).strip().casefold()


def extract_records(payload: Any) -> list[dict[str, Any]]:
    """Read a data.gov.in page envelope, or a JSON file containing row objects."""
    if isinstance(payload, dict):
        records = payload.get("records")
        if isinstance(records, list):
            return [r for r in records if isinstance(r, dict)]
        # Handle a page saved directly as a single row, if encountered.
        if all(field in payload for field in ("station", "pollutant_id", "last_update")):
            return [payload]
    elif isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    return []


def load_snapshot(folder: Path) -> tuple[list[dict[str, Any]], int, list[str]]:
    if not folder.is_dir():
        raise FileNotFoundError(f"Snapshot folder does not exist: {folder}")

    records: list[dict[str, Any]] = []
    json_files = 0
    errors: list[str] = []
    for file_path in sorted(folder.rglob("*.json")):
        json_files += 1
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8-sig"))
            records.extend(extract_records(payload))
        except Exception as exc:
            errors.append(f"{file_path.relative_to(folder)}: {exc}")

    if not records:
        raise ValueError(
            f"No API records found under {folder}. Check that page JSON files contain a 'records' array."
        )
    return records, json_files, errors


def record_key(record: dict[str, Any]) -> tuple[str, ...]:
    return tuple(norm(record.get(field)) for field in KEY_FIELDS)


def reading_signature(record: dict[str, Any]) -> tuple[str, ...]:
    return tuple(norm(record.get(field)) for field in MEASUREMENT_FIELDS)


def grouped_records(records: list[dict[str, Any]]) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[record_key(record)].append(record)
    return grouped


def snapshot_stats(records: list[dict[str, Any]]) -> dict[str, Any]:
    groups = grouped_records(records)
    exact_excess = 0
    conflicting_keys = 0
    candidate_excess = 0
    for group in groups.values():
        signatures = [reading_signature(row) for row in group]
        candidate_excess += max(0, len(group) - 1)
        exact_excess += len(signatures) - len(set(signatures))
        if len(set(signatures)) > 1:
            conflicting_keys += 1

    unavailable_raw = sum(
        all(norm(row.get(field)) in MISSING_TOKENS for field in MEASUREMENT_FIELDS)
        for row in records
    )
    unavailable_keys = sum(
        all(
            all(norm(row.get(field)) in MISSING_TOKENS for field in MEASUREMENT_FIELDS)
            for row in group
        )
        for group in groups.values()
    )

    timestamps = sorted({str(row.get("last_update", "")).strip() for row in records})
    parsed_timestamps: list[datetime] = []
    for value in timestamps:
        try:
            parsed_timestamps.append(datetime.strptime(value, "%d-%m-%Y %H:%M:%S"))
        except ValueError:
            pass

    return {
        "raw_count": len(records),
        "unique_keys": len(groups),
        "candidate_duplicate_excess": candidate_excess,
        "exact_duplicate_excess": exact_excess,
        "conflicting_duplicate_keys": conflicting_keys,
        "unavailable_raw": unavailable_raw,
        "unavailable_unique_keys": unavailable_keys,
        "cities": {norm(r.get("city")) for r in records if norm(r.get("city"))},
        "states": {norm(r.get("state")) for r in records if norm(r.get("state"))},
        "stations": {norm(r.get("station")) for r in records if norm(r.get("station"))},
        "pollutants": {norm(r.get("pollutant_id")) for r in records if norm(r.get("pollutant_id"))},
        "pollutant_counts": Counter(norm(r.get("pollutant_id")) for r in records),
        "timestamps": timestamps,
        "timestamp_min": min(parsed_timestamps).strftime("%d-%m-%Y %H:%M:%S") if parsed_timestamps else "N/A",
        "timestamp_max": max(parsed_timestamps).strftime("%d-%m-%Y %H:%M:%S") if parsed_timestamps else "N/A",
        "groups": groups,
    }


def display_set_difference(label: str, old: set[str], new: set[str], limit: int = 25) -> None:
    added = sorted(new - old)
    removed = sorted(old - new)
    print(f"\n{label}: {len(old)} -> {len(new)}")
    print(f"  Added ({len(added)}): {added[:limit] if added else 'None'}")
    print(f"  Removed ({len(removed)}): {removed[:limit] if removed else 'None'}")
    if len(added) > limit or len(removed) > limit:
        print(f"  (Showing at most {limit} names in each list.)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare two CPCB data.gov.in raw snapshot folders."
    )
    parser.add_argument("old_snapshot", type=Path, help="Earlier source_profile folder")
    parser.add_argument("new_snapshot", type=Path, help="Later source_profile folder")
    args = parser.parse_args()

    old_records, old_files, old_errors = load_snapshot(args.old_snapshot)
    new_records, new_files, new_errors = load_snapshot(args.new_snapshot)
    old = snapshot_stats(old_records)
    new = snapshot_stats(new_records)

    old_keys = set(old["groups"])
    new_keys = set(new["groups"])
    common_keys = old_keys & new_keys
    new_keys_only = new_keys - old_keys
    removed_keys = old_keys - new_keys
    changed_keys = [
        key for key in common_keys
        if {reading_signature(r) for r in old["groups"][key]}
        != {reading_signature(r) for r in new["groups"][key]}
    ]
    unchanged_keys = len(common_keys) - len(changed_keys)

    print("=" * 72)
    print("CPCB SNAPSHOT COMPARISON")
    print("=" * 72)
    print(f"OLD: {args.old_snapshot}")
    print(f"NEW: {args.new_snapshot}")
    print(f"JSON files read: {old_files} -> {new_files}")
    print(f"Raw records: {old['raw_count']:,} -> {new['raw_count']:,} (change {new['raw_count'] - old['raw_count']:+,})")
    print(f"Distinct natural keys: {old['unique_keys']:,} -> {new['unique_keys']:,}")
    print(f"Candidate duplicate excess: {old['candidate_duplicate_excess']:,} -> {new['candidate_duplicate_excess']:,}")
    print(f"Exact duplicate excess: {old['exact_duplicate_excess']:,} -> {new['exact_duplicate_excess']:,}")
    print(f"Keys with conflicting readings inside snapshot: {old['conflicting_duplicate_keys']:,} -> {new['conflicting_duplicate_keys']:,}")
    print(f"Unavailable rows (all 3 measurements NA/blank/null): {old['unavailable_raw']:,} -> {new['unavailable_raw']:,}")
    print(f"Unavailable unique keys: {old['unavailable_unique_keys']:,} -> {new['unavailable_unique_keys']:,}")
    print(f"Source last_update range: {old['timestamp_min']} to {old['timestamp_max']} -> {new['timestamp_min']} to {new['timestamp_max']}")
    print(f"Distinct last_update values: {len(old['timestamps']):,} -> {len(new['timestamps']):,}")

    display_set_difference("Cities", old["cities"], new["cities"])
    display_set_difference("Stations", old["stations"], new["stations"])
    display_set_difference("Pollutants", old["pollutants"], new["pollutants"])

    print("\nPOLLUTANT ROW COUNTS (raw records)")
    for pollutant in sorted(set(old["pollutant_counts"]) | set(new["pollutant_counts"])):
        before = old["pollutant_counts"].get(pollutant, 0)
        after = new["pollutant_counts"].get(pollutant, 0)
        print(f"  {pollutant}: {before:,} -> {after:,} ({after - before:+,})")

    print("\nCROSS-SNAPSHOT NATURAL-KEY COMPARISON")
    print(f"  New station/pollutant/timestamp keys: {len(new_keys_only):,}")
    print(f"  Keys no longer present: {len(removed_keys):,}")
    print(f"  Common keys with changed reading set: {len(changed_keys):,}")
    print(f"  Common keys with unchanged reading set: {unchanged_keys:,}")

    if changed_keys:
        print("\nSAMPLE CHANGED KEYS (up to 10)")
        for key in sorted(changed_keys)[:10]:
            old_values = sorted({reading_signature(r) for r in old["groups"][key]})
            new_values = sorted({reading_signature(r) for r in new["groups"][key]})
            print(f"  KEY={key}")
            print(f"    OLD readings={old_values}")
            print(f"    NEW readings={new_values}")

    if new_keys_only:
        print("\nSAMPLE NEW KEYS (up to 10)")
        for key in sorted(new_keys_only)[:10]:
            print(f"  {key}")

    errors = [("OLD", e) for e in old_errors] + [("NEW", e) for e in new_errors]
    if errors:
        print("\nJSON FILE READ WARNINGS")
        for label, error in errors[:20]:
            print(f"  {label}: {error}")
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more")

    print("\nNOTE: This comparison key is country + state + city + station + last_update + pollutant_id.")
    print("It is a candidate natural key, not a guaranteed CPCB primary key.")
    print("No source records are modified by this script.")


if __name__ == "__main__":
    main()
