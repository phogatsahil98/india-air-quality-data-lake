from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

MISSING_TOKENS = {"", "na", "n/a", "null", "none", "nan"}
MEASUREMENT_FIELDS = ("min_value", "max_value", "avg_value")
# Candidate entity key: deliberately excludes last_update so a refreshed
# snapshot can be compared with the previous snapshot.
ENTITY_FIELDS = ("country", "state", "city", "station", "pollutant_id")
# Candidate within-snapshot record key: includes last_update.
SNAPSHOT_RECORD_FIELDS = ENTITY_FIELDS[:-1] + ("last_update", "pollutant_id")


def norm(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().casefold().split())


def canonical_measurement(value: Any) -> str:
    text = norm(value)
    if text in MISSING_TOKENS:
        return "<MISSING>"
    try:
        number = Decimal(text)
        if number.is_finite():
            if number == 0:
                return "0"
            return format(number.normalize(), "f")
    except (InvalidOperation, ValueError):
        pass
    return text


def extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("records")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        if all(field in payload for field in ("station", "pollutant_id", "last_update")):
            return [payload]
    elif isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def load_snapshot(folder: Path) -> tuple[list[dict[str, Any]], int, list[str]]:
    if not folder.is_dir():
        raise FileNotFoundError(f"Snapshot folder does not exist: {folder}")
    rows: list[dict[str, Any]] = []
    file_count = 0
    errors: list[str] = []
    for path in sorted(folder.rglob("*.json")):
        file_count += 1
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            rows.extend(extract_records(payload))
        except Exception as exc:
            errors.append(f"{path.relative_to(folder)}: {exc}")
    if not rows:
        raise ValueError(f"No API records found under {folder}; expected page JSON with a 'records' array.")
    return rows, file_count, errors


def make_key(row: dict[str, Any], fields: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(norm(row.get(field)) for field in fields)


def reading_signature(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(canonical_measurement(row.get(field)) for field in MEASUREMENT_FIELDS)


def is_unavailable(signature: tuple[str, ...]) -> bool:
    return all(value == "<MISSING>" for value in signature)


def group_rows(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[make_key(row, fields)].append(row)
    return groups


def snapshot_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    record_groups = group_rows(rows, SNAPSHOT_RECORD_FIELDS)
    entity_groups = group_rows(rows, ENTITY_FIELDS)
    exact_duplicate_excess = 0
    conflicting_record_keys = 0
    for group in record_groups.values():
        signatures = {reading_signature(row) for row in group}
        exact_duplicate_excess += len(group) - len(signatures)
        if len(signatures) > 1:
            conflicting_record_keys += 1

    unavailable_raw = sum(is_unavailable(reading_signature(row)) for row in rows)
    unavailable_record_keys = sum(
        all(is_unavailable(reading_signature(row)) for row in group)
        for group in record_groups.values()
    )
    entity_conflicts = sum(
        len({reading_signature(row) for row in group}) > 1
        for group in entity_groups.values()
    )
    return {
        "raw_count": len(rows),
        "record_key_count": len(record_groups),
        "entity_key_count": len(entity_groups),
        "exact_duplicate_excess": exact_duplicate_excess,
        "conflicting_record_keys": conflicting_record_keys,
        "entity_conflicts": entity_conflicts,
        "unavailable_raw": unavailable_raw,
        "unavailable_record_keys": unavailable_record_keys,
        "timestamps": sorted({norm(row.get("last_update")) for row in rows if norm(row.get("last_update"))}),
        "entity_groups": entity_groups,
    }


def group_readings(group: list[dict[str, Any]]) -> set[tuple[str, ...]]:
    return {reading_signature(row) for row in group}


def fmt_reading(signatures: set[tuple[str, ...]]) -> str:
    if not signatures:
        return "<no rows>"
    values = []
    for signature in sorted(signatures):
        values.append("(min=" + signature[0] + ", max=" + signature[1] + ", avg=" + signature[2] + ")")
    return "; ".join(values)


def print_set_change(label: str, before: set[str], after: set[str], limit: int = 20) -> None:
    added = sorted(after - before)
    removed = sorted(before - after)
    print(f"\n{label}: {len(before):,} -> {len(after):,}")
    print(f"  Added ({len(added):,}): {added[:limit] if added else 'None'}")
    print(f"  Removed ({len(removed):,}): {removed[:limit] if removed else 'None'}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare CPCB snapshots by station-pollutant entity, not timestamp.")
    parser.add_argument("old_snapshot", type=Path)
    parser.add_argument("new_snapshot", type=Path)
    args = parser.parse_args()

    old_rows, old_files, old_errors = load_snapshot(args.old_snapshot)
    new_rows, new_files, new_errors = load_snapshot(args.new_snapshot)
    old = snapshot_metrics(old_rows)
    new = snapshot_metrics(new_rows)

    old_entities = set(old["entity_groups"])
    new_entities = set(new["entity_groups"])
    common = old_entities & new_entities
    added = new_entities - old_entities
    removed = old_entities - new_entities

    unchanged_values: list[tuple[str, ...]] = []
    changed_values: list[tuple[str, ...]] = []
    availability_changed: list[tuple[str, ...]] = []
    for key in common:
        old_sig = group_readings(old["entity_groups"][key])
        new_sig = group_readings(new["entity_groups"][key])
        if old_sig == new_sig:
            unchanged_values.append(key)
        else:
            changed_values.append(key)
        old_available = any(not is_unavailable(sig) for sig in old_sig)
        new_available = any(not is_unavailable(sig) for sig in new_sig)
        if old_available != new_available:
            availability_changed.append(key)

    print("=" * 76)
    print("CPCB SNAPSHOT COMPARISON — STABLE ENTITY KEY")
    print("=" * 76)
    print(f"OLD: {args.old_snapshot}")
    print(f"NEW: {args.new_snapshot}")
    print(f"JSON files: {old_files} -> {new_files}")
    print(f"Raw records: {old['raw_count']:,} -> {new['raw_count']:,} (change {new['raw_count'] - old['raw_count']:+,})")
    print(f"Distinct station-pollutant entity keys: {old['entity_key_count']:,} -> {new['entity_key_count']:,}")
    print(f"Timestamped record keys: {old['record_key_count']:,} -> {new['record_key_count']:,}")
    print(f"Exact duplicate excess: {old['exact_duplicate_excess']:,} -> {new['exact_duplicate_excess']:,}")
    print(f"Conflicting timestamped record keys: {old['conflicting_record_keys']:,} -> {new['conflicting_record_keys']:,}")
    print(f"Entities with multiple distinct reading signatures inside snapshot: {old['entity_conflicts']:,} -> {new['entity_conflicts']:,}")
    print(f"Unavailable raw rows: {old['unavailable_raw']:,} -> {new['unavailable_raw']:,}")
    print(f"Unavailable timestamped keys: {old['unavailable_record_keys']:,} -> {new['unavailable_record_keys']:,}")
    print(f"Source last_update values OLD: {old['timestamps']}")
    print(f"Source last_update values NEW: {new['timestamps']}")

    print("\nCROSS-SNAPSHOT STATION-POLLUTANT COMPARISON")
    print(f"  Common entities: {len(common):,}")
    print(f"  Newly appearing entities: {len(added):,}")
    print(f"  No longer present entities: {len(removed):,}")
    print(f"  Common entities with unchanged measurement values: {len(unchanged_values):,}")
    print(f"  Common entities with changed measurement values: {len(changed_values):,}")
    print(f"  Common entities changing availability status: {len(availability_changed):,}")

    if changed_values:
        print("\nSAMPLE CHANGED MEASUREMENTS (up to 15)")
        for key in sorted(changed_values)[:15]:
            print(f"  ENTITY={key}")
            print(f"    OLD: {fmt_reading(group_readings(old['entity_groups'][key]))}")
            print(f"    NEW: {fmt_reading(group_readings(new['entity_groups'][key]))}")

    if added:
        print("\nSAMPLE NEW ENTITIES (up to 10)")
        for key in sorted(added)[:10]:
            print(f"  {key}")
    if removed:
        print("\nSAMPLE MISSING ENTITIES (up to 10)")
        for key in sorted(removed)[:10]:
            print(f"  {key}")

    errors = [("OLD", err) for err in old_errors] + [("NEW", err) for err in new_errors]
    if errors:
        print("\nJSON READ WARNINGS")
        for label, err in errors[:20]:
            print(f"  {label}: {err}")
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more")

    print("\nINTERPRETATION NOTE")
    print("  The cross-snapshot entity key excludes last_update. It is a candidate key based")
    print("  on country/state/city/station/pollutant_id, not an official CPCB station ID.")
    print("  Station renames or metadata corrections can therefore appear as removed + added.")
    print("  Measurements are compared as normalized values; original source files remain untouched.")


if __name__ == "__main__":
    main()
