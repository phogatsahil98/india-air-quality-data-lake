from __future__ import annotations

import importlib.util
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, relative_path: str) -> ModuleType:
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


profile = load_module(
    "profile_cpcb_full",
    "scripts/ingestion/profile_cpcb_full.py",
)
bronze = load_module(
    "upload_bronze_dynamic",
    "scripts/ingestion/upload_bronze_dynamic.py",
)
silver_builder = load_module(
    "build_historical_silver",
    "scripts/transformation/build_historical_silver.py",
)
silver_uploader = load_module(
    "upload_silver_incremental",
    "scripts/aws/upload_silver_incremental.py",
)
glue_registrar = load_module(
    "register_silver_partition",
    "scripts/aws/register_silver_partition.py",
)


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload


class FakeHttpSession:
    def __init__(self, pages: dict[int, dict[str, Any]]) -> None:
        self.pages = pages
        self.requested_offsets: list[int] = []

    def get(self, url: str, *, params: dict[str, Any], timeout: Any):
        offset = int(params["offset"])
        self.requested_offsets.append(offset)
        return FakeResponse(self.pages[offset])


class TestCpcbProfileHelpers(unittest.TestCase):
    def test_numeric_validation_rejects_non_finite_values(self) -> None:
        self.assertTrue(profile.is_valid_numeric("12.5"))
        self.assertFalse(profile.is_valid_numeric("NA"))
        self.assertFalse(profile.is_valid_numeric("NaN"))
        self.assertFalse(profile.is_valid_numeric("Infinity"))

    def test_missing_value_validation(self) -> None:
        self.assertTrue(profile.is_missing(None))
        self.assertTrue(profile.is_missing("  "))
        self.assertFalse(profile.is_missing(0))

    def test_fetch_all_pages_persists_each_original_envelope(self) -> None:
        pages = {
            0: {"total": 3, "offset": 0, "records": [{"id": 1}, {"id": 2}]},
            2: {"total": 3, "offset": 2, "records": [{"id": 3}]},
        }
        session = FakeHttpSession(pages)
        with tempfile.TemporaryDirectory() as raw_temp:
            records, extraction_id, output_dir = profile.fetch_all_pages(
                api_key="unit-test-key",
                raw_root=Path(raw_temp),
                session=session,
                page_size=2,
                request_delay=0,
            )
            self.assertEqual(len(records), 3)
            self.assertEqual(session.requested_offsets, [0, 2])
            self.assertTrue(output_dir.name.endswith(extraction_id))
            self.assertEqual(len(list(output_dir.glob("page_offset_*.json"))), 2)

    def test_fetch_all_pages_rejects_changing_api_total(self) -> None:
        pages = {
            0: {"total": 3, "offset": 0, "records": [{"id": 1}, {"id": 2}]},
            2: {"total": 4, "offset": 2, "records": [{"id": 3}, {"id": 4}]},
        }
        session = FakeHttpSession(pages)
        with tempfile.TemporaryDirectory() as raw_temp:
            with self.assertRaises(RuntimeError):
                profile.fetch_all_pages(
                    api_key="unit-test-key",
                    raw_root=Path(raw_temp),
                    session=session,
                    page_size=2,
                    request_delay=0,
                )


class TestBronzeCompletenessValidation(unittest.TestCase):
    def make_inventory(self) -> list[dict[str, Any]]:
        return [
            {
                "offset": 0,
                "record_count": 100,
                "local_relative_path": "page_offset_000000.json",
            },
            {
                "offset": 100,
                "record_count": 46,
                "local_relative_path": "page_offset_000100.json",
            },
        ]

    def test_accepts_contiguous_pages_and_matching_total(self) -> None:
        result = bronze.validate_page_inventory(self.make_inventory(), 146, [146])
        self.assertEqual(result["status"], "PASSED")
        self.assertTrue(result["page_offsets_validated"])

    def test_rejects_mismatch_between_collected_and_api_total(self) -> None:
        with self.assertRaises(RuntimeError):
            bronze.validate_page_inventory(self.make_inventory(), 145, [146])

    def test_rejects_inconsistent_api_totals(self) -> None:
        with self.assertRaises(RuntimeError):
            bronze.validate_page_inventory(self.make_inventory(), 146, [146, 147])

    def test_rejects_offset_gap(self) -> None:
        inventory = self.make_inventory()
        inventory[1]["offset"] = 101
        with self.assertRaises(RuntimeError):
            bronze.validate_page_inventory(inventory, 146, [146])


class TestHistoricalSilverOutputSafety(unittest.TestCase):
    def test_rejects_paths_outside_processed_directory(self) -> None:
        outside = ROOT.parent / "unsafe_silver_output"
        with self.assertRaises(ValueError):
            silver_builder.prepare_output_directory(outside, overwrite=True)

    def test_rejects_generated_build_root_itself(self) -> None:
        allowed_root = ROOT / "data" / "processed" / "airflow_runs"
        with self.assertRaises(ValueError):
            silver_builder.prepare_output_directory(allowed_root, overwrite=True)

    def test_overwrite_is_limited_to_generated_child_directory(self) -> None:
        allowed_root = ROOT / "data" / "processed" / "historical_silver"
        allowed_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="_unit_silver_", dir=allowed_root) as temp_dir:
            target = Path(temp_dir) / "generated_output"
            target.mkdir()
            (target / "generated.txt").write_text("generated", encoding="utf-8")
            prepared = silver_builder.prepare_output_directory(target, overwrite=True)
            self.assertEqual(prepared, target.resolve())
            self.assertFalse(prepared.exists())


class TestSilverQualityReportValidation(unittest.TestCase):
    def test_accepts_consistent_single_extraction_report(self) -> None:
        extraction_id = "20261120T170816Z"
        retrieved = datetime(2026, 11, 20, 17, 8, 16, tzinfo=timezone.utc)
        report = {
            "dataset": "cpcb_observation_snapshots",
            "conflicting_timestamped_entity_groups": 0,
            "cross_extraction_deduplication_performed": False,
            "combined_unique_record_count": 100,
            "source_extractions": [
                {
                    "extraction_id": extraction_id,
                    "retrieved_at_utc": "2026-11-20T17:08:16Z",
                    "spark_unique_record_count": 100,
                    "conflicting_timestamped_entity_groups": 0,
                }
            ],
        }
        self.assertEqual(
            silver_uploader.validate_quality_report(report, extraction_id, retrieved),
            100,
        )

    def test_rejects_wrong_extraction_id(self) -> None:
        extraction_id = "20261120T170816Z"
        retrieved = datetime(2026, 11, 20, 17, 8, 16, tzinfo=timezone.utc)
        report = {
            "dataset": "cpcb_observation_snapshots",
            "conflicting_timestamped_entity_groups": 0,
            "cross_extraction_deduplication_performed": False,
            "combined_unique_record_count": 100,
            "source_extractions": [
                {
                    "extraction_id": "20261116T194631Z",
                    "retrieved_at_utc": "2026-11-20T17:08:16Z",
                    "spark_unique_record_count": 100,
                }
            ],
        }
        with self.assertRaises(RuntimeError):
            silver_uploader.validate_quality_report(report, extraction_id, retrieved)


class FakePaginator:
    def __init__(self, objects: list[dict[str, Any]]) -> None:
        self.objects = objects

    def paginate(self, **kwargs: Any):
        yield {"Contents": self.objects}


class FakeS3:
    def __init__(self, objects: list[dict[str, Any]], heads: dict[str, dict[str, Any]]) -> None:
        self.objects = objects
        self.heads = heads

    def get_paginator(self, name: str) -> FakePaginator:
        return FakePaginator(self.objects)

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return self.heads[Key]


class TestGluePartitionPreconditions(unittest.TestCase):
    def test_requires_nonempty_parquet_inventory_and_encryption_metadata(self) -> None:
        key = "silver/cpcb_observation_snapshots/ingestion_date=2026-11-20/file.parquet"
        fake = FakeS3(
            [{"Key": key, "Size": 123}],
            {
                key: {
                    "ContentLength": 123,
                    "ServerSideEncryption": "AES256",
                    "Metadata": {"sha256": "a" * 64},
                }
            },
        )
        self.assertEqual(
            glue_registrar.inspect_partition_objects(
                fake, "test-bucket", "silver/cpcb_observation_snapshots/ingestion_date=2026-11-20/"
            ),
            (1, 123),
        )

    def test_rejects_empty_partition(self) -> None:
        fake = FakeS3([], {})
        with self.assertRaises(RuntimeError):
            glue_registrar.inspect_partition_objects(
                fake, "test-bucket", "silver/cpcb_observation_snapshots/ingestion_date=2026-11-20/"
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
