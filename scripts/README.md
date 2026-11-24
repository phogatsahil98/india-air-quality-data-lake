# Scripts

This directory contains the Python utilities used by the India Air Quality Data Lake.

## Production pipeline scripts

The production Airflow DAG calls these five scripts in order:

| Order | Path | Responsibility |
|---:|---|---|
| 1 | `ingestion/profile_cpcb_full.py` | Fetch all CPCB API pages and preserve the original JSON envelopes |
| 2 | `ingestion/upload_bronze_dynamic.py` | Validate and upload one immutable extraction to S3 Bronze |
| 3 | `transformation/build_historical_silver.py` | Download the selected Bronze extraction(s), validate, deduplicate within each extraction and write Silver Parquet |
| 4 | `aws/upload_silver_incremental.py` | Upload one extraction's Silver Parquet, report and manifest with checksum verification |
| 5 | `aws/register_silver_partition.py` | Verify the S3 partition and register it in AWS Glue |

The Airflow DAG is the orchestrator. These scripts remain independently executable and own the actual ingestion, transformation and AWS operations.

## Production execution contracts

Run from the repository root with the local `.venv` activated.

### 1. Extract CPCB

```powershell
python .\scripts\ingestion\profile_cpcb_full.py
```

Requires `DATA_GOV_API_KEY` in the environment or root `.env`. It creates a unique `data/raw/source_profile_<EXTRACTION_ID>/` directory.

The extractor requires a stable API-reported total throughout pagination and verifies that the collected count matches that total. A changing total causes the extraction to fail rather than publish a potentially incomplete snapshot.

### 2. Upload Bronze

```powershell
python .\scripts\ingestion\upload_bronze_dynamic.py `
  --extraction-id <EXTRACTION_ID>
```

Use `--dry-run` to validate the local page inventory, API total and page offsets without making AWS calls or writes.

The script writes its S3 manifest last. Existing objects are checksum-verified; conflicting objects are not overwritten.

### 3. Build Silver

```powershell
python .\scripts\transformation\build_historical_silver.py `
  --extraction-ids <EXTRACTION_ID> `
  --output-dir .\data\processed\airflow_runs\<EXTRACTION_ID>\cpcb_observation_snapshots `
  --overwrite-output
```

The extraction IDs are mandatory; the script does not contain a default historical run list.

Recursive output deletion is limited to generated child paths under:
- `data/processed/historical_silver/`
- `data/processed/airflow_runs/`

The builder verifies Bronze manifests, S3 page inventories, page checksums, object sizes and record totals before Spark reads the JSON. It removes exact duplicates within each extraction only. It does not deduplicate across extraction IDs.

`ingested_at_utc` is sourced from the immutable Bronze manifest's `uploaded_at_utc`. For legacy Bronze manifests without that field, it falls back to the UTC extraction timestamp. This makes a replay of the same immutable Bronze extraction deterministic at the record-value level.

The Spark output partition count can be adjusted with `--output-partitions` (default: 4).

### 4. Upload incremental Silver

```powershell
python .\scripts\aws\upload_silver_incremental.py `
  --extraction-id <EXTRACTION_ID> `
  --dataset-dir <SILVER_DATASET_DIRECTORY> `
  --quality-report <QUALITY_REPORT_JSON>
```

The script validates the one-extraction quality report, uploads Parquet objects, verifies object size, SSE-S3 encryption and SHA-256, stores the quality report, and writes the immutable manifest last.

A retry with an existing manifest compares file content hashes and sizes rather than Spark-generated part filenames, then verifies the already-committed objects and quality report.

### 5. Register Glue partition

```powershell
python .\scripts\aws\register_silver_partition.py `
  --ingestion-date <YYYY-MM-DD>
```

The script refuses to register a partition unless non-empty Parquet objects exist at the expected S3 prefix and have the expected encryption and SHA-256 metadata. It validates the Glue table and partition schema/location and is safe to rerun for a matching partition.

## Other script categories

| Directory | Purpose |
|---|---|
| `bootstrap/aws/` | Initial S3 bucket, Glue database and Athena workgroup setup |
| `bootstrap/historical_initial_load/` | One-time historical Silver upload and Glue registration |
| `bootstrap/legacy_single_snapshot/` | Original single-snapshot implementation retained for historical reference |
| `research/source_audit/` | Dataset discovery, source profiling and initial data-quality investigation |
| `research/snapshot_comparison/` | Snapshot comparison utilities; prefer `compare_cpcb_snapshots_v2.py` |
| `diagnostics/` | Local AWS and Spark runtime checks |
| `documentation/` | dbt lineage and documentation audit utilities |

Bootstrap, research and legacy scripts are not called by the production DAG. Do not use the hard-coded legacy single-snapshot scripts for new ingestion runs.

## Automated tests

The active-script safety tests are stored under `tests/unit/test_pipeline_safety.py`.

Run them from the repository root:

```powershell
python -m unittest discover -s tests/unit -v
```

These tests validate pagination completeness, numeric handling, safe Silver output paths, single-extraction quality-report checks and Glue partition preconditions. They do not replace an AWS integration run or a full Airflow DAG test.
