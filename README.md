# CPCB historical dbt extension

This bundle adds a separate dbt source and two views without changing the
existing `cpcb_silver` source, `stg_cpcb_observations`, intermediate models,
or current Gold marts.

## Files to copy into the existing dbt project

Copy the bundle's `models/` and `tests/` contents into the matching directories
inside the existing `dbt/` project:

- `models/sources_historical.yml`
- `models/staging/stg_cpcb_observation_snapshots.sql`
- `models/staging/stg_cpcb_observation_snapshots.yml`
- `models/intermediate/int_cpcb_latest_snapshot.sql`
- `models/intermediate/int_cpcb_latest_snapshot.yml`
- all four `.sql` tests under `tests/`

The new source is named `cpcb_silver_history` to avoid redefining the existing
`cpcb_silver` source.

## Activate the dbt environment

From the project root in PowerShell:

```powershell
deactivate
.\.venv-dbt\Scripts\Activate.ps1
Get-Command dbt
dbt --version
```

Confirm that `Get-Command dbt` resolves to this project's `.venv-dbt\Scripts\dbt.exe`
and that dbt Core 1.12.7 / dbt-athena 1.11.2 are active.

## Validate and build

```powershell
cd .\dbt
dbt parse
dbt ls --select stg_cpcb_observation_snapshots int_cpcb_latest_snapshot

dbt run --select stg_cpcb_observation_snapshots int_cpcb_latest_snapshot --fail-fast
dbt test --select stg_cpcb_observation_snapshots int_cpcb_latest_snapshot
```

The four singular tests are included in the test selection when their model
references are selected by dbt's graph selection. If the adapter's graph
selection does not include them, run them explicitly:

```powershell
dbt test --select assert_cpcb_snapshot_source_grain_unique assert_cpcb_snapshot_extraction_metadata_consistent assert_cpcb_snapshot_no_conflicting_timestamped_entities assert_cpcb_latest_snapshot_contains_one_extraction
```

## Expected checks with the current two snapshots

- `stg_cpcb_observation_snapshots`: 7,324 rows.
- `int_cpcb_latest_snapshot`: 3,648 rows from extraction `20261120T170816Z`.
- `ingestion_date` values in staging: `2026-11-16` and `2026-11-20`.
- Seven pollutant IDs in both extractions.
- No source-level duplicate `(extraction_id, source_record_hash)` groups.
- No conflicting `(extraction_id, candidate_entity_key, source_last_update_raw)` groups.

The exact row counts are validation expectations for this bootstrap dataset;
new future extractions will increase the historical staging row count.

## Modeling caveats

- `source_last_update_raw` stays text: no timezone or source timestamp semantics
  are inferred.
- `candidate_station_key` and `candidate_entity_key` are derived from source
  labels, not official CPCB identifiers.
- `int_cpcb_latest_snapshot` selects the newest extraction as a whole. It does
  not retain older-only entities as if they were in the latest API response.
- This work does not calculate official CPCB AQI.
