# dbt canonical source migration

## Decision

The extraction-aware Glue table `india_aqi_lake.cpcb_observation_snapshots`
is the only active dbt source for current-state and historical analytics.

The current-state dimensions, fact and city marts now use the most recently
retrieved complete extraction through `int_cpcb_latest_snapshot` and
`int_cpcb_observation_quality`. Snapshot comparison models continue to read all
retained extraction rows from `stg_cpcb_observation_snapshots`.

## Why

The former `cpcb_observations` table contains only the initial single snapshot.
If the star schema continued using that table while Airflow appended new
extractions to `cpcb_observation_snapshots`, the current-state marts would
silently become stale while dbt builds still passed.

## Semantic choices

- `ingestion_date` is the UTC retrieval date/Glue partition, not an observation date.
- `source_last_update_raw` remains text; no timezone or source timestamp semantics are assumed.
- `candidate_station_key` and `candidate_entity_key` are label-derived keys, not official CPCB IDs.
- The current-state star schema uses one latest complete extraction, not an entity-by-entity carry-forward.
- Historical change marts compare the latest and immediately previous full extractions.
- No official AQI calculation is introduced by this migration.

## Validation

After applying the migration:

1. `dbt parse`
2. `dbt ls` and verify there is no active `stg_cpcb_observations` model.
3. `dbt build --fail-fast`
4. Verify `int_cpcb_latest_snapshot` has one extraction ID.
5. Verify `fct_station_pollutant_observation` has that same extraction ID only.
6. Verify both city marts are grouped by `ingestion_date` and `extraction_id`.
7. Verify snapshot comparison totals remain unchanged for the existing two-snapshot bootstrap data.

The legacy Glue table and any already-created Athena view are not deleted by this
migration. Drop the old dbt-created Athena view only after successful validation
and only if it is no longer needed:

```sql
DROP VIEW IF EXISTS india_aqi_dbt_dev.stg_cpcb_observations;
```
