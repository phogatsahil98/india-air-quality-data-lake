# CPCB snapshot analytics dbt extension

This extension adds a snapshot comparison intermediate model and two Gold views.
It does not replace or modify the existing CPCB models and marts.

## Files to copy

Copy the bundle's `models/intermediate/`, `models/marts/`, and `tests/` contents
into the same directories in the existing `dbt/` project.

## Run from the dbt project directory

```powershell
dbt parse
dbt ls --select int_cpcb_snapshot_entity_comparison mart_cpcb_snapshot_change_detail mart_cpcb_snapshot_change_summary

dbt run --select int_cpcb_snapshot_entity_comparison mart_cpcb_snapshot_change_detail mart_cpcb_snapshot_change_summary --fail-fast
dbt test --select int_cpcb_snapshot_entity_comparison mart_cpcb_snapshot_change_detail mart_cpcb_snapshot_change_summary
```

If the adapter's graph selection does not include singular tests, run the three
explicitly:

```powershell
dbt test --select assert_cpcb_snapshot_comparison_has_two_extractions assert_cpcb_snapshot_comparison_status_reconciles assert_cpcb_snapshot_availability_change_is_common_entity
```

## Model behavior

- `int_cpcb_snapshot_entity_comparison` compares the latest and immediately
  previous complete extractions, ranking runs by `retrieved_at_utc` and then
  `extraction_id`.
- The join uses `candidate_entity_key` and is a full outer join so newly appearing
  and missing entities are preserved.
- `CHANGED` is based on numeric measurement values or measurement status. The
  source `last_update` string is not itself treated as a measurement change.
- Average-value deltas are only calculated when both values are non-null. Percent
  changes are omitted when the previous value is zero.
- Station and entity keys are candidate keys derived from source labels, not
  official CPCB station identifiers.
- The summary and detail marts are views and do not write new Parquet datasets.
- Two source snapshots do not establish a continuous hourly time series, and
  these models do not calculate official CPCB AQI.
- Measurement units have not yet been verified from the source metadata; do not
  interpret cross-pollutant averages or compare magnitudes across pollutants.

## Expected current two-snapshot reconciliation

The comparison has one output row per unioned candidate entity key. Based on the
prior source comparison, the expected classification is 390 new, 418 missing,
2,899 changed, and 359 unchanged entities. 209 common entities changed
measurement availability. These are validation targets for the current two
snapshots; future extractions will change them.
