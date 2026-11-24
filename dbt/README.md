# dbt — India Air Quality Analytics

## Canonical source

`cpcb_observation_snapshots` is the active dbt source. It contains all retained
extraction-aware CPCB Silver snapshots. The former `cpcb_observations` Glue
 table is retained as a legacy bootstrap artifact and is not referenced by
active dbt models.

## Model flow

```text
cpcb_observation_snapshots (Glue source; all extractions)
  ├── stg_cpcb_observation_snapshots (typed historical staging)
  │    ├── int_cpcb_latest_snapshot (one latest complete extraction)
  │    │    └── int_cpcb_observation_quality (current-state quality assessment)
  │    │         ├── int_cpcb_usable_measurements
  │    │         ├── dim_city / dim_station / dim_pollutant
  │    │         └── fct_station_pollutant_observation
  │    │              ├── mart_city_pollutant_snapshot
  │    │              └── mart_city_data_completeness
  │    └── int_cpcb_snapshot_entity_comparison (latest vs previous extraction)
  │         ├── mart_cpcb_snapshot_change_detail
  │         └── mart_cpcb_snapshot_change_summary
```

The current-state star schema uses the latest complete extraction as a whole.
The snapshot-comparison models read all retained extractions. The model does
not carry forward station-pollutant entities missing from the latest response.

## Time semantics

- `retrieved_at_utc` and `ingestion_date` describe our extraction/retrieval.
- `source_last_update_raw` preserves the CPCB `last_update` string as received.
- No timezone or source observation-period semantics are inferred from the raw
  timestamp text.
- These snapshots do not constitute a continuous hourly time series.
- This project does not calculate official CPCB AQI.

## Run

Use the project's `.venv-dbt` environment on Windows or the dedicated
`/opt/dbt-venv` inside the Airflow worker. Do not share dbt `target/` or partial
parse artifacts between Windows and Linux. Airflow runs dbt with a Linux-only
target path and `--no-partial-parse`.

```powershell
cd dbt
dbt parse
dbt build --fail-fast
dbt docs generate
```

## Retired single-snapshot artifacts

`stg_cpcb_observations` and source `cpcb_silver.cpcb_observations` were used by
the original one-time bootstrap path. The staging model is archived outside
`models/`; the Glue table is left intact until a separate retention decision.
