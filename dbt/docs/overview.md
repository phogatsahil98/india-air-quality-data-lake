{% docs __overview__ %}

# India Air Quality Analytics

## Project objective

Build an AWS data lakehouse for CPCB / data.gov.in station-level pollutant
snapshots, preserving each extraction and producing reproducible analytical
views.

## Architecture

CPCB API → Python extraction → S3 Bronze JSON → PySpark → S3 Silver Parquet
→ Glue Data Catalog → Athena → dbt → current-state and snapshot-comparison marts.
Airflow orchestrates extraction, storage, partition registration and dbt builds.

## Canonical dbt source and model flow

The active source is `cpcb_observation_snapshots`, which retains records from
all extractions. `stg_cpcb_observation_snapshots` is the canonical staging model.
`int_cpcb_latest_snapshot` selects the latest complete extraction as a whole.
The current-state quality layer and star schema are built from that latest
snapshot. Historical comparison models read all snapshots to compare the latest
and immediately preceding extraction.

## Data layers

### Bronze

Immutable source API page JSON and extraction manifests in S3.

### Silver

Extraction-aware Parquet. Exact duplicates are removed within each extraction;
records from separate extractions remain distinct. Measurement and coordinate
status are retained with source provenance.

### Gold

Current-state dimensions, fact and city-level summary marts, alongside separate
entity-level and aggregated snapshot-change marts.

## Important analytical distinctions

`retrieved_at_utc` describes when our pipeline fetched a snapshot. The original
CPCB `last_update` is preserved as raw text because its timezone and exact
semantics have not been independently established. `ingestion_date` is the
retrieval-date partition, not an observation date.

Pollutant concentration and official CPCB AQI are different. The inspected
source schema has no explicit measurement-units field and these snapshots do
not provide a continuous hourly history. The project therefore does not claim
official AQI calculation or seasonal/hourly trend reconstruction.

## Technology stack

- Python and PySpark
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- dbt Core and dbt-athena
- Apache Airflow with CeleryExecutor

{% enddocs %}
