# Data dictionary and semantic conventions

## Source API fields

| Field | Meaning | Handling |
|---|---|---|
| `country` | Source country label | Retain as source attribute |
| `state` | State/UT label | Retain; normalize only in derived columns if needed |
| `city` | City label | Retain; names may vary across snapshots |
| `station` | Monitoring station display name | Retain; not guaranteed unique/permanent identifier |
| `last_update` | Timestamp string supplied by source | Preserve raw value; do not assume timezone without evidence |
| `latitude` / `longitude` | Coordinates supplied by source | Cast and classify coordinate validity |
| `pollutant_id` | Pollutant code | Observed values: CO, NH3, NO2, OZONE, PM10, PM2.5, SO2 |
| `min_value` | Source-reported minimum | Source string converted to numeric; `NA`/blank becomes null in normalized field |
| `max_value` | Source-reported maximum | Same handling as minimum |
| `avg_value` | Source-reported average | Same handling as minimum |

The inspected source schema did not provide an explicit units field. Units must be verified before cross-pollutant interpretation or official AQI calculation.

## Pipeline metadata

| Field | Meaning |
|---|---|
| `extraction_id` | Unique identifier for one API retrieval run |
| `retrieved_at_utc` | Timestamp when pipeline retrieved the snapshot |
| `ingestion_date` | Date partition derived from pipeline ingestion metadata |
| `source_record_hash` | Hash for identifying exact source-record copies |
| `duplicate_count` / duplicate excess | Within-extraction repeated source occurrences, depending on model/report field |
| `measurement_status` | Classification of available, unavailable, partial or invalid numeric measurements |
| `coordinate_status` | Coordinate validation classification |
| `candidate_station_key` | Derived candidate key for a station name/location combination |
| `candidate_entity_key` | Derived candidate key for station + pollutant |

## Important temporal distinction

`retrieved_at_utc`, source `last_update`, and `ingestion_date` represent different concepts. Keep them separate. A snapshot captures what the endpoint returned at retrieval time; it does not imply that all intermediate observations are available.

## Snapshot comparison statuses

| Status | Meaning |
|---|---|
| `NEW_IN_LATEST` | Entity exists in latest selected snapshot but not in previous selected snapshot |
| `MISSING_FROM_LATEST` | Entity exists in previous selected snapshot but not latest selected snapshot |
| `CHANGED` | Entity occurs in both snapshots but measurement signature or availability differs |
| `UNCHANGED` | Entity occurs in both snapshots with unchanged compared measurement values |

An availability change is a subcategory of `CHANGED`, not a separate status.
