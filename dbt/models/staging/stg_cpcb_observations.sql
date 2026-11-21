{{ config( materialized='view' ) }}

SELECT

-- Observation identity
record_hash AS observation_id, station_key,

-- Geography
TRIM(country) AS country,
TRIM(state) AS state,
TRIM(city) AS city,
city_normalized,
TRIM(station) AS station,

-- Observation timing
last_update_raw AS source_last_update,
observation_date,
ingestion_date,
ingestion_timestamp_utc,

-- Geographic coordinates
latitude, longitude,

-- Pollutant information
UPPER(TRIM(pollutant_id)) AS pollutant_id,

-- Pollutant measurements
min_value, max_value, avg_value,

-- Data-quality metadata
measurement_status,
coordinate_status,
timestamp_parse_status,
range_consistency_status,
CASE
    WHEN measurement_status = 'AVAILABLE' THEN TRUE
    ELSE FALSE
END AS is_measurement_available,
CASE
    WHEN coordinate_status = 'VALID' THEN TRUE
    ELSE FALSE
END AS is_coordinate_valid,
CASE
    WHEN timestamp_parse_status = 'VALID' THEN TRUE
    ELSE FALSE
END AS is_timestamp_valid,

-- Source provenance
source_pages,
source_page_count,
source_occurrence_count,
duplicate_excess_count,

-- Extraction metadata
extraction_id

FROM {{ source(
    'cpcb_silver',
    'cpcb_observations'
) }}