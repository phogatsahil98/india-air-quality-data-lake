{{ config(materialized='view', tags=['cpcb', 'historical', 'canonical_source']) }}

select
    cast(extraction_id as varchar) as extraction_id,
    cast(retrieved_at_utc as timestamp) as retrieved_at_utc,
    cast(ingestion_date as date) as ingestion_date,

    -- Preserve CPCB's timestamp text. Its timezone and exact business meaning
    -- have not been independently established.
    source_last_update_raw,
    source_last_update_raw as source_last_update,

    nullif(trim(country), '') as country,
    nullif(trim(state), '') as state,
    nullif(trim(city), '') as city,
    lower(trim(city)) as city_normalized,
    nullif(trim(station), '') as station,

    candidate_station_key,
    candidate_station_key as station_key,
    candidate_entity_key,
    source_record_hash,
    source_record_hash as observation_id,

    upper(trim(pollutant_id)) as pollutant_id,

    latitude_raw,
    longitude_raw,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    coordinate_status,

    min_value_raw,
    max_value_raw,
    avg_value_raw,
    cast(min_value as double) as min_value,
    cast(max_value as double) as max_value,
    cast(avg_value as double) as avg_value,
    measurement_status,

    cast(source_occurrence_count as bigint) as source_occurrence_count,
    cast(duplicate_excess_count as bigint) as duplicate_excess_count,
    source_pages,
    cast(source_page_count as integer) as source_page_count,
    cast(ingested_at_utc as timestamp) as ingested_at_utc

from {{ source('cpcb_silver', 'cpcb_observation_snapshots') }}
