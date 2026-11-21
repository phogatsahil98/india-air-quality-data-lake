{{ config( materialized='view' ) }}


SELECT

    observation_id,

    station_key,

    country,
    state,
    city,
    city_normalized,
    station,

    observation_date,
    ingestion_date,

    latitude,
    longitude,

    pollutant_id,

    min_value,
    max_value,
    avg_value,

    measurement_status,

    record_quality_reason,

    is_analytically_usable,

    source_page_count,
    source_occurrence_count,

    extraction_id

FROM {{ ref('int_cpcb_observation_quality') }}

WHERE is_analytically_usable = TRUE