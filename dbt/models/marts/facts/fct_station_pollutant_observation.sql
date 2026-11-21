{{ config(materialized='view') }}


WITH observations AS (

    SELECT *

    FROM {{ ref('int_cpcb_observation_quality') }}

),

enriched_observations AS (

    SELECT

        *,

        CONCAT(

            LOWER(TRIM(country)),
            '|',
            LOWER(TRIM(state)),
            '|',
            city_normalized

        ) AS derived_city_key

    FROM observations

)

SELECT

    CONCAT(
        o.extraction_id,
        '|',
        o.observation_id
    ) AS fact_observation_key,

    o.observation_id,

    c.city_key,

    s.station_key,

    p.pollutant_id,

    o.observation_date,

    o.ingestion_date,

    o.source_last_update,

    o.extraction_id,

    o.min_value,

    o.max_value,

    o.avg_value,

    o.measurement_status,

    o.record_quality_reason,

    o.is_analytically_usable,

    o.coordinate_status,

    o.timestamp_parse_status,

    o.range_consistency_status,

    o.source_occurrence_count,

    o.source_page_count

FROM enriched_observations o

LEFT JOIN {{ ref('dim_city') }} c

    ON o.derived_city_key = c.city_key

LEFT JOIN {{ ref('dim_station') }} s

    ON o.station_key = s.station_key

LEFT JOIN {{ ref('dim_pollutant') }} p

    ON o.pollutant_id = p.pollutant_id