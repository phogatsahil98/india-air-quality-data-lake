{{ config(materialized='view') }}


SELECT

    f.city_key,

    c.country,

    c.state,

    c.city,

    f.observation_date,

    f.ingestion_date,

    f.extraction_id,

    COUNT(*) AS station_pollutant_records,

    COUNT(
        DISTINCT f.station_key
    ) AS stations_reporting,

    COUNT(
        DISTINCT f.pollutant_id
    ) AS pollutants_reported,

    COUNT_IF(
        f.measurement_status = 'AVAILABLE'
    ) AS available_measurements,

    COUNT_IF(
        f.measurement_status = 'UNAVAILABLE'
    ) AS unavailable_measurements,

    COUNT_IF(
        f.is_analytically_usable
    ) AS analytically_usable_measurements,

    ROUND(

        100.0 * COUNT_IF(
            f.is_analytically_usable
        )

/
NULLIF(COUNT(*), 0),

        2

    ) AS measurement_availability_pct

FROM {{ ref('fct_station_pollutant_observation') }} f

INNER JOIN {{ ref('dim_city') }} c

    ON f.city_key = c.city_key

GROUP BY

    f.city_key,

    c.country,

    c.state,

    c.city,

    f.observation_date,

    f.ingestion_date,

    f.extraction_id