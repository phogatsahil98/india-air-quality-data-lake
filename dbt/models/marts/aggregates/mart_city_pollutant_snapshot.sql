{{ config(materialized='view') }}


SELECT

    f.city_key,

    c.country,

    c.state,

    c.city,

    f.pollutant_id,

    p.pollutant_name,

    p.pollutant_category,

    f.observation_date,

    f.ingestion_date,

    f.extraction_id,

    COUNT(*) AS station_observation_count,

    COUNT_IF(
        f.is_analytically_usable
    ) AS usable_observation_count,

    COUNT_IF(
        NOT f.is_analytically_usable
    ) AS unusable_observation_count,

    COUNT(
        DISTINCT f.station_key
    ) AS reporting_station_count,

    COUNT(
        DISTINCT CASE

            WHEN f.is_analytically_usable
                THEN f.station_key

        END
    ) AS usable_station_count,

    ROUND(

        100.0 * COUNT_IF(
            f.is_analytically_usable
        )

/
NULLIF(COUNT(*), 0),

        2

    ) AS measurement_availability_pct,

    ROUND(

        AVG(

            CASE

                WHEN f.is_analytically_usable
                    THEN f.avg_value

            END

        ),

        2

    ) AS mean_station_avg_value,

    MAX(

        CASE

            WHEN f.is_analytically_usable
                THEN f.max_value

        END

    ) AS highest_station_max_value,

    MIN(

        CASE

            WHEN f.is_analytically_usable
                THEN f.min_value

        END

    ) AS lowest_station_min_value

FROM {{ ref('fct_station_pollutant_observation') }} f

INNER JOIN {{ ref('dim_city') }} c

    ON f.city_key = c.city_key

INNER JOIN {{ ref('dim_pollutant') }} p

    ON f.pollutant_id = p.pollutant_id

GROUP BY

    f.city_key,

    c.country,

    c.state,

    c.city,

    f.pollutant_id,

    p.pollutant_name,

    p.pollutant_category,

    f.observation_date,

    f.ingestion_date,

    f.extraction_id