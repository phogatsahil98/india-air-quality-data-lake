{{ config(materialized='view') }}


WITH station_attributes AS (

    SELECT

        station_key,

        MIN(country) AS country,

        MIN(state) AS state,

        MIN(city) AS city,

        MIN(city_normalized) AS city_normalized,

        MIN(station) AS station_name,

        COUNT(
            DISTINCT CONCAT(
                CAST(latitude AS VARCHAR),
                '|',
                CAST(longitude AS VARCHAR)
            )
        ) AS coordinate_pair_count

    FROM {{ ref('int_cpcb_observation_quality') }}

    GROUP BY station_key

),

coordinate_pairs AS (

    SELECT

        station_key,

        latitude,

        longitude,

        COUNT(*) AS pair_frequency

    FROM {{ ref('int_cpcb_observation_quality') }}

    WHERE

        latitude IS NOT NULL

        AND longitude IS NOT NULL

    GROUP BY

        station_key,

        latitude,

        longitude

),

ranked_coordinates AS (

    SELECT

        *,

        ROW_NUMBER() OVER (

            PARTITION BY station_key

            ORDER BY

                pair_frequency DESC,

                latitude,

                longitude

        ) AS coordinate_rank

    FROM coordinate_pairs

)

SELECT

    s.station_key,

    s.country,

    s.state,

    s.city,

    s.city_normalized,

    s.station_name,

    c.latitude,

    c.longitude,

    s.coordinate_pair_count,

    CASE

        WHEN s.coordinate_pair_count = 1
            THEN 'CONSISTENT'

        WHEN s.coordinate_pair_count > 1
            THEN 'MULTIPLE_COORDINATE_PAIRS'

        ELSE 'UNAVAILABLE'

    END AS station_coordinate_status

FROM station_attributes s

LEFT JOIN ranked_coordinates c

    ON s.station_key = c.station_key

    AND c.coordinate_rank = 1