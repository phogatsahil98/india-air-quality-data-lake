{{ config(materialized='view') }}


WITH pollutants AS (

    SELECT DISTINCT

        pollutant_id

    FROM {{ ref('int_cpcb_observation_quality') }}

    WHERE pollutant_id IS NOT NULL

)

SELECT

    pollutant_id,

    CASE pollutant_id

        WHEN 'CO' THEN 'Carbon Monoxide'

        WHEN 'NH3' THEN 'Ammonia'

        WHEN 'NO2' THEN 'Nitrogen Dioxide'

        WHEN 'OZONE' THEN 'Ground-level Ozone'

        WHEN 'PM10' THEN 'Particulate Matter PM10'

        WHEN 'PM2.5' THEN 'Particulate Matter PM2.5'

        WHEN 'SO2' THEN 'Sulphur Dioxide'

        ELSE 'Unknown Pollutant'

    END AS pollutant_name,

    CASE

        WHEN pollutant_id IN ('PM10', 'PM2.5')
            THEN 'PARTICULATE'

        ELSE 'GASEOUS'

    END AS pollutant_category

FROM pollutants