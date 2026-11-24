{{ config(materialized='view', tags=['cpcb', 'current_state', 'gold']) }}

select
    f.city_key,
    c.country,
    c.state,
    c.city,
    f.ingestion_date,
    f.extraction_id,
    max(f.retrieved_at_utc) as retrieved_at_utc,
    count(*) as station_pollutant_records,
    count(distinct f.station_key) as stations_reporting,
    count(distinct f.pollutant_id) as pollutants_reported,
    count_if(f.measurement_status = 'AVAILABLE') as available_measurements,
    count_if(f.measurement_status = 'UNAVAILABLE') as unavailable_measurements,
    count_if(f.is_analytically_usable) as analytically_usable_measurements,
    round(
        100.0 * count_if(f.is_analytically_usable) / nullif(count(*), 0),
        2
    ) as measurement_availability_pct
from {{ ref('fct_station_pollutant_observation') }} as f
inner join {{ ref('dim_city') }} as c
    on f.city_key = c.city_key
group by
    f.city_key,
    c.country,
    c.state,
    c.city,
    f.ingestion_date,
    f.extraction_id
