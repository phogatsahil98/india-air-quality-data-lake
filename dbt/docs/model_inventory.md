# India AQI — dbt Model Inventory

Generated from the dbt manifest.

Total project models: 9

## Staging

### stg_cpcb_observations

- Materialization: `view`
- File: `models\staging\stg_cpcb_observations.sql`
- Database: `awsdatacatalog`
- Schema: `india_aqi_dbt_dev`
- Upstream resource count: 1
- Description: Standardized staging model for CPCB air quality observations from Indian monitoring stations.


## Intermediate

### int_cpcb_observation_quality

- Materialization: `view`
- File: `models\intermediate\int_cpcb_observation_quality.sql`
- Database: `awsdatacatalog`
- Schema: `india_aqi_dbt_dev`
- Upstream resource count: 1
- Description: Evaluates the quality of each CPCB observation using measurement availability, coordinate validity, timestamp validity and measurement consistency.


### int_cpcb_usable_measurements

- Materialization: `view`
- File: `models\intermediate\int_cpcb_usable_measurements.sql`
- Database: `awsdatacatalog`
- Schema: `india_aqi_dbt_dev`
- Upstream resource count: 1
- Description: Contains CPCB measurements that passed the intermediate data-quality assessment.


## Gold - Dimensions

### dim_city

- Materialization: `view`
- File: `models\marts\dimensions\dim_city.sql`
- Database: `awsdatacatalog`
- Schema: `india_aqi_dbt_dev`
- Upstream resource count: 1
- Description: City dimension containing normalized geographic identifiers and original CPCB city labels.


### dim_pollutant

- Materialization: `view`
- File: `models\marts\dimensions\dim_pollutant.sql`
- Database: `awsdatacatalog`
- Schema: `india_aqi_dbt_dev`
- Upstream resource count: 1
- Description: Reference dimension for CPCB pollutants.


### dim_station

- Materialization: `view`
- File: `models\marts\dimensions\dim_station.sql`
- Database: `awsdatacatalog`
- Schema: `india_aqi_dbt_dev`
- Upstream resource count: 1
- Description: Monitoring station dimension.


## Gold - Facts

### fct_station_pollutant_observation

- Materialization: `view`
- File: `models\marts\facts\fct_station_pollutant_observation.sql`
- Database: `awsdatacatalog`
- Schema: `india_aqi_dbt_dev`
- Upstream resource count: 4
- Description: Central fact table containing exact station-level pollutant observations for each extraction.


## Gold - Aggregates

### mart_city_data_completeness

- Materialization: `view`
- File: `models\marts\aggregates\mart_city_data_completeness.sql`
- Database: `awsdatacatalog`
- Schema: `india_aqi_dbt_dev`
- Upstream resource count: 2
- Description: City-level monitoring observation availability for each extraction and observation date.


### mart_city_pollutant_snapshot

- Materialization: `view`
- File: `models\marts\aggregates\mart_city_pollutant_snapshot.sql`
- Database: `awsdatacatalog`
- Schema: `india_aqi_dbt_dev`
- Upstream resource count: 3
- Description: City and pollutant level summary for each extraction and observation date.

