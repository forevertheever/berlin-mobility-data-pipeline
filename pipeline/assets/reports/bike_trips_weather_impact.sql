/* @bruin

name: sharing_bike_mobility.bike_trips_weather_impact
type: bq.sql
connection: bigquery-default

materialization:
  type: table
  strategy: create+replace

depends:
  - sharing_bike_mobility.stg_bike_trips

@bruin */

SELECT
  weather_situation,
  COUNT(*) as record_count,
  ROUND(AVG(temp), 2) as avg_temperature,
  ROUND(AVG(atemp), 2) as avg_feel_temperature,
  ROUND(AVG(hum), 2) as avg_humidity,
  ROUND(AVG(windspeed), 2) as avg_windspeed,
  ROUND(AVG(cnt), 2) as avg_total_rentals,
  ROUND(AVG(casual), 2) as avg_casual_rentals,
  ROUND(AVG(registered), 2) as avg_registered_rentals,
  MIN(cnt) as min_rentals,
  MAX(cnt) as max_rentals,
  SUM(cnt) as total_rentals
FROM `{{ project_id }}.{{ dataset_id }}.stg_bike_trips`
WHERE weather_situation IS NOT NULL
GROUP BY weather_situation
ORDER BY total_rentals DESC
