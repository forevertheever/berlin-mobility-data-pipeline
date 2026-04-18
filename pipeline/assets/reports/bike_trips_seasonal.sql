/* @bruin

name: sharing_bike_mobility.bike_trips_seasonal
type: bq.sql
connection: bigquery-default

materialization:
  type: table
  strategy: create+replace

@bruin */

SELECT
  season_name,
  SUM(cnt) AS total_rentals,
  AVG(cnt) AS avg_rentals_per_record,
  COUNT(*) AS record_count,
  ROUND(AVG(temp), 2) AS avg_temperature,
  ROUND(AVG(hum), 2) AS avg_humidity,
  ROUND(AVG(windspeed), 2) AS avg_windspeed
FROM `data-engineering-2026-484614.sharing_bike_mobility.stg_bike_trips`
GROUP BY season_name
ORDER BY total_rentals DESC