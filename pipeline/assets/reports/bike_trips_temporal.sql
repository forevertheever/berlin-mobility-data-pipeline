/* @bruin

name: sharing_bike_mobility.bike_trips_temporal
type: bq.sql
connection: bigquery-default

materialization:
  type: table
  strategy: create+replace

@bruin */

SELECT
  year,
  mnth AS month_number,
  month_name,
  SUM(cnt) AS total_rentals,
  AVG(cnt) AS avg_rentals_per_record,
  COUNT(*) AS record_count,
  ROUND(AVG(temp), 2) AS avg_temperature,
  ROUND(AVG(hum), 2) AS avg_humidity
FROM `{{ project_id }}.{{ dataset_id }}.stg_bike_trips`
WHERE year IN (2011, 2012)
GROUP BY year, mnth, month_name
ORDER BY year, mnth