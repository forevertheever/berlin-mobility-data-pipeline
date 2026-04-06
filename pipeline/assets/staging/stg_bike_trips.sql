-- Staging query for bike trips data
-- Transforms raw day and hour tables into human-readable format
-- Downstream of load_to_bigquery.py in Bruin pipeline

SELECT
  'day' as aggregation_level,
  instant,
  dteday,
  CASE season
    WHEN 1 THEN 'Spring'
    WHEN 2 THEN 'Summer'
    WHEN 3 THEN 'Fall'
    WHEN 4 THEN 'Winter'
  END as season_name,
  CASE yr
    WHEN 0 THEN 2011
    WHEN 1 THEN 2012
  END as year,
  mnth,
  CASE mnth
    WHEN 1 THEN 'January'
    WHEN 2 THEN 'February'
    WHEN 3 THEN 'March'
    WHEN 4 THEN 'April'
    WHEN 5 THEN 'May'
    WHEN 6 THEN 'June'
    WHEN 7 THEN 'July'
    WHEN 8 THEN 'August'
    WHEN 9 THEN 'September'
    WHEN 10 THEN 'October'
    WHEN 11 THEN 'November'
    WHEN 12 THEN 'December'
  END as month_name,
  NULL as hr,  -- Not available in day aggregation
  CASE holiday
    WHEN 0 THEN 'No'
    WHEN 1 THEN 'Yes'
  END as is_holiday,
  CASE weekday
    WHEN 0 THEN 'Sunday'
    WHEN 1 THEN 'Monday'
    WHEN 2 THEN 'Tuesday'
    WHEN 3 THEN 'Wednesday'
    WHEN 4 THEN 'Thursday'
    WHEN 5 THEN 'Friday'
    WHEN 6 THEN 'Saturday'
  END as weekday_name,
  CASE workingday
    WHEN 0 THEN 'No'
    WHEN 1 THEN 'Yes'
  END as is_workingday,
  CASE weathersit
    WHEN 1 THEN 'Clear, Few clouds, Partly cloudy'
    WHEN 2 THEN 'Mist + Cloudy, Mist + Broken clouds, Mist + Few clouds, Mist'
    WHEN 3 THEN 'Light Snow, Light Rain + Thunderstorm + Scattered clouds, Light Rain + Scattered clouds'
    WHEN 4 THEN 'Heavy Rain + Ice Pallets + Thunderstorm + Mist, Snow + Fog'
  END as weather_situation,
  temp,
  atemp,
  hum,
  windspeed,
  casual,
  registered,
  cnt
FROM `data-engineering-2026-484614.sharing_bike_mobility.day`

UNION ALL

SELECT
  'hour' as aggregation_level,
  instant,
  dteday,
  CASE season
    WHEN 1 THEN 'Spring'
    WHEN 2 THEN 'Summer'
    WHEN 3 THEN 'Fall'
    WHEN 4 THEN 'Winter'
  END as season_name,
  CASE yr
    WHEN 0 THEN 2011
    WHEN 1 THEN 2012
  END as year,
  mnth,
  CASE mnth
    WHEN 1 THEN 'January'
    WHEN 2 THEN 'February'
    WHEN 3 THEN 'March'
    WHEN 4 THEN 'April'
    WHEN 5 THEN 'May'
    WHEN 6 THEN 'June'
    WHEN 7 THEN 'July'
    WHEN 8 THEN 'August'
    WHEN 9 THEN 'September'
    WHEN 10 THEN 'October'
    WHEN 11 THEN 'November'
    WHEN 12 THEN 'December'
  END as month_name,
  hr,
  CASE holiday
    WHEN 0 THEN 'No'
    WHEN 1 THEN 'Yes'
  END as is_holiday,
  CASE weekday
    WHEN 0 THEN 'Sunday'
    WHEN 1 THEN 'Monday'
    WHEN 2 THEN 'Tuesday'
    WHEN 3 THEN 'Wednesday'
    WHEN 4 THEN 'Thursday'
    WHEN 5 THEN 'Friday'
    WHEN 6 THEN 'Saturday'
  END as weekday_name,
  CASE workingday
    WHEN 0 THEN 'No'
    WHEN 1 THEN 'Yes'
  END as is_workingday,
  CASE weathersit
    WHEN 1 THEN 'Clear, Few clouds, Partly cloudy'
    WHEN 2 THEN 'Mist + Cloudy, Mist + Broken clouds, Mist + Few clouds, Mist'
    WHEN 3 THEN 'Light Snow, Light Rain + Thunderstorm + Scattered clouds, Light Rain + Scattered clouds'
    WHEN 4 THEN 'Heavy Rain + Ice Pallets + Thunderstorm + Mist, Snow + Fog'
  END as weather_situation,
  temp,
  atemp,
  hum,
  windspeed,
  casual,
  registered,
  cnt
FROM `data-engineering-2026-484614.sharing_bike_mobility.hour`
