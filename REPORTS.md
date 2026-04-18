# Berlin Mobility Data Pipeline - Reports Documentation

## Overview

This document describes the visualizations and insights available in the Looker Studio dashboard for the bike sharing data pipeline.

## Dashboard Link

**[Berlin Mobility Data Pipeline Report](https://lookerstudio.google.com/reporting/07dca53f-9446-40e1-acc3-fdaeaf289ede)**

**Access Level**: Viewer

---

## Visualizations

### 1. Bike Trips by Season (2011-2012)

**Data Source**: `bike_trips_seasonal` table

**Description**: 
This visualization shows bike rental volumes aggregated by season across the entire dataset (2011-2012).

**Key Insights**:
- Seasonal demand patterns and peak seasons

**Metrics**:
- Total rentals by season over years 2011 and 2012

---

### 2. Bike Demand Trends (2011-2012)

**Data Source**: `bike_trips_temporal` table

**Description**:
This visualization displays monthly bike rental trends across the 2011-2012 period.

**Key Insights**:
- Growth or decline patterns month-to-month
- Year-over-year comparison (2011 vs 2012)

**Metrics**:
- Monthly total rentals

---

### 3. Weather Impact on Trips

**Data Source**: `bike_trips_weather_impact` table

**Description**:
This visualization analyzes how different weather conditions affect bike rental volumes.

**Key Insights**:
- Correlation between weather situations and trip volumes
- Impact of temperature, humidity, and wind on demand

**Metrics**:
- Total trips by weather condition
- Average rentals under each weather situation
- Extreme weather impact (clear vs heavy rain)

---

## Data Freshness

The dashboard is connected to BigQuery tables that are updated via the Bruin pipeline:

- **Frequency**: Daily incremental batch processing
- **Latest Update**: Automatically reflects the most recent data from `share_bike_mobility` dataset
- **Pipeline Assets**: 
  - `stg_bike_trips` - Staging transformations
  - `bike_trips_seasonal` - Seasonal aggregations
  - `bike_trips_temporal` - Monthly trends
  - `bike_trips_weather_impact` - Weather analysis


