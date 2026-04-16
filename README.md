# Berlin Mobility Data Pipeline

A Bruin-based data pipeline for processing Berlin bike sharing data from local archive → Google Cloud Storage → BigQuery → staging transformations.

## Overview

This pipeline demonstrates a complete **batch-processing data engineering workflow**:

1. **Ingestion**: Upload local CSV files (`day.csv`, `hour.csv`) from `archive/` to Google Cloud Storage
2. **Loading**: Load CSV data from GCS into BigQuery tables
3. **Staging**: Transform raw data into human-readable format with mapped values
4. **Reporting**: Generate analytical reports on seasonal and temporal trends

**Batch Processing Features**:
- Daily scheduled execution with incremental SQL transformations
- Date-range filtering for processing specific time windows
- Full refresh capability for initial loads or reprocessing
- Dependency-based execution ensuring data consistency

## Prerequisites

- Python 3.8+
- Google Cloud Platform account with BigQuery and Cloud Storage enabled
- Service account key with appropriate permissions
- Bruin CLI installed (`pip install bruin`)

## Setup

1. **Clone and navigate to the repository**:
   ```bash
   cd /path/to/berlin-mobility-data-pipeline
   ```

2. **Set up virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r pipeline/assets/ingestion/requirements.txt
   pip install bruin
   ```

4. **Configure Google Cloud credentials**:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/service_account_key.json"
   ```
   or you can also specify the path to your credential file in the 

5. **Verify Bruin installation**:
   ```bash
   bruin --version
   ```

## Configuration

The pipeline is configured via `pipeline/pipeline.yml`:

- **Connections**: GCS and BigQuery connections with project/bucket/dataset details
- **Assets**: Python ingestion scripts and SQL staging transformations
- **Dependencies**: Asset execution order (ingest → load → stage)

## Running the Pipeline

The pipeline is configured for **daily batch processing** with incremental SQL transformations. From the repository root:

```bash
# Validate pipeline structure
bruin validate

# View pipeline lineage
bruin lineage pipeline/assets/staging/stg_bike_trips.sql

# Run full pipeline refresh (initial load or complete reprocessing)
bruin run --full-refresh

# Run incremental batch for current date (daily scheduled execution)
bruin run

# Run batch for specific date range
bruin run --start-date 2011-01-01 --end-date 2011-01-02

# Run specific assets in batch mode
bruin run --full-refresh ingest_data
bruin run --full-refresh load_to_bigquery
bruin run stg_bike_trips  # Incremental for current batch
```

## Pipeline Assets

### Python Assets

- **`ingest_data`**: Uploads `archive/day.csv` and `archive/hour.csv` to `gs://bucket-data-engineering-2026/raw/`
- **`load_to_bigquery`**: Loads GCS CSV files into BigQuery tables `day` and `hour` in dataset `sharing_bike_mobility`

### SQL Assets

- **`stg_bike_trips`**: Incremental staging transformation that:
  - Unions day and hour tables for the current batch date range
  - Maps numeric codes to human-readable values (seasons, weekdays, weather, etc.)
  - Adds descriptive columns like `season_name`, `month_name`, `weekday_name`
- **`bike_trips_seasonal`**: Incremental report aggregating bike rentals by season with weather metrics
- **`bike_trips_temporal`**: Incremental monthly bike rental trends for 2011-2012 with weather averages
- **`bike_trips_weather_impact`**: Incremental analysis of weather impact on rental patterns

## Data Schema

Based on the bike sharing dataset from `archive/Readme.txt`:

- **Raw tables**: `day` (daily aggregates) and `hour` (hourly aggregates)
- **Staging table**: `stg_bike_trips` with readable mappings
- **Key transformations**:
  - `yr`: 0 → 2011, 1 → 2012
  - `season`: 1 → 'Spring', 2 → 'Summer', etc.
  - `weekday`: 0 → 'Sunday', 1 → 'Monday', etc.
  - `weathersit`: Mapped to descriptive weather conditions

## Output

After successful execution:

- GCS bucket contains raw CSV files in `raw/` prefix
- BigQuery dataset `sharing_bike_mobility` contains tables:
  - `day`: Daily bike sharing counts
  - `hour`: Hourly bike sharing counts
  - `stg_bike_trips`: Human-readable staging table

## Troubleshooting

- Ensure GCP credentials are correctly set and have BigQuery/Storage permissions
- Check that local `archive/` files exist before running
- Use `bruin validate` to check pipeline configuration
- View logs with `bruin run --verbose`

## Reports & Visualizations

### Looker Studio Dashboard

**Link**: [Berlin Mobility Data Pipeline Report](https://lookerstudio.google.com/reporting/07dca53f-9446-40e1-acc3-fdaeaf289ede)

**Access**: Viewer mode for peer review

**Visualizations**:
- Bike trips by season (2011-2012)
- Bike demand trends over time (2011-2012)
- Weather impact on trip volumes

The dashboard connects directly to the BigQuery tables powering the pipeline, providing real-time visibility into:
- Seasonal rental patterns
- Temporal demand trends (monthly)
- Weather conditions' impact on bike sharing usage

