# Bike Sharing Mobility Data Pipeline

A [Bruin-based](https://getbruin.com/) data pipeline for processing bike sharing data from local CSV archive → Google Cloud Storage → BigQuery → staging transformations.

## Overview

This pipeline demonstrates a complete **batch-processing data engineering workflow**:

1. **Ingestion**: Upload local CSV files (`day.csv`, `hour.csv`) from the `archive/` folder to Google Cloud Storage
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
- Service account key with the following IAM roles:
  - `roles/storage.objectCreator` - Create objects in GCS
  - `roles/storage.objectViewer` - Read objects from GCS
  - `roles/bigquery.dataEditor` - Create/edit BigQuery datasets and tables
  - `roles/bigquery.jobUser` - Run BigQuery jobs (queries, loads, exports)
- uv (pip install uv)

## Setup

1. **Clone and navigate to the repository**:
   ```bash
   cd /path/to/bike-sharing-mobility-data-pipeline
   ```

2. **Set up virtual environment with uv**:
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   uv pip install -r pipeline/assets/requirements.txt
   uv pip install bruin
   ```

4. **Verify Bruin installation**:
   ```bash
   bruin --version
   ```

5. **Ensure local CSV files exist**:
   - Place `day.csv` and `hour.csv` in the repository `archive/` folder
   - Confirm the files are present before running the pipeline

6. **Download Google Cloud credentials**:
   - In the service accounts list, click on the service account you created, go to the **Keys** tab
   - Click **Add Key** → **Create new key**
   - Choose **JSON** format
   - Download the JSON file and save it securely (e.g., in your project root)
   
   **Make sure that this credential file is not exposed!** Add it to `.gitignore`

## Configuration

The pipeline is configured via `pipeline/pipeline.yml`:

- **Connections**: GCS and BigQuery connections with project/bucket/dataset details
- **Assets**: Python ingestion scripts and SQL staging transformations
- **Dependencies**: Asset execution order (ingest → load → stage → transformation → report)

## Running the Pipeline

The pipeline is configured for **daily batch processing** with incremental SQL transformations. 

Create a `.bruin.yml` file under the root folder with your GCP credentials. 

```yaml
default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: bigquery-default
          project_id: your_project_id
          location: your_location
          service_account_file: $GOOGLE_APPLICATION_CREDENTIALS
      gcs:
        - name: gcs-default
          service_account_file: $GOOGLE_APPLICATION_CREDENTIALS
```

Then set the environment variable before running:
```bash
# Google Credential, if you choose the Option 2 in your .bruin.yml file
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account.json"
# GCP Project Configuration
export PROJECT_ID="your-project-id"
export DATASET_ID="your-dataset-name"
export BUCKET_NAME="your-gcs-bucket-name"
export RAW_GCS_PREFIX="raw/"  # GCS prefix for raw data files (e.g., "raw/", "raw/bike-sharing/")
```

Adapting the project_id and dataset_name in **all sql file**s according to your own project.

Assuming you are now in the /pipeline folder:

```bash
# Validate pipeline structure
bruin validate

# View pipeline lineage, e.g.,
bruin lineage pipeline/assets/staging/stg_bike_trips.sql

# Run batch for full range data from 2011 to 2012
bruin run

# You could also run batch for specific date range
bruin run --start-date 2011-01-01 --end-date 2011-01-02

```
Note that the report is genearted by using all the data in the two years 2011 and 2012.

## Pipeline Assets

### Python Assets

- **`ingest_data`**: Uploads `archive/day.csv` and `archive/hour.csv` to `gs://{your_bucket}/raw/`
- **`load_to_bigquery`**: Loads GCS CSV files into BigQuery tables `day` and `hour` in dataset `{your dataset}`

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

## Troubleshooting

- Ensure GCP credentials are correctly set and have BigQuery/Storage permissions
- Ensure that all environmental variables are set correctly, the project_id and dataset name are the same as your specificaiton
- Check that local `archive/` files exist before running
- Use `bruin validate` to check pipeline configuration
- View logs with `bruin run --verbose`

## Reports & Visualizations

> [Bike Sharing Reports PDF](bike_sharing_reports.pdf)

note that the axis index for time is following the local language in the Looker Studio, therefore it's shown in my system language in the report.

**Visualizations**:
- Bike trips by season (2011-2012)
- Bike demand trends over time (2011-2012)
- Weather impact on trip volumes

The dashboard connects directly to the BigQuery tables powering the pipeline, providing real-time visibility into:
- Seasonal rental patterns
- Temporal demand trends (monthly)
- Weather conditions' impact on bike sharing usage

