# berlin-mobility-data-pipeline

## GCS to BigQuery pipeline

This workspace includes an end-to-end pipeline to:
- upload local archived CSV data from `archive/` to GCS
- load CSV files from GCS into BigQuery in dataset `berlin_mobility`

### Setup

1. Set Google credentials:
   - `export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/service_account_key.json"`
2. Install dependencies:
   - `python -m pip install -r pipeline/assets/ingestion/requirements.txt`
3. Configure `pipeline/pipeline.yml` (already has example settings for project/bucket/dataset).

### Run

From repository root:
- `python pipeline/assets/ingestion/run_pipeline.py`

### Files

- `pipeline/assets/ingestion/ingest_data.py`: upload local `archive/day.csv` and `archive/hour.csv` to `gs://bucket-data-engineering-2026/raw/`
- `pipeline/assets/ingestion/load_to_bigquery.py`: load those GCS CSV files into BigQuery tables `day` and `hour`
- `pipeline/assets/ingestion/run_pipeline.py`: orchestrator for both steps
- `pipeline/assets/staging/stg_bike_trips.sql`: staging transformation to make data human-readable (maps codes to names)
