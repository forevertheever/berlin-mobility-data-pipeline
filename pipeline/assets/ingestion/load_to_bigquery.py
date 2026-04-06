from pathlib import Path
from google.cloud import bigquery

# Keep these aligned with pipeline/pipeline.yml
PROJECT_ID = "data-engineering-2026-484614"
DATASET_ID = "sharing_bike_mobility"
BUCKET_NAME = "bucket-data-engineering-2026"
RAW_GCS_PREFIX = "raw"

ARCHIVE_FILES = ["day.csv", "hour.csv"]

# Schema definitions from archive/Readme.txt
BASE_SCHEMA = [
    bigquery.SchemaField("instant", "INTEGER"),
    bigquery.SchemaField("dteday", "DATE"),
    bigquery.SchemaField("season", "INTEGER"),
    bigquery.SchemaField("yr", "INTEGER"),
    bigquery.SchemaField("mnth", "INTEGER"),
    bigquery.SchemaField("holiday", "INTEGER"),
    bigquery.SchemaField("weekday", "INTEGER"),
    bigquery.SchemaField("workingday", "INTEGER"),
    bigquery.SchemaField("weathersit", "INTEGER"),
    bigquery.SchemaField("temp", "FLOAT"),
    bigquery.SchemaField("atemp", "FLOAT"),
    bigquery.SchemaField("hum", "FLOAT"),
    bigquery.SchemaField("windspeed", "FLOAT"),
    bigquery.SchemaField("casual", "INTEGER"),
    bigquery.SchemaField("registered", "INTEGER"),
    bigquery.SchemaField("cnt", "INTEGER"),
]

SCHEMA_BY_TABLE = {
    "day": BASE_SCHEMA,
    "hour": [
        bigquery.SchemaField("hr", "INTEGER"),
        *BASE_SCHEMA,
    ],
}


def ensure_dataset(client: bigquery.Client, dataset_id: str):
    dataset_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset exists: {dataset_ref}")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset: {dataset_ref}")


def load_csv_to_bq(client: bigquery.Client, table_id: str, gcs_uri: str, schema: list[bigquery.SchemaField]):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        field_delimiter=",",
        autodetect=False,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
    )

    print(f"Starting load: {gcs_uri} -> {table_id}")
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()

    destination = client.get_table(table_id)
    print(f"Loaded {destination.num_rows} rows into {table_id}")


def main():
    client = bigquery.Client(project=PROJECT_ID)
    ensure_dataset(client, DATASET_ID)

    for filename in ARCHIVE_FILES:
        table_name = filename.replace(".csv", "")
        gcs_uri = f"gs://{BUCKET_NAME}/{RAW_GCS_PREFIX}/{filename}"
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        schema = SCHEMA_BY_TABLE.get(table_name, BASE_SCHEMA)

        load_csv_to_bq(client, table_id, gcs_uri, schema)


if __name__ == "__main__":
    main()
