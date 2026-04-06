"""End-to-end ingestion from local archive -> GCS -> BigQuery."""
from ingest_data import main as upload_main
from load_to_bigquery import main as load_main


def main():
    print("Step 1/2: Upload local archive files to GCS")
    upload_main()

    print("Step 2/2: Load GCS CSV files into BigQuery")
    load_main()


if __name__ == "__main__":
    main()
