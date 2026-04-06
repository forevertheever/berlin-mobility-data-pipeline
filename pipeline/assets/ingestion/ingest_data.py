from google.cloud import storage
from pathlib import Path

# set your bucket from the pipeline config or env
BUCKET_NAME = "bucket-data-engineering-2026"

# File names in project archive folder
ARCHIVE_FILES = ["day.csv", "hour.csv"]


def upload_to_gcs(client: storage.Client, local_file: Path, destination_dir: str = "raw"):
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{destination_dir}/{local_file.name}")
    blob.upload_from_filename(str(local_file))
    print(f"Uploaded {local_file} -> gs://{BUCKET_NAME}/{destination_dir}/{local_file.name}")


def main():
    # Auth via GOOGLE_APPLICATION_CREDENTIALS should be set in environment
    # e.g. export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
    project_root = Path(__file__).resolve().parents[3]
    archive_dir = project_root / "archive"

    client = storage.Client()

    for filename in ARCHIVE_FILES:
        local_file = archive_dir / filename
        if not local_file.exists():
            raise FileNotFoundError(f"Local archive file missing: {local_file}")
        upload_to_gcs(client, local_file)


if __name__ == "__main__":
    main()