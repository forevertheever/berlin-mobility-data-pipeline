"""@bruin
name: ingest_data
type: python
image: python:3.11

@bruin"""

from google.cloud import storage
from pathlib import Path
import os

# Get variables from Bruin environment
BUCKET_NAME = os.getenv("BRUIN_VAR_BUCKET_NAME", "bucket-date-engineering-2026")

# File names in project archive folder
ARCHIVE_FILES = ["day.csv", "hour.csv"]


def upload_to_gcs(client: storage.Client, local_file: Path, destination_dir: str = "raw"):
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{destination_dir}/{local_file.name}")
    blob.upload_from_filename(str(local_file))
    print(f"✅ Uploaded {local_file} -> gs://{BUCKET_NAME}/{destination_dir}/{local_file.name}")
    # Verify upload
    if blob.exists():
        print(f"✅ Verified: {blob.name} exists in bucket")
    else:
        print(f"❌ Error: {blob.name} not found after upload")


def materialize():
    # Auth via GOOGLE_APPLICATION_CREDENTIALS should be set in environment
    # e.g. export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
    project_root = Path(__file__).resolve().parents[3]
    archive_dir = project_root / "archive"

    print(f"Using bucket: {BUCKET_NAME}")
    print(f"Archive dir: {archive_dir}")
    print(f"Files to upload: {ARCHIVE_FILES}")

    client = storage.Client()
    print(f"Authenticated as: {client.project}")

    # Check if bucket exists
    bucket = client.bucket(BUCKET_NAME)
    if not bucket.exists():
        print(f"❌ Bucket {BUCKET_NAME} does not exist!")
        raise Exception(f"Bucket {BUCKET_NAME} not found")

    for filename in ARCHIVE_FILES:
        local_file = archive_dir / filename
        print(f"Checking file: {local_file}")
        if not local_file.exists():
            raise FileNotFoundError(f"Local archive file missing: {local_file}")
        upload_to_gcs(client, local_file)


# Invoke materialize when run by Bruin
materialize()
