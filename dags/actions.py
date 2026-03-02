import requests
import datetime
import os
import io
import time
import logging
from datetime import timezone

import google.cloud.storage as gcs

# Config
PROJECT_ID = "smart-exchange-489005-v6"
BUCKET_NAME = f"{PROJECT_ID}-bucket"
GCS_FOLDER = "raw/"
GCS_LOG_FOLDER = "gcs/logs/"

# Logs en mémoire
log_stream = io.StringIO()
logging.basicConfig(
    stream=log_stream,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_storage_client():
    """Initialise le client GCS."""
    return gcs.Client(project=PROJECT_ID)


def file_exists_in_gcs(bucket_name: str, gcs_path: str) -> bool:
    """Vérifie si un fichier existe déjà dans GCS."""
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    return blob.exists()


def upload_to_gcs(bucket_name: str, gcs_path: str, content: bytes) -> None:
    """Upload un fichier binaire vers GCS."""
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(content, content_type="application/octet-stream")
    logging.info(f"Uploaded to gs://{bucket_name}/{gcs_path}")


def upload_log_to_gcs() -> None:
    """Upload les logs vers GCS."""
    timestamp = datetime.datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    log_filename = f"{GCS_LOG_FOLDER}extract_log_{timestamp}.log"
    client = get_storage_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(log_filename)
    blob.upload_from_string(log_stream.getvalue())
    logging.info(f"Log uploaded to {log_filename}")


def download_histo_data() -> None:
    """
    Télécharge les fichiers Parquet des taxis jaunes NYC
    de 2024 à l'année courante et les envoie dans GCS.
    """
    current_year = datetime.datetime.now(timezone.utc).year

    try:
        for year in range(2024, current_year + 1):
            for month in range(1, 13):
                file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
                gcs_path = f"{GCS_FOLDER}{file_name}"
                download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

                if file_exists_in_gcs(BUCKET_NAME, gcs_path):
                    logging.info(f"{file_name} already exists, skipping...")
                    continue

                try:
                    logging.info(f"Downloading {file_name}...")
                    response = requests.get(download_url, stream=True, timeout=30)

                    if response.status_code == 200:
                        upload_to_gcs(BUCKET_NAME, gcs_path, response.content)
                    elif response.status_code == 404:
                        logging.warning(f"{file_name} not found on source, skipping...")
                    else:
                        logging.error(f"Failed to download {file_name}. HTTP {response.status_code}")

                except requests.exceptions.RequestException as e:
                    logging.error(f"Request error for {file_name}: {str(e)}")

                time.sleep(1)

        logging.info("Download and upload to GCS completed!")

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

    finally:
        upload_log_to_gcs()


if __name__ == '__main__':
    logging.info(f"Start historical data download: {datetime.datetime.now(timezone.utc)}")
    download_histo_data()
