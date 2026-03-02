from google.cloud import bigquery, storage
from process import process_nyc_yellow_taxi
from actions import BUCKET_NAME, PROJECT_ID
import pandas as pd
import io
import time
import logging
from datetime import datetime


def load_to_bigquery(month: str = None) -> None:
    """
    Si month fourni (ex: '2024-01') → charge uniquement ce fichier
    Sinon → charge le mois en cours
    """
    if month is None:
        month = datetime.now().strftime("%Y-%m")

    client_gcs = storage.Client(project=PROJECT_ID)
    client_bq = bigquery.Client(project=PROJECT_ID)
    bucket = client_gcs.bucket(BUCKET_NAME)

    table_id = f"{PROJECT_ID}.raw_yellowtrips.yellow_trips"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    #  Cible uniquement le fichier du mois
    target = f"raw/yellow_tripdata_{month}.parquet"
    blob = bucket.blob(target)

    if not blob.exists():
        print(f" Fichier introuvable : {target}")
        return

    try:
        print(f" Traitement : {target}")
        data = blob.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(data))
        df = process_nyc_yellow_taxi(df)
        print(f" Transformé : {len(df)} lignes")

        chunk_size = 500_000
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            job = client_bq.load_table_from_dataframe(
                chunk, table_id, job_config=job_config
            )
            job.result()

        del df
        print(f" Chargé dans BigQuery !")
        logging.info(f"{target} chargé avec succès")

    except Exception as e:
        print(f" ERREUR : {str(e)}")


if __name__ == "__main__":
    load_to_bigquery()
