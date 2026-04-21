from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
#------------------- Avec Airflow 3.2---------------------------------#
# from airflow.sdk import dag
# from airflow.sdk.base.operator import chain

from tasks import Extract, Load
# Variables 
# nom_projet = "NYW-ETL"
default_args = {
    'owner': 'Moussa SISSOKO',
    'depends_on_past': False,
    #"email": ["sissokomoussa611@gmail.com"],
    #'email_on_failure': True,
    #'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_yellow_trips_etl",
    default_args=default_args,
    schedule="0 23 * * 5",  # tous les vendredis à 23h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="ETL New York Yellow Taxi",
    tags=["nyc_taxi", "etl"]
) as dag:

    chain(
        Extract(),
        #Transform(),
        Load()
    )
