from airflow.decorators import task_group          # ✅ Airflow 2
from airflow.operators.python import PythonOperator 

from process import process_nyc_yellow_taxi
from load import load_to_bigquery
from actions import download_histo_data


@task_group
def Extract():
    PythonOperator(
        task_id="download_data",
        python_callable=download_histo_data,
    )


@task_group
def Load():
    def task_load(**context):
        execution_date = context["execution_date"]
        month = execution_date.strftime("%Y-%m")
        load_to_bigquery(month=month)

    PythonOperator(
        task_id="load_data",
        python_callable=task_load,
    )
