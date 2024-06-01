from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.transformers.velib_api.populate_stations import populate_stations
from utils.data_loaders.fetch_data import load_data_from_api
from utils.transformers.velib_api.process_data import process_station_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_data(**kwargs):
    return load_data_from_api()

def process_station_data_op(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data')
    return process_station_data(data)

def populate_stations_op(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='process_station_data')
    return populate_stations(data)

with DAG(
    'populate_stations', 
    default_args=default_args, 
    schedule_interval=timedelta(seconds=120)
) as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        provide_context=True,
    )

    process_station_data_task = PythonOperator(
        task_id='process_station_data',
        python_callable=process_station_data_op,
        provide_context=True,
    )

    populate_stations_task = PythonOperator(
        task_id='populate_stations',
        python_callable=populate_stations_op,
        provide_context=True,
    )

    fetch_data_task >> process_station_data_task >> populate_stations_task