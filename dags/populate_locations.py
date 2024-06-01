from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.transformers.velib_api.populate_locations import populate_locations
from utils.data_loaders.fetch_data import load_data_from_api
from utils.transformers.velib_api.process_data import process_data

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

def process_data_op(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data')
    return process_data(data)

def populate_locations_op(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='process_data')
    return populate_locations(data)

with DAG('populate_locations', default_args=default_args, schedule_interval=None) as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        provide_context=True,
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data_op,
        provide_context=True,
    )

    populate_locations_task = PythonOperator(
        task_id='populate_locations',
        python_callable=populate_locations_op,
        provide_context=True,
    )

    fetch_data_task >> process_data_task >> populate_locations_task