from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.database.database import create_tables
from utils.velib_api.check_station_codes_count import check_station_codes_count

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_tables_op(**kwargs):
    create_tables()

def check_station_codes_count_op(**kwargs):
    check_station_codes_count()

with DAG('initialize_tables', default_args=default_args, schedule_interval=None) as dag:
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables_op,
        provide_context=True,
    )

    check_station_codes_count_task = PythonOperator(
        task_id='check_station_codes_count',
        python_callable=check_station_codes_count_op,
        provide_context=True,
    )

    create_tables_task >> check_station_codes_count_task