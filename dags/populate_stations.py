import os
import requests
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

pg_hook = PostgresHook(postgres_conn_id='velib_postgres')

def check_postgres_connection(**kwargs):
    pg_hook.get_conn()
    logger.info("PostgreSQL connection check passed.")

def fetch_data_from_api(**kwargs):
    logger.info("Fetching data from API...")
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&timezone=Europe/Paris&rows=2000"
    try:
        response = requests.get(url, headers={"accept": "application/json"})
        response.raise_for_status()
        data = response.json()
        logger.info("Data fetched successfully.")
        kwargs['ti'].xcom_push(key='api_data', value=data)
    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch data from API: %s", e)
        raise

def process_data_op(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='api_data', task_ids='fetch_data')
    records = raw_data.get("records", [])
    processed_data = []
    for record in records:
        fields = record.get("fields", {})
        processed_data.append({
            "record_timestamp": record.get("record_timestamp", ""),
            "stationcode": fields.get("stationcode", ""),
            "ebike": fields.get("ebike", 0),
            "mechanical": fields.get("mechanical", 0),
            "duedate": fields.get("duedate", ""),
            "numbikesavailable": fields.get("numbikesavailable", 0),
            "numdocksavailable": fields.get("numdocksavailable", 0),
            "capacity": fields.get("capacity", 0),
            "is_renting": fields.get("is_renting", ""),
            "is_installed": fields.get("is_installed", ""),
            "is_returning": fields.get("is_returning", "")
        })
    logger.info("Data processing completed. Processed %d records.", len(processed_data))
    ti.xcom_push(key='processed_data', value=processed_data)

def check_timestamp_exists(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='api_data', task_ids='fetch_data')
    latest_timestamp = raw_data["records"][0]["record_timestamp"]

    result = pg_hook.get_first("SELECT 1 FROM stations WHERE record_timestamp = %s LIMIT 1", parameters=(latest_timestamp,))

    if result:
        logger.error("Timestamp %s already exists in the database. Failing the task.", latest_timestamp)
        raise ValueError(f"Timestamp {latest_timestamp} already exists in the database.")
    else:
        logger.info("Timestamp %s does not exist in the database. Proceeding with data insertion.", latest_timestamp)

def populate_stations_op(**kwargs):
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(key='processed_data', task_ids='process_station_data')
    insert_query = """
    INSERT INTO stations (record_timestamp, stationcode, ebike, mechanical, duedate, numbikesavailable, numdocksavailable, capacity, is_renting, is_installed, is_returning)
    VALUES (%(record_timestamp)s, %(stationcode)s, %(ebike)s, %(mechanical)s, %(duedate)s, %(numbikesavailable)s, %(numdocksavailable)s, %(capacity)s, %(is_renting)s, %(is_installed)s, %(is_returning)s)
    """
    for record in processed_data:
        pg_hook.run(insert_query, parameters=record)

with DAG(
    'populate_stations', 
    default_args=default_args, 
    schedule_interval=timedelta(seconds=120),
    catchup=False
) as dag:
    check_postgres_task = PythonOperator(
        task_id='check_postgres_connection',
        python_callable=check_postgres_connection,
        provide_context=True,
    )

    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_api,
        provide_context=True,
    )

    process_station_data_task = PythonOperator(
        task_id='process_station_data',
        python_callable=process_data_op,
        provide_context=True,
    )

    check_timestamp_exists_task = PythonOperator(
        task_id='check_timestamp_exists',
        python_callable=check_timestamp_exists,
        provide_context=True,
    )

    populate_stations_task = PythonOperator(
        task_id='populate_stations',
        python_callable=populate_stations_op,
        provide_context=True,
    )

    check_postgres_task >> fetch_data_task >> process_station_data_task >> check_timestamp_exists_task >> populate_stations_task