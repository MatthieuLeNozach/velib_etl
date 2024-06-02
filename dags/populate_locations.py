import os
import requests
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Connection
from airflow.utils.dates import days_ago
from airflow import settings
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Create a new connection object using environment variables
conn = Connection(
    conn_id="velib_postgres_connection",
    conn_type="postgres",
    host=os.getenv("VELIB_POSTGRES_HOST"),
    schema=os.getenv("VELIB_POSTGRES_DB"),
    login=os.getenv("VELIB_POSTGRES_USER"),
    password=os.getenv("VELIB_POSTGRES_PASSWORD"),
    port=int(os.getenv("VELIB_POSTGRES_PORT") or "0"),
)

# Add the connection to Airflow's session
session = settings.Session()
if not session.query(Connection).filter(Connection.conn_id == 'velib_postgres_connection').first():
    session.add(conn)
    session.commit()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_postgres_connection(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='velib_postgres_connection')
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
            "stationcode": fields.get("stationcode", ""),
            "name": fields.get("name", ""),
            "latitude": fields.get("coordonnees_geo", [])[0] if fields.get("coordonnees_geo") else None,
            "longitude": fields.get("coordonnees_geo", [])[1] if fields.get("coordonnees_geo") else None,
            "nom_arrondissement_communes": fields.get("nom_arrondissement_communes", "")
        })
    logger.info("Data processing completed. Processed %d records.", len(processed_data))
    ti.xcom_push(key='processed_data', value=processed_data)

def insert_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(key='processed_data', task_ids='process_data')
    pg_hook = PostgresHook(postgres_conn_id='velib_postgres_connection')
    insert_query = """
    INSERT INTO locations (stationcode, name, latitude, longitude, nom_arrondissement_communes)
    VALUES (%(stationcode)s, %(name)s, %(latitude)s, %(longitude)s, %(nom_arrondissement_communes)s)
    ON CONFLICT (stationcode) DO NOTHING;
    """
    for record in processed_data:
        pg_hook.run(insert_query, parameters=record)

def check_row_count(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='velib_postgres_connection')
    row_count = pg_hook.get_first("SELECT COUNT(*) FROM locations")[0]
    if row_count >= 1460:
        logger.info("Row count check passed. Total rows: %d", row_count)
    else:
        raise ValueError(f"Row count check failed. Expected at least 1460 rows, but found {row_count} rows.")

with DAG('populate_locations', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
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

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data_op,
        provide_context=True,
    )

    insert_data_task = PythonOperator(
        task_id='insert_data_to_postgres',
        python_callable=insert_data_to_postgres,
        provide_context=True,
    )

    check_row_count_task = PythonOperator(
        task_id='check_row_count',
        python_callable=check_row_count,
        provide_context=True,
    )

    check_postgres_task >> fetch_data_task >> process_data_task >> insert_data_task >> check_row_count_task