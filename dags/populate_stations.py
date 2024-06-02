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
            "is_returning": fields.get("is_returning", ""),
            "name": fields.get("name", ""),
            "latitude": fields.get("coordonnees_geo", [])[0] if fields.get("coordonnees_geo") else None,
            "longitude": fields.get("coordonnees_geo", [])[1] if fields.get("coordonnees_geo") else None,
            "nom_arrondissement_communes": fields.get("nom_arrondissement_communes", "")
        })
    logger.info("Data processing completed. Processed %d records.", len(processed_data))
    ti.xcom_push(key='processed_data', value=processed_data)

def populate_stations_op(**kwargs):
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(key='processed_data', task_ids='process_station_data')
    pg_hook = PostgresHook(postgres_conn_id='velib_postgres_connection')
    insert_query = """
    INSERT INTO stations (record_timestamp, stationcode, ebike, mechanical, duedate, numbikesavailable, numdocksavailable, capacity, is_renting, is_installed, is_returning, name, latitude, longitude, nom_arrondissement_communes)
    VALUES (%(record_timestamp)s, %(stationcode)s, %(ebike)s, %(mechanical)s, %(duedate)s, %(numbikesavailable)s, %(numdocksavailable)s, %(capacity)s, %(is_renting)s, %(is_installed)s, %(is_returning)s, %(name)s, %(latitude)s, %(longitude)s, %(nom_arrondissement_communes)s)
    ON CONFLICT (record_timestamp) DO UPDATE
    SET stationcode = EXCLUDED.stationcode,
        ebike = EXCLUDED.ebike,
        mechanical = EXCLUDED.mechanical,
        duedate = EXCLUDED.duedate,
        numbikesavailable = EXCLUDED.numbikesavailable,
        numdocksavailable = EXCLUDED.numdocksavailable,
        capacity = EXCLUDED.capacity,
        is_renting = EXCLUDED.is_renting,
        is_installed = EXCLUDED.is_installed,
        is_returning = EXCLUDED.is_returning,
        name = EXCLUDED.name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        nom_arrondissement_communes = EXCLUDED.nom_arrondissement_communes;
    """
    for record in processed_data:
        pg_hook.run(insert_query, parameters=record)

def check_data_added(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='api_data', task_ids='fetch_data')
    latest_timestamp = raw_data["records"][0]["record_timestamp"]

    pg_hook = PostgresHook(postgres_conn_id='velib_postgres_connection')
    last_row = pg_hook.get_first("SELECT record_timestamp FROM stations ORDER BY record_timestamp DESC LIMIT 1")

    if last_row and last_row[0] == latest_timestamp:
        logger.info("Data has been successfully added to the database. Latest timestamp: %s", latest_timestamp)
    else:
        raise ValueError("Timestamp mismatch: fetched timestamp does not match the last inserted row timestamp.")

with DAG(
    'populate_stations', 
    default_args=default_args, 
    schedule_interval=timedelta(seconds=120)
) as dag:
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

    populate_stations_task = PythonOperator(
        task_id='populate_stations',
        python_callable=populate_stations_op,
        provide_context=True,
    )

    check_data_added_task = PythonOperator(
        task_id='check_data_added',
        python_callable=check_data_added,
        provide_context=True,
    )

    fetch_data_task >> process_station_data_task >> populate_stations_task >> check_data_added_task