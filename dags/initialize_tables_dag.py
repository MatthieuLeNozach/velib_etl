# dags/initialize_tables_dag.py

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('initialize_tables', default_args=default_args, schedule_interval=None) as dag:
    create_database_task = DockerOperator(
        task_id='create_database',
        image='docker.io/matthieujln/velib_airflow:operator',
        api_version='auto',
        auto_remove=True,
        command='python -c "from utils.database.create import create_database; create_database()"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        volumes=['./src:/app/src']
    )

    create_tables_task = DockerOperator(
        task_id='create_tables',
        image='docker.io/matthieujln/velib_airflow:operator',
        api_version='auto',
        auto_remove=True,
        command='python -c "from utils.database.create import create_tables, create_database; engine = create_database(); create_tables(engine)"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        volumes=['./src:/app/src']
    )

    create_database_task >> create_tables_task