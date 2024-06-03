from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Using the connection ID defined in environment variables
pg_hook = PostgresHook(postgres_conn_id='velib_postgres')

def check_postgres_connection(**kwargs):
    pg_hook.get_conn()
    logger.info("PostgreSQL connection check passed.")

def dump_database():
    pg_hook = PostgresHook(postgres_conn_id='velib_postgres')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    with open('/tmp/backup.sql', 'w') as f:
        pg_cursor.copy_expert("COPY (SELECT * FROM stations) TO STDOUT WITH CSV HEADER", f)



def upload_to_minio():
    s3_hook = S3Hook(aws_conn_id='velib_s3')
    s3_hook.load_file(
        filename='/tmp/backup.sql',
        key='backups/backup.sql',
        bucket_name='mediae',
        replace=True
    )
    logger.info("File uploaded to MinIO.")

with DAG(
    'backup_stations_table_to_s3',
    default_args=default_args,
    description='A simple DAG to backup the database and store it in MinIO',
    schedule_interval=timedelta(hours=6),
    start_date=days_ago(1),
    tags=['backup'],
    catchup=False
) as dag:

    check_postgres_task = PythonOperator(
        task_id='check_postgres_connection',
        python_callable=check_postgres_connection,
        provide_context=True,
    )

    dump_database_task = PythonOperator(
        task_id='dump_database',
        python_callable=dump_database,
        provide_context=True,
    )

    upload_to_minio_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
    )

    check_postgres_task >> dump_database_task >> upload_to_minio_task