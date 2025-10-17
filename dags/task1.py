import pandas as pd
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
import pendulum
import io
import logging


# Default arguments for the DAG
default_args = {
   'owner': 'airflow',
   'start_date': pendulum.datetime(2025, 1, 1, tz="UTC")
,
   'retries': 1,
}


# Define the DAG
dag = DAG(
   's3_read_daily_files',
   default_args=default_args,
   description='DAG to read daily files',
   schedule=None, # Set to None for manual execution
)


# AWS S3 connection details
BUCKET_NAME = 'airflowbuckets25'
OBJECT_KEY1 = 'daily_files/transaction_20251017_part001.csv'
OBJECT_KEY2 = 'daily_files/transaction_20251017_part002.csv'
#LOCAL_FILE_PATH = '/usr/local/airflow/include/'

wait_for_file1 = S3KeySensor(
   task_id='wait_for_s3_file1',
   bucket_name=BUCKET_NAME,
   bucket_key=OBJECT_KEY1,
   aws_conn_id='aws_s3', # Use your AWS connection ID
   poke_interval=60, # How often to check the S3 bucket (in seconds)
   timeout=600, # How long to wait before giving up (in seconds)
   mode='poke', # This mode will keep checking until the file is found
   dag=dag,
)

wait_for_file2 = S3KeySensor(
   task_id='wait_for_s3_file2',
   bucket_name=BUCKET_NAME,
   bucket_key=OBJECT_KEY2,
   aws_conn_id='aws_s3', # Use your AWS connection ID
   poke_interval=60, # How often to check the S3 bucket (in seconds)
   timeout=600, # How long to wait before giving up (in seconds)
   mode='poke', # This mode will keep checking until the file is found
   dag=dag,
)

def create_success_file():
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    BUCKET_NAME = 'airflowbuckets25'
    success_file_key = 'SUCCESS_20251017'
    s3_hook.load_string(
        string_data='SUCCESS',
        key=success_file_key,
        BUCKET_NAME=BUCKET_NAME,
        replace=True,
    )
    logging.info(f"Created {success_file_key} in bucket {BUCKET_NAME}")
create_success_task = PythonOperator(
    task_id='create_success_file',
    python_callable=create_success_file,
)
[wait_for_file1, wait_for_file1] >> create_success_task