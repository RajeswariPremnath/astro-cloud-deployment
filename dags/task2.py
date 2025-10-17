import pandas as pd
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum
import io
import logging


# Default arguments for the DAG
default_args = {
   'owner': 'airflow',
   'start_date': pendulum.datetime(2025, 1, 1, tz="UTC")
,
   'retries': 1,
   'depends_on_past': True,
}


# Define the DAG
dag = DAG(
   's3_read_success_file',
   default_args=default_args,
   description='DAG to read daily files',
   schedule=None, # Set to None for manual execution
   catchup=False,
   max_active_runs=1,
)


# AWS S3 connection details
BUCKET_NAME = 'airflowbuckets25'
OBJECT_KEY1 = 'daily_files/SUCCESS_20251017'
#LOCAL_FILE_PATH = '/usr/local/airflow/include/'

wait_for_file1 = S3KeySensor(
   task_id='wait_for_success_file',
   bucket_name=BUCKET_NAME,
   bucket_key=OBJECT_KEY1,
   aws_conn_id='aws_s3', # Use your AWS connection ID
   poke_interval=60, # How often to check the S3 bucket (in seconds)
   timeout=600, # How long to wait before giving up (in seconds)
   mode='poke', # This mode will keep checking until the file is found
   dag=dag,
)

def process_data(**kwargs):
    print("processing data for the day")

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    execution_timeout=timedelta(hours=2),
)

wait_for_file1 >> process_data_task