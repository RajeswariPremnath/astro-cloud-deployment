import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator


default_args = {
'owner': 'airflow',
'retries': 1,
'retry_delay': timedelta(minutes=5),
}

with DAG(
'git_deployment_example',
default_args=default_args,
description='A simple DAG deployed via GitHub integration',
schedule='@daily',
start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
catchup=False,
) as dag:

    hello_task = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello from Astro Cloud deployment!"',
    )

    goodbye_task = BashOperator(

    task_id='print_goodbye',
    bash_command='echo "Goodbye from Astro Cloud deployment!"',
    )

hello_task >> goodbye_task