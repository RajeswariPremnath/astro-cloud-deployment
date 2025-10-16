import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# 1. Define the DAG's arguments (settings)
with DAG(
    dag_id="simple_print_dag_schedule",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule='0 13,14,18,19 * * *',
    catchup=False,
    tags=["example", "basic"],
) as dag:
    
    # 2. Define the Task
    print_message_task = BashOperator(
        task_id="print_a_message",
        bash_command='echo "Hello from my first Airflow DAG!"',
    )