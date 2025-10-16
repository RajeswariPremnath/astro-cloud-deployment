import pendulum
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def my_python_function():
    print("Hello")

def my_python_function1():
    print("Hello World")

with DAG(
    dag_id="two_task_parallel_python_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example", "parallel"],
) as dag:
    
  
    python_task = PythonOperator(
        task_id='run_python_function',
        python_callable=my_python_function,
    )
    python_task1 = PythonOperator(
        task_id='run_python_function_1',
        python_callable=my_python_function1,
    )