from __future__ import annotations
import pendulum
from airflow.decorators import dag, task

@dag(schedule=None,start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),catchup=False)
def DAG1():
	
    @task
    def task1():
        print("Hey , I am in Task 1 now using Task flow API")


    @task
    def task2():
        print("Hey , I am in Task 2 now using Task flow API")


    task1()
    task2()

DAG1()