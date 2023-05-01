"""
### DAG that waits X seconds depending on conf

Downstream DAG for the toy example of a deferrable TriggerDagRunOperator. 
"""

from airflow.decorators import dag, task
from pendulum import datetime
import time


@dag(
    start_date=datetime(2023, 4, 18),
    schedule=None,
    catchup=False,
    tags=["toy", "TriggerDagRunOperator", "trigger logs"],
    params={"sleep_time": 5},
)
def downstream_trigger_dagrun():
    @task
    def wait_X_seconds(**context):
        sleep_time = context["params"]["sleep_time"]
        print(f"I will now sleep for {sleep_time} seconds, good night!")
        time.sleep(sleep_time)

    wait_X_seconds()


downstream_trigger_dagrun()
