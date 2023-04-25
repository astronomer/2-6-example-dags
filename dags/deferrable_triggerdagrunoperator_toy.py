"""
Toy example of a deferrable TriggerDagRunOperator. 
"""

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
import time


@dag(
    start_date=datetime(2023, 4, 18),
    schedule=None,
    catchup=False,
    tags=["toy", "TriggerDagRunOperator", "triggerer logs"],
)
def deferrable_triggerdagrunoperator_toy():
    @task
    def wait_10_seconds():
        time.sleep(10)

    trigger_dagrun = TriggerDagRunOperator(
        task_id="trigger_dagrun",
        trigger_dag_id="downstream_trigger_dagrun",
        conf={"sleep_time": 23},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=5,
    )

    wait_10_seconds() >> trigger_dagrun >> wait_10_seconds()


deferrable_triggerdagrunoperator_toy()
