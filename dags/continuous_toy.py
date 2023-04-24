"""
Toy example showing a simple implementation of the ContinuousTimetable.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import time
import random


@dag(
    start_date=datetime(2023, 4, 18),
    schedule="@continuous",
    max_active_runs=1,
    catchup=False,
    tags=["toy", "ContinuousTimetable"],
)
def continuous_toy():
    @task
    def sleep_randomly():
        time.sleep(random.randint(5, 20))
        return "Good morning!"

    sleep_randomly()


continuous_toy()
