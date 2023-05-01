"""
### Toy DAG showing the new max_active_tis_per_dag parameter.

This parameter was added in Airflow 2.6 and limits the number 
of mapped task instances within one Dagrun.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import time


@dag(
    start_date=datetime(2023, 4, 18),
    schedule=None,
    catchup=False,
    tags=["toy", "Pandas"],
)
def max_active_tis_per_dagrun():
    @task(max_active_tis_per_dagrun=2, max_active_tis_per_dag=5)
    def return_n(n):
        time.sleep(5)
        return n

    return_n.expand(n=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])


max_active_tis_per_dagrun()
