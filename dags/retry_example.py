"""
### Toy DAG showing different configurations of retry parameters

Note that you can see the behavior of this DAG in case of task success by 
setting the param 'valid_bash_command' to any valid bash command.
"""

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration


@dag(
    start_date=datetime(2023, 4, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": duration(hours=2),
        # As of Airflow 2.6 maximum retry task delay is set to be 24h (86400s) by default.
        # You can change it globally via core.max_task_retry_delay config.
    },
    params={
        "valid_bash_command": False
    },  # set to any valid bash command to make tasks succeed
)
def retry_example():
    t1 = BashOperator(
        task_id="t1",
        bash_command="echo I get 3 retries! && {{ params.valid_bash_command }}",
    )

    t2 = BashOperator(
        task_id="t2",
        bash_command="echo I get 6 retries and never wait long! && {{ params.valid_bash_command }}",
        retries=6,
        max_retry_delay=duration(seconds=10),
    )

    t3 = BashOperator(
        task_id="t3",
        bash_command="echo I wait exactly 20 seconds between each of my 4 retries! && {{ params.valid_bash_command }}",
        retries=4,
        retry_delay=duration(seconds=20),
        retry_exponential_backoff=False,
    )

    t4 = BashOperator(
        task_id="t4",
        bash_command="echo I have to get it right the first time! && {{ params.valid_bash_command }}",
        retries=0,
    )


retry_example()
