"""
Toy example of different types of callback parameters with functions and a Notifier.
Notifiers were added in Airflow 2.6.
"""

from airflow.decorators import dag, task
from pendulum import datetime, duration
from airflow.notifications.basenotifier import BaseNotifier

class MyNotifier(BaseNotifier):
    """
    Basic notifier, says hi.
    """

    def __init__(self):
        pass

    def notify(self, context):
        t_id = context["ti"].task_id
        t_state = context["ti"].state
        print(f"Hi from MyNotifier! {t_id} finished as: {t_state}")


def my_callback_function(context):
    t_id = context["ti"].task_id
    t_state = context["ti"].state
    print(f"Hi from my_callback_function! {t_id} finished as: {t_state}")


def success_function():
    return 10


def failure_function():
    return 10 / 0


@dag(
    start_date=datetime(2023, 4, 18),
    schedule=None,
    catchup=False,
    # DAG level callbacks depend on events happening to the DAG itself
    on_success_callback=[my_callback_function, MyNotifier()],
    on_failure_callback=[my_callback_function, MyNotifier()],
    sla_miss_callback=[my_callback_function, MyNotifier()],
    # callbacks provided in the default_args are giving to all tasks in the DAG
    default_args={
        "on_execute_callback": [my_callback_function, MyNotifier()],
        "on_retry_callback": [my_callback_function, MyNotifier()],
        "on_success_callback": [my_callback_function, MyNotifier()],
        "on_failure_callback": [my_callback_function, MyNotifier()],
        "retries": 2,
        "retry_delay": duration(seconds=5)
    },
    tags=["toy"],
)
def callbacks_overview():
    @task(
        # you can override default_args on the task level
        on_execute_callback=[my_callback_function, MyNotifier()],
        on_retry_callback=[my_callback_function, MyNotifier()],
        on_success_callback=[my_callback_function, MyNotifier()],
        on_failure_callback=[my_callback_function, MyNotifier()],
    )
    def task_succeeding_task_level_callback():
        return 10

    @task(
        # you can override default_args on the task level
        on_execute_callback=[my_callback_function, MyNotifier()],
        on_retry_callback=[my_callback_function, MyNotifier()],
        on_success_callback=[my_callback_function, MyNotifier()],
        on_failure_callback=[my_callback_function, MyNotifier()],
    )
    def task_failing_task_level_callback():
        return 10 / 0

    @task()
    def task_succeeding():
        return 10

    @task()
    def task_failing():
        return 10 / 0

    task_succeeding()
    task_failing()
    task_succeeding_task_level_callback()
    task_failing_task_level_callback()


callbacks_overview()
