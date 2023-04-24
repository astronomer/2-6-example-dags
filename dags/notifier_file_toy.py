"""
Toy example of using a simple custom notifier that writes files to the include folder.
Two of the 4 tasks in this DAG are set up to fail in order to show on_failure_callback
notifications.
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.notifications.basenotifier import BaseNotifier
import os


class MyFileNotifier(BaseNotifier):
    """Very basic example of a Notifier which writes files to the include folder.
    :param message: The message to be used as the file name.
    """

    template_fields = ("message",)

    def __init__(self, message):
        self.message = message

    def notify(self, context):
        file_name = self.message
        file_path = os.path.join("include", file_name)
        with open(file_path, "w") as file:
            file.write(f"{context['task_instance'].task_id}")


def my_additional_callback_function(context):
    print("hello!")


def success_function():
    return 10


def failure_function():
    return 10 / 0


@dag(
    start_date=datetime(2023, 4, 18),
    schedule=None,
    catchup=False,
    # DAG level callbacks depend on whether the DAG itself is successful or fails
    on_failure_callback=MyFileNotifier(message="Failure_DAG_level_callback"),
    on_success_callback=MyFileNotifier(message="Success_DAG_level_callback."),
    # callbacks provided in the default_args are giving to all tasks in the DAG
    default_args={
        "on_failure_callback": MyFileNotifier(
            message="Failure_default_arg_level_callback"
        ),
        "on_success_callback": [
            MyFileNotifier(message="Success_default_arg_level_callback"),
            my_additional_callback_function,
        ],
    },
    tags=["toy", "Notifier"],
)
def notifier_file_toy_example():
    @task(
        # you can override default_args on the task level
        on_success_callback=MyFileNotifier(message="Success_Task_level_callback"),
    )
    def task_succeeding_task_level_callback():
        return 10

    @task(
        # you can override default_args on the task level
        on_failure_callback=MyFileNotifier(message="Failure_Task_level_callback"),
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


notifier_file_toy_example()
