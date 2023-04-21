"""
Toy example of using macros to access json and yaml inputs.
"""

from airflow.decorators import dag, task
from pendulum import datetime

my_json = '{"a":"hello", "b":4, "c":true, "d": [ 1, 2, 3, 4, 5 ], "e" : {"e1" : 1, "e2" : "bye"}}'
my_yaml = (
    "a: hello\nb: 4\nc: true\nd:\n  - 1\n  - 2\n  - 3\n  - 4\n  - 5\ne:\n  e1: 1\n  e2: "
    "bye"
    ""
)


@dag(
    start_date=datetime(2023, 4, 18),
    schedule=None,
    catchup=False,
    user_defined_macros={"my_json": my_json, "my_yaml": my_yaml},
    tags=["toy", "templating"],
)
def json_yaml_macros_toy_dag():
    @task(templates_dict={"my_config": "{{ macros.json.loads(my_json)}}"})
    def print_json_macro(**context):
        print(context["templates_dict"]["my_config"])

    print_json_macro()

    @task(templates_dict={"my_config": "{{ macros.yaml.safe_load(my_yaml)}}"})
    def print_yaml_macro(**context):
        print(context["templates_dict"]["my_config"])

    print_yaml_macro()


json_yaml_macros_toy_dag()
