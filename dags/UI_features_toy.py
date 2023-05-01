"""
### Toy DAG showing nested task groups to test UI features.

Toy example of a complex DAG structure to show UI features and nested task groups. 
"""

from airflow.decorators import dag, task_group, task
from airflow.operators.empty import EmptyOperator
from pendulum import datetime


@dag(start_date=datetime(2023, 4, 18), schedule=None, catchup=False, tags=["toy", "UI"])
def UI_features_toy():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task_group
    def tg_1():
        @task
        def t1():
            return "hi"

        @task
        def t2():
            return "hi"

        t1() >> t2()

    @task_group
    def tg_2(num):
        @task
        def t3(n):
            return n

        @task
        def t4(n):
            return n + 1

        t3(num) >> t4(num)

    @task_group
    def tg_3(num):
        @task
        def t5(n):
            return n

        @task
        def t6(n):
            return n + 1

        t5(num) >> t6(num)

    @task
    def t7(n):
        return n

    @task
    def t8(n):
        return n

    (
        start
        >> tg_1()
        >> [tg_2.expand(num=[1, 2, 3, 4]), tg_3(23), t7(19), t8.expand(n=[1, 2, 3, 42])]
        >> end
    )

    @task_group
    def big_tg():
        @task_group
        def nested_tg_1():
            @task_group
            def subnested_tg():
                @task
                def t1():
                    return "hi"

                @task
                def t2():
                    return "hi"

                t1() >> t2()

            subnested_tg()

        nested_tg_1()

        @task_group
        def nested_tg_2():
            @task_group
            def subnested_tg_1():
                @task
                def t1():
                    return "hi"

                @task
                def t2():
                    return "hi"

                t1() >> t2()

            for i in range(3):

                @task(task_id=f"task_{i}")
                def t1():
                    return "hi"

                t1()

            subnested_tg_1()

            @task_group
            def subnested_tg_2():
                @task
                def t1():
                    return "hi"

                @task
                def t2():
                    return "hi"

                t1() >> t2()

            subnested_tg_2()

        nested_tg_2()

        @task
        def t1():
            return "hi"

        @task
        def t2():
            return "hi"

        t1() >> t2()

    start >> big_tg()

    for x in ["A", "B", "C", "D"]:

        @task(task_id=f"path_{x}_step_1")
        def path_X_step_1():
            return "hi"

        @task(task_id=f"path_{x}_step_2")
        def path_X_step_2():
            return "hi"

        @task(task_id=f"path_{x}_step_3")
        def path_X_step_3():
            return "hi"

        @task(task_id=f"path_{x}_step_4")
        def path_X_step_4():
            return "hi"

        (
            start
            >> path_X_step_1()
            >> path_X_step_2()
            >> path_X_step_3()
            >> path_X_step_4()
            >> end
        )


UI_features_toy()
