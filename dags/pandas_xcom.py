"""
### Toy DAG passing a pandas.DataFrame object via XCom

Toy DAG showing XCom serialization of Pandas dataframes.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd


@dag(
    start_date=datetime(2023, 4, 18),
    schedule=None,
    catchup=False,
    tags=["toy", "Pandas"],
)
def pandas_xcom():

    @task 
    def push_pd_df():
        return pd.DataFrame({"a":[1,2], "b": [3,4]})
    

    @task 
    def pull_pd_df(df):
        print(df)


    pull_pd_df(push_pd_df())

pandas_xcom()