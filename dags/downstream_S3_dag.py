"""
A DAG triggered from a Dataset update which fetches a CSV file from an AWS S3
bucket and reads its contents into a DuckDB database, runs a transformation
and provides an analysis using the Astro Python SDK.
"""

from airflow import Dataset
from airflow.decorators import dag
from pendulum import datetime
import pandas as pd

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

MY_BUCKET_NAME = "mytxtbucket"
PROCESS_FOLDER = "process"
AWS_CONN_ID = "aws_conn"
DB_CONN_ID = "duckdb_conn"
S3_URI = f"s3://{MY_BUCKET_NAME}/{PROCESS_FOLDER}/"
ITEM = "strawberries"


@aql.transform
def transform_orders(in_table, item):
    return """
            SELECT * FROM {{ in_table }} WHERE items = {{ item }};
            """


@aql.dataframe
def analyze(df: pd.DataFrame):
    df["total_price"] = df["amount"] * df["price per item"]
    bill_by_customer = df.groupby("customer")["total_price"].sum().reset_index()
    print(bill_by_customer)
    return bill_by_customer


@dag(
    start_date=datetime(2023, 4, 18),
    schedule=[Dataset(S3_URI)],
    catchup=False,
    tags=["ContinuousTimetable"],
)
def downstream_S3_dag():
    ingest_data = aql.load_file(
        input_file=File(path=f"{S3_URI}" + "{{ ds }}_log.csv", conn_id=AWS_CONN_ID),
        output_table=Table(conn_id=DB_CONN_ID),
    )

    transformed_data = transform_orders(
        ingest_data,
        item=ITEM,
        output_table=Table(
            conn_id=DB_CONN_ID,
        ),
    )

    analyze(
        transformed_data,
        output_table=Table(conn_id=DB_CONN_ID, name="billing_table"),
    )

    #aql.cleanup()


downstream_S3_dag()
