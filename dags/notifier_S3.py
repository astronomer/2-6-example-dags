"""
### Simple ELT DAG using a custom notifier to notify with writing files to S3

Use case example of a custom notifier set at the DAG level. This DAG will upload 
a local CSV file with orders data to S3 and then run a pipeline on said file. 
Depending on the THIS_DAG_SHOULD_SUCCEED variable, the DAG will succeed of fail.

In both cases, DAG success and failure, the DAG will use the custom MyS3Notifier
to upload a new file to an S3 bucket containing information about the failed DAG
and depending on the include_metadata parameter of the custom notifier including
metadata about the CSV file.
"""

from airflow.decorators import dag
from pendulum import datetime
from include.aws_notifier import MyS3Notifier
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
import pandas as pd
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

MY_BUCKET_NAME = "mytxtbucket"
PIPELINE_FOLDER = "my_logs"
NOTIFICATION_FOLDER = "alerts"
OBEJCT_KEY = f"{PIPELINE_FOLDER}/my_log.csv"
LOCAL_FILE_PATH = "include/my_log.csv"
AWS_CONN_ID = "aws_conn"
DB_CONN_ID = "duckdb_conn"
ITEM = "melons"

# Set this variable to False to see the behavior in case of a pipeline failure
THIS_DAG_SHOULD_SUCCEED = True


@aql.transform
def transform_orders(in_table, item):
    return """
            SELECT * FROM {{ in_table }} WHERE items = {{ item }};
            """


@aql.dataframe
def analyze_success(df: pd.DataFrame):
    df["total_price"] = df["amount"] * df["price per item"]
    bill_by_customer = df.groupby("customer")["total_price"].sum().reset_index()
    print(bill_by_customer)
    return bill_by_customer


@aql.dataframe
def analyze_fail(df: pd.DataFrame):
    df["total_price"] = df["amount"] * df["price per item"] + df["FAKE COLUMN"]
    bill_by_customer = df.groupby("customer")["total_price"].sum().reset_index()
    print(bill_by_customer)
    return bill_by_customer


@dag(
    start_date=datetime(2023, 4, 18),
    schedule=None,
    catchup=False,
    on_failure_callback=MyS3Notifier(
        message="{{ ds }}: my_log.csv pipeline failed on!",
        bucket_name=MY_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
        s3_key_notification=NOTIFICATION_FOLDER
        + "/{{ ds }}_{{ ti.state }}_{{ ti.dag_id }}.txt",
        include_metadata=True,
        s3_key_metadata=PIPELINE_FOLDER + "/" + "my_log.csv",
    ),
    on_success_callback=MyS3Notifier(
        message="{{ ds }}: my_log.csv pipeline was successful!",
        bucket_name=MY_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
        s3_key_notification=NOTIFICATION_FOLDER
        + "/{{ ds }}_{{ ti.state }}_{{ ti.dag_id }}.txt",
    ),
    tags=["Notifier"],
)
def notifier_S3_dag():
    copy_file_to_S3 = LocalFilesystemToS3Operator(
        task_id="copy_file_to_S3",
        filename=LOCAL_FILE_PATH,
        dest_key=OBEJCT_KEY,
        dest_bucket=MY_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
        replace=True,
    )

    ingest_data = aql.load_file(
        input_file=File(
            path=f"s3://{MY_BUCKET_NAME}/" + OBEJCT_KEY, conn_id=AWS_CONN_ID
        ),
        output_table=Table(conn_id=DB_CONN_ID),
    )

    transformed_data = transform_orders(
        ingest_data,
        item=ITEM,
        output_table=Table(
            conn_id=DB_CONN_ID,
        ),
    )

    if THIS_DAG_SHOULD_SUCCEED:
        analyze_success(
            transformed_data,
            output_table=Table(conn_id=DB_CONN_ID, name="billing_table"),
        )

    else:
        analyze_fail(
            transformed_data,
            output_table=Table(conn_id=DB_CONN_ID, name="billing_table"),
        )

    copy_file_to_S3 >> ingest_data


notifier_S3_dag()
