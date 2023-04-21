"""
Helper DAG to upload a CSV file to S3 for the ContinuousTimetable example.
"""

from airflow.decorators import dag
from pendulum import datetime
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

MY_BUCKET_NAME = "mytxtbucket"
FILE_NAME = "{{ ds }}_log.csv"
FILE_PATH = "include/my_log.csv"
AWS_CONN_ID = "aws_conn"


@dag(
    start_date=datetime(2023, 4, 18),
    schedule=None,
    catchup=False,
    tags=["ContinuousTimetable", "Helper"],
)
def load_file_to_S3():
    copy_file_to_S3 = LocalFilesystemToS3Operator(
        task_id="copy_file_to_S3",
        filename=FILE_PATH,
        dest_key=f"logs/{FILE_NAME}",
        dest_bucket=MY_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
        replace=True,
    )


load_file_to_S3()
