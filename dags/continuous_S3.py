"""
A DAG using the ContinuousTimetable to continuously wait for a csv file to drop in
an S3 bucket. This pattern is especially useful when waiting for a very irregular
event in an external data tool.
After the file has dropped the DAG copies the file into a different folder 
and updates a Dataset causing a downstream task to run. Lastly the file in the
ingestion folder is deleted.
"""

from airflow import Dataset
from airflow.decorators import dag
from pendulum import datetime
from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensorAsync
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3ListOperator,
    S3DeleteObjectsOperator,
)

MY_BUCKET_NAME = "mytxtbucket"
LOGS_FOLDER = "logs"
PROCESS_FOLDER = "process"
AWS_CONN_ID = "aws_conn"
S3_URI = f"s3://{MY_BUCKET_NAME}/{PROCESS_FOLDER}/"


@dag(
    start_date=datetime(2023, 4, 18),
    schedule="@continuous",
    max_active_runs=1,
    catchup=False,
    tags=["ContinuousTimetable"],
)
def continuous_S3():
    # deferrable task that waits for a file to drop in an S3 bucket
    wait_for_file = S3KeySensorAsync(
        task_id="wait_for_file",
        bucket_key=f"{LOGS_FOLDER}" + "/{{ ds }}_log.csv",
        bucket_name=MY_BUCKET_NAME,
        wildcard_match=True,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=10,  # look for the file every 10 seconds
        timeout=60 * 60 * 24,  # The task will time out and fail after 24hrs
    )

    list_files = S3ListOperator(
        task_id="list_files",
        bucket=MY_BUCKET_NAME,
        prefix=f"{LOGS_FOLDER}/",
        aws_conn_id=AWS_CONN_ID,
    )

    # map function to turn list of files in the S3 bucket's LOGS_FOLDER into
    # a list of dictionaries containing both the source and destination key
    def create_kwargs(file_name):
        print(file_name)
        return {
            "source_bucket_key": file_name,
            "dest_bucket_key": f"{PROCESS_FOLDER}/" + file_name.split("/")[-1],
        }

    copy_file = S3CopyObjectOperator.partial(
        task_id="copy_file",
        source_bucket_name=MY_BUCKET_NAME,
        dest_bucket_name=MY_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
        outlets=[Dataset(S3_URI)],  # this task produces to a Dataset
    ).expand_kwargs(
        list_files.output.map(create_kwargs)
    )  # expand_kwargs is a dynamic task mapping function

    delete_file = S3DeleteObjectsOperator(
        task_id="delete_file",
        bucket=MY_BUCKET_NAME,
        keys=list_files.output,
        aws_conn_id=AWS_CONN_ID,
    )

    wait_for_file >> list_files >> copy_file >> delete_file


continuous_S3()
