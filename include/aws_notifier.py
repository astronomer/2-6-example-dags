from airflow.notifications.basenotifier import BaseNotifier
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json

AWS_CONN_ID = "aws_conn"
S3_BUCKET_NAME = "mytxtbucket"
S3_NOTIFIER_FOLDER = "alert/"


class MyS3Notifier(BaseNotifier):
    template_fields = ("message",)

    def __init__(self, message, include_metadata=False):
        self.message = message
        self.include_metadata = include_metadata

    def notify(self, context):
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        dag_run_id = context["task_instance"].dag_id
        task_id = context["task_instance"].task_id
        task_status = context["task_instance"].state
        ds = context["ds"]
        s3_key_pipeline_file = "notify_test/" + ds + "_log.csv"

        upload_dict = {
            "Task": task_id,
            "DAG": dag_run_id,
            "Status": task_status,
            "Message": self.message,
        }

        if self.include_metadata:
            metadata = s3_hook.get_file_metadata(s3_key_pipeline_file, S3_BUCKET_NAME)

            # make the datetime value serializable
            metadata[0]["LastModified"] = metadata[0]["LastModified"].isoformat()

            print(
                f"This is the metadata of s3://{S3_BUCKET_NAME}/{s3_key_pipeline_file}: "
                + str(metadata)
            )

            upload_dict["Metadata"] = metadata

        object_key = f"{S3_NOTIFIER_FOLDER}{ds}_{task_status}_{dag_run_id}.txt"

        # Serialize the dictionary to a JSON string
        upload_str = json.dumps(upload_dict)

        # upload the file to S3
        s3_hook.load_string(
            string_data=upload_str,
            key=object_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )

        print(f"S3 metadata notification successful! Logged at {object_key}")
