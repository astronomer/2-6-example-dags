from airflow.notifications.basenotifier import BaseNotifier
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json


# all custom notifiers should inherit from the BaseNotifier
class MyS3Notifier(BaseNotifier):
    """An notifier that writes a textfile to an S3 bucket containing a string with information
    about the DAG and task sending the notification as well as task status and a message.

    :param message: The message to be written into the uploaded textfile.
    :param bucket_name: A list of topics or regex patterns the consumer should subscribe to.
    :param s3_key_notification: key to the file into which the notification information should be written,
    has to be able to receive string content.
    :param aws_conn_id: The connection ID used for the connection to AWS.
    :param include_metadata: The function that should be applied to fetched one at a time.
    :param s3_key_metadata: The full S3 key from which to retrieve file metadata.
    """

    template_fields = (
        "message",
        "bucket_name",
        "s3_key_notification",
        "aws_conn_id",
        "include_metadata",
        "s3_key_metadata",
    )

    def __init__(
        self,
        message,
        bucket_name,
        s3_key_notification,
        aws_conn_id="aws_default",
        include_metadata=False,
        s3_key_metadata=None,
    ):
        self.message = message
        self.bucket_name = bucket_name
        self.s3_key_notification = s3_key_notification
        self.aws_conn_id = aws_conn_id
        self.include_metadata = include_metadata
        self.s3_key_metadata = s3_key_metadata

    def notify(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        # access the Airflow context to retrieve values at runtime
        dag_run_id = context["task_instance"].dag_id
        task_id = context["task_instance"].task_id
        task_status = context["task_instance"].state

        upload_dict = {
            "Task": task_id,
            "DAG": dag_run_id,
            "Status": task_status,
            "Message": self.message,
        }

        if self.include_metadata:
            metadata = s3_hook.get_file_metadata(self.s3_key_metadata, self.bucket_name)

            # make the datetime value serializable
            metadata[0]["LastModified"] = metadata[0]["LastModified"].isoformat()

            print(
                f"This is the metadata of s3://{self.bucket_name}/{self.s3_key_metadata}: "
                + str(metadata)
            )

            upload_dict["Metadata"] = metadata

        # serialize the dictionary to a JSON string
        upload_str = json.dumps(upload_dict)

        # upload the file to S3
        s3_hook.load_string(
            string_data=upload_str,
            key=self.s3_key_notification,
            bucket_name=self.bucket_name,
            replace=True,
        )

        print(
            f"S3 metadata notification successful! Logged at {self.s3_key_notification}"
        )
