import os
import tempfile

from typing import Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook


class SalesforceToS3Operator(BaseOperator):
    """
    Submits Salesforce query and uploads results to AWS S3
    :param query: The query to make to Salesforce.
    :type query: str
    :param s3_bucket_name: The bucket to upload to.
    :type s3_bucket_name: str
    :param s3_key: The object name to set when uploading the file.
    :type s3_key: str
    :param salesforce_conn_id: The name of the connection that has the parameters needed
        to connect to Salesforce.
    :type salesforce_conn_id: str
    :param export_format: Desired format of files to be exported.
    :type export_format: str
    :param query_params: Additional optional arguments
    :type query_params: dict
    :param include_deleted: True if the query should include deleted records.
    :type include_deleted: bool
    :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
        False if you want them to be left in the same format as they were in Salesforce.
        Leaving the value as False will result in datetimes being strings. Default: False
    :type coerce_to_timestamp: bool
    :param record_time_added: True if you want to add a Unix timestamp field
        to the resulting data that marks when the data was fetched from Salesforce. Default: False
    :type record_time_added: bool
    :param aws_conn_id: The name of the connection that has the parameters we need to connect to S3.
    :type aws_conn_id: str
    :type replace: bool
    :param encrypt: If True, the file will be encrypted on the server-side by S3 and will
        be stored in an encrypted form while at rest in S3.
    :type encrypt: bool
    :param gzip: If True, the file will be compressed locally.
    :type gzip: bool
    :param acl_policy: String specifying the canned ACL policy for the file being uploaded
        to the S3 bucket.
    :type acl_policy: str
    """

    template_fields = ("query", "s3_bucket_name", "s3_key")
    template_ext = (".sql",)
    template_fields_renderers = {"query": "sql"}

    def __init__(
        self,
        query: str,
        s3_bucket_name: str,
        s3_key: str,
        salesforce_conn_id: str,
        export_format: str = "csv",
        query_params: Optional[Dict] = None,
        include_deleted: bool = False,
        coerce_to_timestamp: bool = False,
        record_time_added: bool = False,
        aws_conn_id: str = "aws_default",
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.query = query
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key
        self.salesforce_conn_id = salesforce_conn_id
        self.export_format = export_format
        self.query_params = query_params
        self.include_deleted = include_deleted
        self.coerce_to_timestamp = coerce_to_timestamp
        self.record_time_added = record_time_added
        self.aws_conn_id = aws_conn_id
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy

    def execute(self, context: Dict):
        salesforce_hook = SalesforceHook(conn_id=self.salesforce_conn_id)
        response = salesforce_hook.make_query(
            query=self.query,
            include_deleted=self.include_deleted,
            query_params=self.query_params,
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "salesforce_temp_file")
            salesforce_hook.write_object_to_file(
                query_results=response["records"],
                filename=path,
                fmt=self.export_format,
                coerce_to_timestamp=self.coerce_to_timestamp,
                record_time_added=self.record_time_added,
            )

            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            s3_hook.load_file(
                filename=path,
                key=self.s3_key,
                bucket_name=self.s3_bucket_name,
                replace=self.replace,
                encrypt=self.encrypt,
                gzip=self.gzip,
                acl_policy=self.acl_policy,
            )

            s3_uri = f"s3://{self.s3_bucket_name}/{self.s3_key}"
            self.log.info(f"Salesforce data uploaded to S3 at {s3_uri}.")

            return s3_uri
