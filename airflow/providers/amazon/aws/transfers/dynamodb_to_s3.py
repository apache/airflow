#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import json
import os
from copy import copy
from datetime import datetime
from decimal import Decimal
from functools import cached_property
from tempfile import NamedTemporaryFile
from typing import IO, TYPE_CHECKING, Any, Callable, Sequence
from uuid import uuid4

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.base import AwsToAwsBaseOperator
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from airflow.utils.types import ArgNotSet


class JSONEncoder(json.JSONEncoder):
    """Custom json encoder implementation."""

    def default(self, obj):
        """Convert decimal objects in a json serializable format."""
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def _convert_item_to_json_bytes(item: dict[str, Any]) -> bytes:
    return (json.dumps(item, cls=JSONEncoder) + "\n").encode("utf-8")


def _upload_file_to_s3(
    file_obj: IO,
    bucket_name: str,
    s3_key_prefix: str,
    aws_conn_id: str | None | ArgNotSet = AwsBaseHook.default_conn_name,
) -> None:
    s3_client = S3Hook(aws_conn_id=aws_conn_id).get_conn()
    file_obj.seek(0)
    s3_client.upload_file(
        Filename=file_obj.name,
        Bucket=bucket_name,
        Key=s3_key_prefix + str(uuid4()),
    )


class DynamoDBToS3Operator(AwsToAwsBaseOperator):
    """
    Replicates records from a DynamoDB table to S3.

    It scans a DynamoDB table and writes the received records to a file
    on the local filesystem. It flushes the file to S3 once the file size
    exceeds the file size limit specified by the user.

    Users can also specify a filtering criteria using dynamodb_scan_kwargs
    to only replicate records that satisfy the criteria.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/transfer:DynamoDBToS3Operator`

    :param dynamodb_table_name: Dynamodb table to replicate data from
    :param s3_bucket_name: S3 bucket to replicate data to
    :param file_size: Flush file to s3 if file size >= file_size
    :param dynamodb_scan_kwargs: kwargs pass to
        <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan>
    :param s3_key_prefix: Prefix of s3 object key
    :param process_func: How we transform a dynamodb item to bytes. By default, we dump the json
    :param point_in_time_export: Boolean value indicating the operator to use 'scan' or 'point in time export'
    :param export_time: Time in the past from which to export table data, counted in seconds from the start of
     the Unix epoch. The table export will be a snapshot of the table's state at this point in time.
    :param export_format: The format for the exported data. Valid values for ExportFormat are DYNAMODB_JSON
     or ION.
    :param export_table_to_point_in_time_kwargs: extra parameters for the boto3
        `export_table_to_point_in_time` function all. e.g. `ExportType`, `IncrementalExportSpecification`
    :param check_interval: The amount of time in seconds to wait between attempts. Only if ``export_time`` is
        provided.
    :param max_attempts: The maximum number of attempts to be made. Only if ``export_time`` is provided.
    """

    template_fields: Sequence[str] = (
        *AwsToAwsBaseOperator.template_fields,
        "dynamodb_table_name",
        "s3_bucket_name",
        "file_size",
        "dynamodb_scan_kwargs",
        "s3_key_prefix",
        "export_time",
        "export_format",
        "export_table_to_point_in_time_kwargs",
        "check_interval",
        "max_attempts",
    )

    template_fields_renderers = {
        "dynamodb_scan_kwargs": "json",
        "export_table_to_point_in_time_kwargs": "json",
    }

    def __init__(
        self,
        *,
        dynamodb_table_name: str,
        s3_bucket_name: str,
        file_size: int = 1000,
        dynamodb_scan_kwargs: dict[str, Any] | None = None,
        s3_key_prefix: str = "",
        process_func: Callable[[dict[str, Any]], bytes] = _convert_item_to_json_bytes,
        point_in_time_export: bool = False,
        export_time: datetime | None = None,
        export_format: str = "DYNAMODB_JSON",
        export_table_to_point_in_time_kwargs: dict | None = None,
        check_interval: int = 30,
        max_attempts: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_size = file_size
        self.process_func = process_func
        self.dynamodb_table_name = dynamodb_table_name
        self.dynamodb_scan_kwargs = dynamodb_scan_kwargs
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix
        self.point_in_time_export = point_in_time_export
        self.export_time = export_time
        self.export_format = export_format
        self.export_table_to_point_in_time_kwargs = export_table_to_point_in_time_kwargs or {}
        self.check_interval = check_interval
        self.max_attempts = max_attempts

    @cached_property
    def hook(self):
        """Create DynamoDBHook."""
        return DynamoDBHook(aws_conn_id=self.source_aws_conn_id)

    def execute(self, context: Context) -> None:
        # There are 2 separate export to point in time configuration:
        # 1. Full export, which takes the export_time arg.
        # 2. Incremental export, which takes the incremental_export_... args
        # Hence export time could not be used as the proper indicator for the `_export_table_to_point_in_time`
        # function. This change introduces a new boolean, as the indicator for whether the operator scans
        # and export entire data or using the point in time functionality.
        if self.point_in_time_export or self.export_time:
            self._export_table_to_point_in_time()
        else:
            self._export_entire_data()

    def _export_table_to_point_in_time(self):
        """
        Export data to point in time.

        Full export exports data from start of epoc till `export_time`.
        Table export will be a snapshot of the table's state at this point in time.

        Incremental export exports the data from a specific datetime to a specific datetime


        Note: S3BucketOwner is a required parameter when exporting to a S3 bucket in another account.
        """
        if self.export_time and self.export_time > datetime.now(self.export_time.tzinfo):
            raise ValueError("The export_time parameter cannot be a future time.")

        client = self.hook.conn.meta.client
        table_description = client.describe_table(TableName=self.dynamodb_table_name)

        export_table_to_point_in_time_base_args = {
            "TableArn": table_description.get("Table", {}).get("TableArn"),
            "ExportTime": self.export_time,
            "S3Bucket": self.s3_bucket_name,
            "S3Prefix": self.s3_key_prefix,
            "ExportFormat": self.export_format,
        }
        export_table_to_point_in_time_args = {
            **export_table_to_point_in_time_base_args,
            **self.export_table_to_point_in_time_kwargs,
        }

        args_filtered = prune_dict(export_table_to_point_in_time_args)

        response = client.export_table_to_point_in_time(**args_filtered)
        waiter = self.hook.get_waiter("export_table")
        export_arn = response.get("ExportDescription", {}).get("ExportArn")
        waiter.wait(
            ExportArn=export_arn,
            WaiterConfig={"Delay": self.check_interval, "MaxAttempts": self.max_attempts},
        )

    def _export_entire_data(self):
        """Export all data from the table."""
        table = self.hook.conn.Table(self.dynamodb_table_name)
        scan_kwargs = copy(self.dynamodb_scan_kwargs) if self.dynamodb_scan_kwargs else {}
        err = None
        f: IO[Any]
        with NamedTemporaryFile() as f:
            try:
                f = self._scan_dynamodb_and_upload_to_s3(f, scan_kwargs, table)
            except Exception as e:
                err = e
                raise e
            finally:
                if err is None:
                    _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix, self.dest_aws_conn_id)

    def _scan_dynamodb_and_upload_to_s3(self, temp_file: IO, scan_kwargs: dict, table: Any) -> IO:
        while True:
            response = table.scan(**scan_kwargs)
            items = response["Items"]
            for item in items:
                temp_file.write(self.process_func(item))

            if "LastEvaluatedKey" not in response:
                # no more items to scan
                break

            last_evaluated_key = response["LastEvaluatedKey"]
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

            # Upload the file to S3 if reach file size limit
            if os.path.getsize(temp_file.name) >= self.file_size:
                _upload_file_to_s3(temp_file, self.s3_bucket_name, self.s3_key_prefix, self.dest_aws_conn_id)
                temp_file.close()

                temp_file = NamedTemporaryFile()
        return temp_file
