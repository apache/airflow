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
"""
This module contains operators to replicate records from
DynamoDB table to S3.
"""
from __future__ import annotations

import json
from copy import copy
from decimal import Decimal
from os.path import getsize
from tempfile import NamedTemporaryFile
from typing import IO, TYPE_CHECKING, Any, Callable, Sequence
from uuid import uuid4

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class JSONEncoder(json.JSONEncoder):
    """Custom json encoder implementation"""

    def default(self, obj):
        """Convert decimal objects in a json serializable format."""
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def _convert_item_to_json_bytes(item: dict[str, Any]) -> bytes:
    return (json.dumps(item, cls=JSONEncoder) + "\n").encode("utf-8")


def _upload_file_to_s3(
    file_obj: IO, bucket_name: str, s3_key_prefix: str, aws_conn_id: str = "aws_default"
) -> None:
    s3_client = S3Hook(aws_conn_id=aws_conn_id).get_conn()
    file_obj.seek(0)
    s3_client.upload_file(
        Filename=file_obj.name,
        Bucket=bucket_name,
        Key=s3_key_prefix + str(uuid4()),
    )


class DynamoDBToS3Operator(BaseOperator):
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
    :param dynamodb_scan_kwargs: kwargs pass to <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan>
    :param s3_key_prefix: Prefix of s3 object key
    :param process_func: How we transforms a dynamodb item to bytes. By default we dump the json
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    """  # noqa: E501

    template_fields: Sequence[str] = (
        "s3_bucket_name",
        "s3_key_prefix",
        "dynamodb_table_name",
    )
    template_fields_renderers = {
        "dynamodb_scan_kwargs": "json",
    }

    def __init__(
        self,
        *,
        dynamodb_table_name: str,
        s3_bucket_name: str,
        file_size: int,
        dynamodb_scan_kwargs: dict[str, Any] | None = None,
        s3_key_prefix: str = "",
        process_func: Callable[[dict[str, Any]], bytes] = _convert_item_to_json_bytes,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_size = file_size
        self.process_func = process_func
        self.dynamodb_table_name = dynamodb_table_name
        self.dynamodb_scan_kwargs = dynamodb_scan_kwargs
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context) -> None:
        hook = DynamoDBHook(aws_conn_id=self.aws_conn_id)
        table = hook.get_conn().Table(self.dynamodb_table_name)

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
                    _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix, self.aws_conn_id)

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
            if getsize(temp_file.name) >= self.file_size:
                _upload_file_to_s3(temp_file, self.s3_bucket_name, self.s3_key_prefix, self.aws_conn_id)
                temp_file.close()

                temp_file = NamedTemporaryFile()
        return temp_file
