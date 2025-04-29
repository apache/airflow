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
"""This module contains the Amazon DynamoDB Hook."""

from __future__ import annotations

from collections.abc import Iterable
from functools import cached_property
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

if TYPE_CHECKING:
    from botocore.client import BaseClient


class DynamoDBHook(AwsBaseHook):
    """
    Interact with Amazon DynamoDB.

    Provide thick wrapper around
    :external+boto3:py:class:`boto3.resource("dynamodb") <DynamoDB.ServiceResource>`.

    :param table_keys: partition key and sort key
    :param table_name: target DynamoDB table

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(
        self, *args, table_keys: list | None = None, table_name: str | None = None, **kwargs
    ) -> None:
        self.table_keys = table_keys
        self.table_name = table_name
        kwargs["resource_type"] = "dynamodb"
        super().__init__(*args, **kwargs)

    @cached_property
    def client(self) -> BaseClient:
        """Return boto3 client."""
        return self.get_conn().meta.client

    def write_batch_data(self, items: Iterable) -> bool:
        """
        Write batch items to DynamoDB table with provisioned throughout capacity.

        .. seealso::
            - :external+boto3:py:meth:`DynamoDB.ServiceResource.Table`
            - :external+boto3:py:meth:`DynamoDB.Table.batch_writer`
            - :external+boto3:py:meth:`DynamoDB.Table.put_item`

        :param items: list of DynamoDB items.
        """
        try:
            table = self.get_conn().Table(self.table_name)

            with table.batch_writer(overwrite_by_pkeys=self.table_keys) as batch:
                for item in items:
                    batch.put_item(Item=item)
            return True
        except Exception as general_error:
            raise AirflowException(f"Failed to insert items in dynamodb, error: {general_error}")

    def get_import_status(self, import_arn: str) -> tuple[str, str | None, str | None]:
        """
        Get import status from Dynamodb.

        :param import_arn: The Amazon Resource Name (ARN) for the import.
        :return: Import status, Error code and Error message
        """
        self.log.info("Poking for Dynamodb import %s", import_arn)

        try:
            describe_import = self.client.describe_import(ImportArn=import_arn)
            status = describe_import["ImportTableDescription"]["ImportStatus"]
            error_code = describe_import["ImportTableDescription"].get("FailureCode")
            error_msg = describe_import["ImportTableDescription"].get("FailureMessage")
            return status, error_code, error_msg
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "ImportNotFoundException":
                raise AirflowException("S3 import into Dynamodb job not found.")
            raise e
