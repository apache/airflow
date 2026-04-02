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

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.sdk import Context


class DynamoDBValueSensor(AwsBaseSensor[DynamoDBHook]):
    """
    Waits for an attribute value to be present for an item in a DynamoDB table.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:DynamoDBValueSensor`

    :param table_name: DynamoDB table name
    :param partition_key_name: DynamoDB partition key name
    :param partition_key_value: DynamoDB partition key value
    :param attribute_name: DynamoDB attribute name
    :param attribute_value: DynamoDB attribute value
    :param sort_key_name: (optional) DynamoDB sort key name
    :param sort_key_value: (optional) DynamoDB sort key value
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = DynamoDBHook
    template_fields: Sequence[str] = aws_template_fields(
        "table_name",
        "partition_key_name",
        "partition_key_value",
        "attribute_name",
        "attribute_value",
        "sort_key_name",
        "sort_key_value",
    )

    def __init__(
        self,
        table_name: str,
        partition_key_name: str,
        partition_key_value: str,
        attribute_name: str,
        attribute_value: str | Iterable[str],
        sort_key_name: str | None = None,
        sort_key_value: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.partition_key_name = partition_key_name
        self.partition_key_value = partition_key_value
        self.attribute_name = attribute_name
        self.attribute_value = attribute_value
        self.sort_key_name = sort_key_name
        self.sort_key_value = sort_key_value

    def poke(self, context: Context) -> bool:
        """Test DynamoDB item for matching attribute value."""
        key = {self.partition_key_name: self.partition_key_value}
        msg = (
            f"Checking table {self.table_name} for "
            f"item Partition Key: {self.partition_key_name}={self.partition_key_value}"
        )

        if self.sort_key_name and self.sort_key_value:
            key = {self.partition_key_name: self.partition_key_value, self.sort_key_name: self.sort_key_value}
            msg += f"\nSort Key: {self.sort_key_name}={self.sort_key_value}"

        msg += f"\nattribute: {self.attribute_name}={self.attribute_value}"

        self.log.info(msg)
        table = self.hook.conn.Table(self.table_name)
        self.log.info("Table: %s", table)
        self.log.info("Key: %s", key)

        try:
            response = table.get_item(Key=key)
        except ClientError as err:
            self.log.error(
                "Couldn't get %s from table %s.\nError Code: %s\nError Message: %s",
                key,
                self.table_name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            return False
        else:
            try:
                item_attribute_value = response["Item"][self.attribute_name]
                self.log.info("Response: %s", response)
                self.log.info("Want: %s = %s", self.attribute_name, self.attribute_value)
                self.log.info("Got: {response['Item'][self.attribute_name]} = %s", item_attribute_value)
                return item_attribute_value in (
                    [self.attribute_value] if isinstance(self.attribute_value, str) else self.attribute_value
                )
            except KeyError:
                return False
