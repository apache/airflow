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

from typing import TYPE_CHECKING, Any, Optional

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DynamoDBValueSensor(BaseSensorOperator):
    """
    Waits for an attribute value to be present for an item in a DynamoDB table.

    :param partition_key_name: DynamoDB partition key name
    :param partition_key_value: DynamoDB partition key value
    :param attribute_name: DynamoDB attribute name
    :param attribute_value: DynamoDB attribute value
    :param sort_key_name: (optional) DynamoDB sort key name
    :param sort_key_value: (optional) DynamoDB sort key value
    """

    def __init__(
        self,
        table_name: str,
        partition_key_name: str,
        partition_key_value: str,
        attribute_name: str,
        attribute_value: str,
        sort_key_name: Optional[str] = None,
        sort_key_value: Optional[str] = None,
        aws_conn_id: str | None = DynamoDBHook.default_conn_name,
        region_name: str | None = None,
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
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def poke(self, context: Context) -> bool:
        """Test DynamoDB item for matching attribute value"""
        key = {self.partition_key_name: self.partition_key_value}
        msg = (
            f"Checking table {self.table_name} for "
            + f"item Partition Key: {self.partition_key_name}={self.partition_key_value}"
        )

        if self.sort_key_value:
            key[self.sort_key_name] = self.sort_key_value
            msg += f"\nSort Key: {self.sort_key_name}={self.sort_key_value}"

        msg += f"\nattribute: {self.attribute_name}={self.attribute_value}"

        self.log.info(msg)
        table = self.hook.conn.Table(self.table_name)
        self.log.info(f"Table: {table}")
        response = table.get_item(Key=key)
        self.log.info(f"Response: {response}")
        self.log.info(f"Want: {self.attribute_name} = {self.attribute_value}")
        self.log.info(f'Got: {response["Item"][self.attribute_name]} = {self.attribute_value}')
        return response["Item"][self.attribute_name] == self.attribute_value

    @cached_property
    def hook(self) -> DynamoDBHook:
        """Create and return a DynamoDBHook"""
        return DynamoDBHook(self.aws_conn_id, region_name=self.region_name)
