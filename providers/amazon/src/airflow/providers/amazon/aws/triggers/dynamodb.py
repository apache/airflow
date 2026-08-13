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

import asyncio
from collections.abc import AsyncIterator, Iterable
from functools import cached_property
from typing import Any

from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger, TriggerEvent
else:
    from airflow.triggers.base import (  # type: ignore
        BaseTrigger as BaseEventTrigger,
        TriggerEvent,
    )


class DynamoDBValueSensorTrigger(BaseEventTrigger):
    """
    Asynchronously poll a DynamoDB item until the given attribute matches one of the expected values.

    The polling uses the low-level DynamoDB client (aiobotocore exposes clients only, not the
    boto3 resource API used by ``DynamoDBHook``), so keys are sent as typed string attributes,
    matching the string-typed key parameters of :class:`~airflow.providers.amazon.aws.sensors.dynamodb.DynamoDBValueSensor`.

    :param table_name: DynamoDB table name
    :param partition_key_name: DynamoDB partition key name
    :param partition_key_value: DynamoDB partition key value
    :param attribute_name: DynamoDB attribute name
    :param attribute_value: expected DynamoDB attribute value (or values, any of which matches)
    :param sort_key_name: (optional) DynamoDB sort key name
    :param sort_key_value: (optional) DynamoDB sort key value
    :param waiter_delay: The time in seconds to wait between DynamoDB API calls
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    def __init__(
        self,
        table_name: str,
        partition_key_name: str,
        partition_key_value: str,
        attribute_name: str,
        attribute_value: str | Iterable[str],
        sort_key_name: str | None = None,
        sort_key_value: str | None = None,
        waiter_delay: int = 60,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ):
        self.table_name = table_name
        self.partition_key_name = partition_key_name
        self.partition_key_value = partition_key_value
        self.attribute_name = attribute_name
        self.attribute_value = (
            [attribute_value] if isinstance(attribute_value, str) else list(attribute_value)
        )
        self.sort_key_name = sort_key_name
        self.sort_key_value = sort_key_value
        self.waiter_delay = waiter_delay
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "table_name": self.table_name,
                "partition_key_name": self.partition_key_name,
                "partition_key_value": self.partition_key_value,
                "attribute_name": self.attribute_name,
                "attribute_value": self.attribute_value,
                "sort_key_name": self.sort_key_name,
                "sort_key_value": self.sort_key_value,
                "waiter_delay": self.waiter_delay,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
            },
        )

    @cached_property
    def hook(self) -> AwsBaseHook:
        # DynamoDBHook is resource-based, but async connections are only available for
        # clients, so the trigger talks to DynamoDB through a client-type hook instead.
        return AwsBaseHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
            client_type="dynamodb",
        )

    @property
    def key(self) -> dict[str, Any]:
        key = {self.partition_key_name: {"S": self.partition_key_value}}
        if self.sort_key_name and self.sort_key_value:
            key[self.sort_key_name] = {"S": self.sort_key_value}
        return key

    async def poke(self, client: Any) -> bool:
        """Test the DynamoDB item for a matching attribute value, mirroring the sensor's poke."""
        self.log.info(
            "Checking table %s for item with key %s, waiting for attribute %s to be one of %s",
            self.table_name,
            self.key,
            self.attribute_name,
            self.attribute_value,
        )
        try:
            response = await client.get_item(TableName=self.table_name, Key=self.key)
        except ClientError as err:
            # Same tolerance as the sensor's poke: log and keep trying until the task times out.
            self.log.error(
                "Couldn't get %s from table %s.\nError Code: %s\nError Message: %s",
                self.key,
                self.table_name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            return False

        try:
            typed_attribute_value = response["Item"][self.attribute_name]
        except KeyError:
            return False
        item_attribute_value = TypeDeserializer().deserialize(typed_attribute_value)
        self.log.info("Got: %s = %s", self.attribute_name, item_attribute_value)
        return item_attribute_value in self.attribute_value

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            # This loop runs until the timeout set in the sensor's self.defer call is reached.
            async with await self.hook.get_async_conn() as client:
                if await self.poke(client=client):
                    yield TriggerEvent({"status": "success"})
                    return
            await asyncio.sleep(self.waiter_delay)
