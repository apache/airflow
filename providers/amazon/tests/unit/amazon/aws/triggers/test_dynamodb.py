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

from unittest import mock
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.triggers.dynamodb import DynamoDBValueSensorTrigger
from airflow.triggers.base import TriggerEvent

TEST_TABLE_NAME = "test-table"
TEST_PK_NAME = "PK"
TEST_PK_VALUE = "PKTest"
TEST_SK_NAME = "SK"
TEST_SK_VALUE = "SKTest"
TEST_ATTRIBUTE_NAME = "Foo"
TEST_ATTRIBUTE_VALUE = "Bar"
TEST_WAITER_DELAY = 1
TEST_CONN_ID = "test-conn-id"
TEST_REGION_NAME = "eu-central-1"


def _build_trigger(**kwargs) -> DynamoDBValueSensorTrigger:
    params = {
        "table_name": TEST_TABLE_NAME,
        "partition_key_name": TEST_PK_NAME,
        "partition_key_value": TEST_PK_VALUE,
        "attribute_name": TEST_ATTRIBUTE_NAME,
        "attribute_value": TEST_ATTRIBUTE_VALUE,
        "waiter_delay": TEST_WAITER_DELAY,
        "aws_conn_id": TEST_CONN_ID,
        "region_name": TEST_REGION_NAME,
        **kwargs,
    }
    return DynamoDBValueSensorTrigger(**params)


class TestDynamoDBValueSensorTrigger:
    def test_serialize(self):
        trigger = _build_trigger(sort_key_name=TEST_SK_NAME, sort_key_value=TEST_SK_VALUE)

        class_path, args = trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.dynamodb.DynamoDBValueSensorTrigger"
        assert args["table_name"] == TEST_TABLE_NAME
        assert args["partition_key_name"] == TEST_PK_NAME
        assert args["partition_key_value"] == TEST_PK_VALUE
        assert args["attribute_name"] == TEST_ATTRIBUTE_NAME
        # a single string value is normalized to a list so serialization round-trips
        assert args["attribute_value"] == [TEST_ATTRIBUTE_VALUE]
        assert args["sort_key_name"] == TEST_SK_NAME
        assert args["sort_key_value"] == TEST_SK_VALUE
        assert args["waiter_delay"] == TEST_WAITER_DELAY
        assert args["aws_conn_id"] == TEST_CONN_ID
        assert args["region_name"] == TEST_REGION_NAME
        assert args["verify"] is None
        assert args["botocore_config"] is None

    def test_serialize_generic_hook_params(self):
        trigger = _build_trigger(verify=False, botocore_config={"read_timeout": 99})
        _, args = trigger.serialize()
        assert args["verify"] is False
        assert args["botocore_config"] == {"read_timeout": 99}

        hook = trigger.hook
        assert hook.client_type == "dynamodb"
        assert hook.aws_conn_id == TEST_CONN_ID
        assert hook._region_name == TEST_REGION_NAME
        assert hook._verify is False
        assert hook._config.read_timeout == 99

    def test_key_with_and_without_sort_key(self):
        trigger = _build_trigger()
        assert trigger.key == {TEST_PK_NAME: {"S": TEST_PK_VALUE}}

        trigger = _build_trigger(sort_key_name=TEST_SK_NAME, sort_key_value=TEST_SK_VALUE)
        assert trigger.key == {
            TEST_PK_NAME: {"S": TEST_PK_VALUE},
            TEST_SK_NAME: {"S": TEST_SK_VALUE},
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_async_conn")
    async def test_run_success_when_value_matches(self, mock_async_conn):
        client = AsyncMock()
        mock_async_conn.return_value.__aenter__.return_value = client
        client.get_item.return_value = {
            "Item": {
                TEST_PK_NAME: {"S": TEST_PK_VALUE},
                TEST_ATTRIBUTE_NAME: {"S": TEST_ATTRIBUTE_VALUE},
            }
        }

        trigger = _build_trigger()
        generator = trigger.run()
        response = await generator.asend(None)

        client.get_item.assert_called_once_with(
            TableName=TEST_TABLE_NAME, Key={TEST_PK_NAME: {"S": TEST_PK_VALUE}}
        )
        assert response == TriggerEvent({"status": "success"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_async_conn")
    async def test_run_keeps_polling_until_value_matches(self, mock_async_conn, mock_sleep):
        mock_sleep.return_value = True
        client = AsyncMock()
        mock_async_conn.return_value.__aenter__.return_value = client
        client.get_item.side_effect = [
            {},  # item does not exist yet
            {"Item": {TEST_PK_NAME: {"S": TEST_PK_VALUE}, TEST_ATTRIBUTE_NAME: {"S": "wrong"}}},
            {"Item": {TEST_PK_NAME: {"S": TEST_PK_VALUE}, TEST_ATTRIBUTE_NAME: {"S": TEST_ATTRIBUTE_VALUE}}},
        ]

        trigger = _build_trigger()
        generator = trigger.run()
        response = await generator.asend(None)

        assert client.get_item.call_count == 3
        assert response == TriggerEvent({"status": "success"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_async_conn")
    async def test_run_tolerates_client_error(self, mock_async_conn, mock_sleep):
        # Same tolerance as the sensor's poke: a ClientError means "not there yet", not failure.
        mock_sleep.return_value = True
        client = AsyncMock()
        mock_async_conn.return_value.__aenter__.return_value = client
        client.get_item.side_effect = [
            ClientError(
                error_response={"Error": {"Code": "ResourceNotFoundException", "Message": "no table"}},
                operation_name="GetItem",
            ),
            {"Item": {TEST_PK_NAME: {"S": TEST_PK_VALUE}, TEST_ATTRIBUTE_NAME: {"S": TEST_ATTRIBUTE_VALUE}}},
        ]

        trigger = _build_trigger()
        generator = trigger.run()
        response = await generator.asend(None)

        assert client.get_item.call_count == 2
        assert response == TriggerEvent({"status": "success"})

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_async_conn")
    async def test_run_success_with_multiple_expected_values(self, mock_async_conn):
        client = AsyncMock()
        mock_async_conn.return_value.__aenter__.return_value = client
        client.get_item.return_value = {
            "Item": {TEST_PK_NAME: {"S": TEST_PK_VALUE}, TEST_ATTRIBUTE_NAME: {"S": "Bar2"}}
        }

        trigger = _build_trigger(attribute_value=["Bar1", "Bar2", "Bar3"])
        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success"})
