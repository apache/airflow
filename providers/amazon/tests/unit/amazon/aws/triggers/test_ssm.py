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

import pytest
from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from airflow.providers.amazon.aws.triggers.ssm import SsmRunCommandTrigger
from airflow.providers.common.compat.sdk import AirflowException
from airflow.triggers.base import TriggerEvent

from unit.amazon.aws.utils.test_waiter import assert_expected_waiter_type

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.ssm."
EXPECTED_WAITER_NAME = "command_executed"
COMMAND_ID = "123e4567-e89b-12d3-a456-426614174000"
INSTANCE_ID_1 = "i-1234567890abcdef0"
INSTANCE_ID_2 = "i-1234567890abcdef1"


@pytest.fixture
def mock_ssm_list_invocations():
    def _setup(mock_get_async_conn):
        mock_client = mock.MagicMock()
        mock_get_async_conn.return_value.__aenter__.return_value = mock_client
        mock_client.list_command_invocations = mock.AsyncMock(
            return_value={
                "CommandInvocations": [
                    {"CommandId": COMMAND_ID, "InstanceId": INSTANCE_ID_1},
                    {"CommandId": COMMAND_ID, "InstanceId": INSTANCE_ID_2},
                ]
            }
        )
        return mock_client

    return _setup


class TestSsmRunCommandTrigger:
    def test_serialization(self):
        trigger = SsmRunCommandTrigger(command_id=COMMAND_ID)
        classpath, kwargs = trigger.serialize()

        assert classpath == BASE_TRIGGER_CLASSPATH + "SsmRunCommandTrigger"
        assert kwargs.get("command_id") == COMMAND_ID

    def test_serialization_with_region(self):
        """Test that region_name and other AWS parameters are properly serialized."""
        trigger = SsmRunCommandTrigger(
            command_id=COMMAND_ID,
            region_name="us-east-1",
            aws_conn_id="test_conn",
            verify=True,
            botocore_config={"retries": {"max_attempts": 3}},
        )
        classpath, kwargs = trigger.serialize()

        assert classpath == BASE_TRIGGER_CLASSPATH + "SsmRunCommandTrigger"
        assert kwargs.get("command_id") == COMMAND_ID
        assert kwargs.get("region_name") == "us-east-1"
        assert kwargs.get("aws_conn_id") == "test_conn"
        assert kwargs.get("verify") is True
        assert kwargs.get("botocore_config") == {"retries": {"max_attempts": 3}}

    @pytest.mark.asyncio
    @mock.patch.object(SsmHook, "get_async_conn")
    @mock.patch.object(SsmHook, "get_waiter")
    async def test_run_success(self, mock_get_waiter, mock_get_async_conn, mock_ssm_list_invocations):
        mock_client = mock_ssm_list_invocations(mock_get_async_conn)
        mock_get_waiter().wait = mock.AsyncMock(name="wait")

        trigger = SsmRunCommandTrigger(command_id=COMMAND_ID)
        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "command_id": COMMAND_ID})
        assert_expected_waiter_type(mock_get_waiter, EXPECTED_WAITER_NAME)
        assert mock_get_waiter().wait.call_count == 2
        mock_get_waiter().wait.assert_any_call(
            CommandId=COMMAND_ID, InstanceId=INSTANCE_ID_1, WaiterConfig={"MaxAttempts": 1}
        )
        mock_get_waiter().wait.assert_any_call(
            CommandId=COMMAND_ID, InstanceId=INSTANCE_ID_2, WaiterConfig={"MaxAttempts": 1}
        )
        mock_client.list_command_invocations.assert_called_once_with(CommandId=COMMAND_ID)

    @pytest.mark.asyncio
    @mock.patch.object(SsmHook, "get_async_conn")
    @mock.patch.object(SsmHook, "get_waiter")
    async def test_run_fails(self, mock_get_waiter, mock_get_async_conn, mock_ssm_list_invocations):
        mock_ssm_list_invocations(mock_get_async_conn)
        mock_get_waiter().wait.side_effect = WaiterError(
            "name", "terminal failure", {"CommandInvocations": [{"CommandId": COMMAND_ID}]}
        )

        trigger = SsmRunCommandTrigger(command_id=COMMAND_ID)
        generator = trigger.run()

        with pytest.raises(AirflowException):
            await generator.asend(None)
