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

from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.providers.amazon.aws.triggers.step_function import StepFunctionsExecutionCompleteTrigger
from airflow.triggers.base import TriggerEvent

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.step_function."


class TestStepFunctionsExecutionCompleteTrigger:
    EXPECTED_WAITER_NAME = "step_function_succeeded"
    EXECUTION_ARN = (
        "arn:aws:states:us-east-1:123456789012:execution:"
        "pseudo-state-machine:020f5b16-b1a1-4149-946f-92dd32d97934"
    )

    def test_serialization(self):
        trigger = StepFunctionsExecutionCompleteTrigger(
            execution_arn=self.EXECUTION_ARN,
            waiter_delay=10,
            waiter_max_attempts=5,
            aws_conn_id="aws_step_function_conn",
            region_name="eu-central-1",
        )

        classpath, kwargs = trigger.serialize()

        assert classpath == BASE_TRIGGER_CLASSPATH + "StepFunctionsExecutionCompleteTrigger"
        assert kwargs.get("execution_arn") == self.EXECUTION_ARN
        assert kwargs.get("waiter_delay") == 10
        assert kwargs.get("waiter_max_attempts") == 5
        assert kwargs.get("aws_conn_id") == "aws_step_function_conn"
        assert kwargs.get("region_name") == "eu-central-1"

    def test_hook_forwards_connection_config(self):
        trigger = StepFunctionsExecutionCompleteTrigger(
            execution_arn=self.EXECUTION_ARN,
            aws_conn_id="aws_step_function_conn",
            region_name="eu-central-1",
            verify=False,
            botocore_config={"read_timeout": 100},
        )

        hook = trigger.hook()

        assert isinstance(hook, StepFunctionHook)
        assert hook.aws_conn_id == "aws_step_function_conn"
        assert hook._region_name == "eu-central-1"
        assert hook._verify is False
        assert hook._config.read_timeout == 100

    @pytest.mark.asyncio
    @mock.patch.object(StepFunctionHook, "get_waiter")
    @mock.patch.object(StepFunctionHook, "get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = StepFunctionsExecutionCompleteTrigger(execution_arn=self.EXECUTION_ARN)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "execution_arn": self.EXECUTION_ARN})
        assert mock_get_waiter().wait.call_count == 1
        mock_get_waiter.assert_any_call(
            self.EXPECTED_WAITER_NAME, deferrable=True, client=mock.ANY, config_overrides=None
        )
        assert mock_get_waiter().wait.call_args.kwargs["executionArn"] == self.EXECUTION_ARN
