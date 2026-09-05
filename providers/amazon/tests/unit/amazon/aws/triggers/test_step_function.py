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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.providers.amazon.aws.triggers.step_function import StepFunctionsExecutionCompleteTrigger
from airflow.triggers.base import TriggerEvent

EXECUTION_ARN = "arn:aws:states:us-east-1:123456789012:execution:test-state-machine:test-execution"
TRIGGER_CLASSPATH = (
    "airflow.providers.amazon.aws.triggers.step_function.StepFunctionsExecutionCompleteTrigger"
)


class TestStepFunctionsExecutionCompleteTrigger:
    def test_serialization(self):
        trigger = StepFunctionsExecutionCompleteTrigger(
            execution_arn=EXECUTION_ARN,
            waiter_delay=10,
            waiter_max_attempts=5,
            aws_conn_id="aws_default",
            region_name="us-west-2",
        )

        classpath, kwargs = trigger.serialize()

        assert classpath == TRIGGER_CLASSPATH
        assert kwargs == {
            "execution_arn": EXECUTION_ARN,
            "waiter_delay": 10,
            "waiter_max_attempts": 5,
            "aws_conn_id": "aws_default",
            "region_name": "us-west-2",
        }

    def test_serialization_with_verify_and_botocore_config(self):
        trigger = StepFunctionsExecutionCompleteTrigger(
            execution_arn=EXECUTION_ARN,
            aws_conn_id="aws_default",
            region_name="us-west-2",
            verify=False,
            botocore_config={"connect_timeout": 30},
        )

        classpath, kwargs = trigger.serialize()

        assert classpath == TRIGGER_CLASSPATH
        assert kwargs["verify"] is False
        assert kwargs["botocore_config"] == {"connect_timeout": 30}

    @mock.patch("airflow.providers.amazon.aws.triggers.step_function.StepFunctionHook")
    def test_hook_propagates_verify_and_botocore_config(self, mock_hook_cls):
        trigger = StepFunctionsExecutionCompleteTrigger(
            execution_arn=EXECUTION_ARN,
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify="/path/to/ca-bundle.crt",
            botocore_config={"read_timeout": 60},
        )

        trigger.hook()

        mock_hook_cls.assert_called_once_with(
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify="/path/to/ca-bundle.crt",
            config={"read_timeout": 60},
        )

    @pytest.mark.asyncio
    @mock.patch.object(StepFunctionHook, "get_waiter")
    @mock.patch.object(StepFunctionHook, "get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = StepFunctionsExecutionCompleteTrigger(execution_arn=EXECUTION_ARN)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "execution_arn": EXECUTION_ARN})
        assert mock_get_waiter().wait.call_count == 1
        mock_get_waiter().wait.assert_called_once_with(
            executionArn=EXECUTION_ARN, WaiterConfig={"MaxAttempts": 1}
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.triggers.base.async_wait")
    @mock.patch.object(StepFunctionHook, "get_waiter")
    @mock.patch.object(StepFunctionHook, "get_async_conn")
    async def test_run_failure(self, mock_async_conn, mock_get_waiter, mock_async_wait):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_async_wait.side_effect = AirflowException("Step function failed")
        trigger = StepFunctionsExecutionCompleteTrigger(execution_arn=EXECUTION_ARN)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "error", "message": "Step function failed", "execution_arn": EXECUTION_ARN}
        )
