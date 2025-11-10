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
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.triggers.step_function import StepFunctionsExecutionCompleteTrigger
from airflow.triggers.base import TriggerEvent


class TestStepFunctionsExecutionCompleteTrigger:
    def setup_method(self):
        self.execution_arn = (
            "arn:aws:states:us-east-1:123456789012:execution:test-state-machine:test-execution"
        )
        self.trigger = StepFunctionsExecutionCompleteTrigger(
            execution_arn=self.execution_arn,
            waiter_delay=1,
            waiter_max_attempts=2,
            aws_conn_id="aws_default",
            region_name="us-east-1",
        )

    def test_serialized_fields(self):
        """Test that the trigger correctly serializes its fields."""
        classpath, kwargs = self.trigger.serialize()
        assert (
            classpath
            == "airflow.providers.amazon.aws.triggers.step_function.StepFunctionsExecutionCompleteTrigger"
        )
        assert kwargs["execution_arn"] == self.execution_arn
        assert kwargs["region_name"] == "us-east-1"
        assert kwargs["waiter_delay"] == 1
        assert kwargs["waiter_max_attempts"] == 2
        assert kwargs["aws_conn_id"] == "aws_default"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.triggers.base.async_wait")
    async def test_run_success(self, mock_async_wait):
        """Test that the trigger yields a success event when the waiter completes."""
        mock_async_wait.return_value = None

        generator = self.trigger.run()
        result = await generator.asend(None)

        mock_async_wait.assert_called_once()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "success", "execution_arn": self.execution_arn}

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.triggers.base.async_wait")
    async def test_run_exception(self, mock_async_wait):
        """Test that the trigger yields an error event when the waiter raises an exception."""
        mock_async_wait.side_effect = AirflowException("Waiter error")

        generator = self.trigger.run()

        with pytest.raises(AirflowException):
            await generator.asend(None)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.step_function.StepFunctionHook.get_async_conn")
    @mock.patch("airflow.providers.amazon.aws.hooks.step_function.StepFunctionHook.get_waiter")
    async def test_run_waiter_error(self, mock_get_waiter, mock_get_async_conn):
        """Test that the trigger handles WaiterError correctly."""
        # Mock the async connection
        mock_client = AsyncMock()
        mock_get_async_conn.return_value.__aenter__.return_value = mock_client

        # Mock the waiter to raise a WaiterError
        mock_waiter = MagicMock()
        mock_get_waiter.return_value = mock_waiter
        mock_waiter.wait.side_effect = WaiterError(
            name="step_function_succeeded", reason="terminal failure", last_response={"status": "FAILED"}
        )

        # Mock the hook method
        with mock.patch.object(self.trigger, "hook") as mock_hook:
            mock_hook.return_value.get_async_conn = mock_get_async_conn
            mock_hook.return_value.get_waiter = mock_get_waiter

            # Create a new trigger for this test to avoid side effects
            trigger = StepFunctionsExecutionCompleteTrigger(
                execution_arn=self.execution_arn,
                waiter_delay=1,
                waiter_max_attempts=1,
                aws_conn_id="aws_default",
            )

            generator = trigger.run()

            with pytest.raises(AirflowException):
                await generator.asend(None)
