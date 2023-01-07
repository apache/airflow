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
from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor

TASK_ID = "step_function_execution_sensor"
EXECUTION_ARN = (
    "arn:aws:states:us-east-1:123456789012:execution:"
    "pseudo-state-machine:020f5b16-b1a1-4149-946f-92dd32d97934"
)
AWS_CONN_ID = "aws_non_default"
REGION_NAME = "us-west-2"


class TestStepFunctionExecutionSensor:
    def setup_method(self):
        self.mock_context = MagicMock()

    def test_init(self):
        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME
        )

        assert TASK_ID == sensor.task_id
        assert EXECUTION_ARN == sensor.execution_arn
        assert AWS_CONN_ID == sensor.aws_conn_id
        assert REGION_NAME == sensor.region_name

    @pytest.mark.parametrize("mock_status", ["FAILED", "TIMED_OUT", "ABORTED"])
    @mock.patch("airflow.providers.amazon.aws.sensors.step_function.StepFunctionHook")
    def test_exceptions(self, mock_hook, mock_status):
        hook_response = {"status": mock_status}

        hook_instance = mock_hook.return_value
        hook_instance.describe_execution.return_value = hook_response

        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME
        )

        with pytest.raises(AirflowException):
            sensor.poke(self.mock_context)

    @mock.patch("airflow.providers.amazon.aws.sensors.step_function.StepFunctionHook")
    def test_running(self, mock_hook):
        hook_response = {"status": "RUNNING"}

        hook_instance = mock_hook.return_value
        hook_instance.describe_execution.return_value = hook_response

        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME
        )

        assert not sensor.poke(self.mock_context)

    @mock.patch("airflow.providers.amazon.aws.sensors.step_function.StepFunctionHook")
    def test_succeeded(self, mock_hook):
        hook_response = {"status": "SUCCEEDED"}

        hook_instance = mock_hook.return_value
        hook_instance.describe_execution.return_value = hook_response

        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME
        )

        assert sensor.poke(self.mock_context)
