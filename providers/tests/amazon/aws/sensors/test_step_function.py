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

import json
from unittest import mock

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


@pytest.fixture
def mocked_context():
    return mock.MagicMock(name="FakeContext")


class TestStepFunctionExecutionSensor:
    def test_init(self):
        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID,
            execution_arn=EXECUTION_ARN,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert sensor.execution_arn == EXECUTION_ARN
        assert sensor.hook.aws_conn_id == AWS_CONN_ID
        assert sensor.hook._region_name == REGION_NAME
        assert sensor.hook._verify is True
        assert sensor.hook._config is not None
        assert sensor.hook._config.read_timeout == 42

        sensor = StepFunctionExecutionSensor(task_id=TASK_ID, execution_arn=EXECUTION_ARN)
        assert sensor.hook.aws_conn_id == "aws_default"
        assert sensor.hook._region_name is None
        assert sensor.hook._verify is None
        assert sensor.hook._config is None

    @mock.patch.object(StepFunctionExecutionSensor, "hook")
    @pytest.mark.parametrize("status", StepFunctionExecutionSensor.INTERMEDIATE_STATES)
    def test_running(self, mocked_hook, status, mocked_context):
        mocked_hook.describe_execution.return_value = {"status": status}
        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=None
        )
        assert sensor.poke(mocked_context) is False

    @mock.patch.object(StepFunctionExecutionSensor, "hook")
    @pytest.mark.parametrize("status", StepFunctionExecutionSensor.SUCCESS_STATES)
    def test_succeeded(self, mocked_hook, status, mocked_context):
        mocked_hook.describe_execution.return_value = {"status": status}
        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=None
        )
        assert sensor.poke(mocked_context) is True

    @mock.patch.object(StepFunctionExecutionSensor, "hook")
    @pytest.mark.parametrize("status", StepFunctionExecutionSensor.FAILURE_STATES)
    def test_failure(self, mocked_hook, status, mocked_context):
        output = {"test": "test"}
        mocked_hook.describe_execution.return_value = {
            "status": status,
            "output": json.dumps(output),
        }
        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=None
        )
        message = f"Step Function sensor failed. State Machine Output: {output}"
        with pytest.raises(AirflowException, match=message):
            sensor.poke(mocked_context)
