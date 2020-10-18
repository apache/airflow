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

import unittest
from unittest import mock
from unittest.mock import MagicMock

from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.step_function_execution import StepFunctionExecutionSensor

TASK_ID = 'step_function_execution_sensor'
EXECUTION_ARN = (
    'arn:aws:states:us-east-1:123456789012:execution:'
    'pseudo-state-machine:020f5b16-b1a1-4149-946f-92dd32d97934'
)
AWS_CONN_ID = 'aws_non_default'
REGION_NAME = 'us-west-2'


class TestStepFunctionExecutionSensor(unittest.TestCase):
    def setUp(self):
        self.mock_context = MagicMock()

    def test_init(self):
        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME
        )

        self.assertEqual(TASK_ID, sensor.task_id)
        self.assertEqual(EXECUTION_ARN, sensor.execution_arn)
        self.assertEqual(AWS_CONN_ID, sensor.aws_conn_id)
        self.assertEqual(REGION_NAME, sensor.region_name)

    @parameterized.expand([('FAILED',), ('TIMED_OUT',), ('ABORTED',)])
    @mock.patch('airflow.providers.amazon.aws.sensors.step_function_execution.StepFunctionHook')
    def test_exceptions(self, mock_status, mock_hook):
        hook_response = {'status': mock_status}

        hook_instance = mock_hook.return_value
        hook_instance.describe_execution.return_value = hook_response

        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME
        )

        with self.assertRaises(AirflowException):
            sensor.poke(self.mock_context)

    @mock.patch('airflow.providers.amazon.aws.sensors.step_function_execution.StepFunctionHook')
    def test_running(self, mock_hook):
        hook_response = {'status': 'RUNNING'}

        hook_instance = mock_hook.return_value
        hook_instance.describe_execution.return_value = hook_response

        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME
        )

        self.assertFalse(sensor.poke(self.mock_context))

    @mock.patch('airflow.providers.amazon.aws.sensors.step_function_execution.StepFunctionHook')
    def test_succeeded(self, mock_hook):
        hook_response = {'status': 'SUCCEEDED'}

        hook_instance = mock_hook.return_value
        hook_instance.describe_execution.return_value = hook_response

        sensor = StepFunctionExecutionSensor(
            task_id=TASK_ID, execution_arn=EXECUTION_ARN, aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME
        )

        self.assertTrue(sensor.poke(self.mock_context))
