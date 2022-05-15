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
#

import unittest
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks import RunState
from airflow.providers.databricks.sensors.databricks import DatabricksJobRunSensor

RUN_ID = 1
TASK_ID = 'databricks-sensor'
RUN_PAGE_URL = 'workspace.databricks.com/jobs/1'
DEFAULT_CONN_ID = 'databricks_default'


class TestDatabricksJobRunSensor(unittest.TestCase):
    @mock.patch('airflow.providers.databricks.sensors.databricks.DatabricksHook')
    def test_terminal_state_success(self, db_mock_class):
        """Test DatabricksJobRunSensor in a successful terminal state"""
        sensor = DatabricksJobRunSensor(task_id=TASK_ID, run_id=RUN_ID)
        db_mock = db_mock_class.return_value
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'SUCCESS', '')

        with self.assertLogs(sensor.log) as logger:
            sensor.execute(None)
            self.assertIn(
                f'INFO:airflow.task.operators:{TASK_ID} completed successfully.',
                logger.output,
            )

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=sensor.retry_limit,
            retry_delay=sensor.retry_delay_seconds,
            retry_args=sensor.databricks_retry_args,
        )
        db_mock.get_run_state.assert_called_once_with(RUN_ID)

    @mock.patch('airflow.providers.databricks.sensors.databricks.DatabricksHook')
    def test_terminal_state_error(self, db_mock_class):
        """Test DatabricksJobRunSensor in a failed terminal state"""
        sensor = DatabricksJobRunSensor(task_id=TASK_ID, run_id=RUN_ID)
        db_mock = db_mock_class.return_value
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'INTERNAL_ERROR', '')
        db_mock.get_run_output.return_value = {'error': 'internal error'}

        exception_message = (
            f'{TASK_ID} failed with terminal state: {db_mock.get_run_state.return_value} '
            f'and with the error: internal error'
        )
        with pytest.raises(AirflowException, match=exception_message):
            sensor.execute(None)

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=sensor.retry_limit,
            retry_delay=sensor.retry_delay_seconds,
            retry_args=sensor.databricks_retry_args,
        )
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        db_mock.get_run_output.assert_called_once_with(RUN_ID)

    @mock.patch('airflow.providers.databricks.sensors.databricks.DatabricksHook')
    def test_non_terminal_state(self, db_mock_class):
        """Test DatabricksJobRunSensor in a non terminal state"""
        sensor = DatabricksJobRunSensor(task_id=TASK_ID, run_id=RUN_ID)
        db_mock = db_mock_class.return_value
        db_mock.get_run_state.return_value = RunState('RUNNING', '', '')
        db_mock.get_run_page_url.return_value = RUN_PAGE_URL

        with self.assertLogs(sensor.log) as logger:
            sensor.poke(None)
            self.assertListEqual(
                [
                    f'INFO:airflow.task.operators:Task {TASK_ID} is in state: '
                    f'{db_mock.get_run_state.return_value}, '
                    f'Spark UI and logs are available at {RUN_PAGE_URL}',
                    f'INFO:airflow.task.operators:Waiting for run {RUN_ID} to complete.',
                ],
                logger.output,
            )

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=sensor.retry_limit,
            retry_delay=sensor.retry_delay_seconds,
            retry_args=sensor.databricks_retry_args,
        )
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
