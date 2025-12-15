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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.amazon.aws.sensors.sagemaker_unified_studio import (
    SageMakerNotebookSensor,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.context import Context


class TestSageMakerNotebookSensor:
    def test_init(self):
        # Test the initialization of the sensor
        sensor = SageMakerNotebookSensor(
            task_id="test_task",
            execution_id="test_execution_id",
            execution_name="test_execution_name",
        )
        assert sensor.execution_id == "test_execution_id"
        assert sensor.execution_name == "test_execution_name"
        assert sensor.success_state == ["COMPLETED"]
        assert sensor.in_progress_states == ["PENDING", "RUNNING"]

    @patch("airflow.providers.amazon.aws.sensors.sagemaker_unified_studio.SageMakerNotebookHook")
    def test_poke_success_state(self, mock_notebook_hook):
        mock_hook_instance = mock_notebook_hook.return_value
        mock_hook_instance.get_execution_status.return_value = "COMPLETED"

        sensor = SageMakerNotebookSensor(
            task_id="test_task",
            execution_id="test_execution_id",
            execution_name="test_execution_name",
        )

        # Test the poke method
        result = sensor.poke()
        assert result is True
        mock_hook_instance.get_execution_status.assert_called_once_with(execution_id="test_execution_id")

    @patch("airflow.providers.amazon.aws.sensors.sagemaker_unified_studio.SageMakerNotebookHook")
    def test_poke_failure_state(self, mock_notebook_hook):
        mock_hook_instance = mock_notebook_hook.return_value
        mock_hook_instance.get_execution_status.return_value = "FAILED"

        sensor = SageMakerNotebookSensor(
            task_id="test_task",
            execution_id="test_execution_id",
            execution_name="test_execution_name",
        )

        # Test the poke method and assert exception
        with pytest.raises(AirflowException, match="Exiting Execution test_execution_id State: FAILED"):
            sensor.poke()

        mock_hook_instance.get_execution_status.assert_called_once_with(execution_id="test_execution_id")

    @patch("airflow.providers.amazon.aws.sensors.sagemaker_unified_studio.SageMakerNotebookHook")
    def test_poke_in_progress_state(self, mock_notebook_hook):
        mock_hook_instance = mock_notebook_hook.return_value
        mock_hook_instance.get_execution_status.return_value = "RUNNING"

        sensor = SageMakerNotebookSensor(
            task_id="test_task",
            execution_id="test_execution_id",
            execution_name="test_execution_name",
        )

        # Test the poke method
        result = sensor.poke()
        assert result is False
        mock_hook_instance.get_execution_status.assert_called_once_with(execution_id="test_execution_id")

    @patch.object(SageMakerNotebookSensor, "poke", return_value=True)
    def test_execute_calls_poke(self, mock_poke):
        # Create the sensor
        sensor = SageMakerNotebookSensor(
            task_id="test_task",
            execution_id="test_execution_id",
            execution_name="test_execution_name",
        )

        context = MagicMock(spec=Context)
        sensor.execute(context=context)

        # Assert that the poke method was called
        mock_poke.assert_called_once_with(context)
