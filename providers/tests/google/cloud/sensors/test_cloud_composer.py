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
from datetime import datetime
from unittest import mock

import pytest

from airflow.exceptions import (
    AirflowException,
    AirflowProviderDeprecationWarning,
    TaskDeferred,
)
from airflow.providers.google.cloud.sensors.cloud_composer import (
    CloudComposerDAGRunSensor,
    CloudComposerEnvironmentSensor,
)
from airflow.providers.google.cloud.triggers.cloud_composer import (
    CloudComposerExecutionTrigger,
)

TEST_PROJECT_ID = "test_project_id"
TEST_OPERATION_NAME = "test_operation_name"
TEST_REGION = "region"
TEST_ENVIRONMENT_ID = "test_env_id"
TEST_JSON_RESULT = lambda state: json.dumps(
    [
        {
            "dag_id": "test_dag_id",
            "run_id": "scheduled__2024-05-22T11:10:00+00:00",
            "state": state,
            "execution_date": "2024-05-22T11:10:00+00:00",
            "start_date": "2024-05-22T11:20:01.531988+00:00",
            "end_date": "2024-05-22T11:20:11.997479+00:00",
        }
    ]
)
TEST_EXEC_RESULT = lambda state: {
    "output": [{"line_number": 1, "content": TEST_JSON_RESULT(state)}],
    "output_end": True,
    "exit_info": {"exit_code": 0, "error": ""},
}


class TestCloudComposerEnvironmentSensor:
    @pytest.mark.db_test
    def test_cloud_composer_existence_sensor_async(self):
        """
        Asserts that a task is deferred and a CloudComposerExecutionTrigger will be fired
        when the CloudComposerEnvironmentSensor is executed.
        """
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerEnvironmentSensor(
                task_id="task_id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                operation_name=TEST_OPERATION_NAME,
            )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context={})
        assert isinstance(
            exc.value.trigger, CloudComposerExecutionTrigger
        ), "Trigger is not a CloudComposerExecutionTrigger"

    def test_cloud_composer_existence_sensor_async_execute_failure(
        self,
    ):
        """Tests that an expected exception is raised in case of error event."""
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerEnvironmentSensor(
                task_id="task_id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                operation_name=TEST_OPERATION_NAME,
            )
        with pytest.raises(AirflowException, match="No event received in trigger callback"):
            task.execute_complete(context={}, event=None)

    def test_cloud_composer_existence_sensor_async_execute_complete(self):
        """Asserts that logging occurs as expected"""
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerEnvironmentSensor(
                task_id="task_id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                operation_name=TEST_OPERATION_NAME,
            )
        with mock.patch.object(task.log, "info"):
            task.execute_complete(
                context={}, event={"operation_done": True, "operation_name": TEST_OPERATION_NAME}
            )


class TestCloudComposerDAGRunSensor:
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.ExecuteAirflowCommandResponse.to_dict")
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_wait_ready(self, mock_hook, to_dict_mode):
        mock_hook.return_value.wait_command_execution_result.return_value = TEST_EXEC_RESULT("success")

        task = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            allowed_states=["success"],
        )

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.ExecuteAirflowCommandResponse.to_dict")
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_wait_not_ready(self, mock_hook, to_dict_mode):
        mock_hook.return_value.wait_command_execution_result.return_value = TEST_EXEC_RESULT("running")

        task = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            allowed_states=["success"],
        )

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})
