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

from airflow.providers.google.cloud.sensors.cloud_composer import CloudComposerDAGRunSensor

TEST_PROJECT_ID = "test_project_id"
TEST_OPERATION_NAME = "test_operation_name"
TEST_REGION = "region"
TEST_ENVIRONMENT_ID = "test_env_id"
TEST_JSON_RESULT = lambda state, date_key: json.dumps(
    [
        {
            "dag_id": "test_dag_id",
            "run_id": "scheduled__2024-05-22T11:10:00+00:00",
            "state": state,
            date_key: "2024-05-22T11:10:00+00:00",
            "start_date": "2024-05-22T11:20:01.531988+00:00",
            "end_date": "2024-05-22T11:20:11.997479+00:00",
        }
    ]
)
TEST_EXEC_RESULT = lambda state, date_key: {
    "output": [{"line_number": 1, "content": TEST_JSON_RESULT(state, date_key)}],
    "output_end": True,
    "exit_info": {"exit_code": 0, "error": ""},
}


class TestCloudComposerDAGRunSensor:
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.ExecuteAirflowCommandResponse.to_dict")
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_wait_ready(self, mock_hook, to_dict_mode, composer_airflow_version):
        mock_hook.return_value.wait_command_execution_result.return_value = TEST_EXEC_RESULT(
            "success", "execution_date" if composer_airflow_version < 3 else "logical_date"
        )

        task = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.ExecuteAirflowCommandResponse.to_dict")
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_wait_not_ready(self, mock_hook, to_dict_mode, composer_airflow_version):
        mock_hook.return_value.wait_command_execution_result.return_value = TEST_EXEC_RESULT(
            "running", "execution_date" if composer_airflow_version < 3 else "logical_date"
        )

        task = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})
