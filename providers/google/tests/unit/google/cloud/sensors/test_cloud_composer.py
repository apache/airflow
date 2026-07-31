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
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models.dag import DAG
from airflow.providers.google.cloud.sensors.cloud_composer import (
    CloudComposerDAGRunSensor,
    CloudComposerExternalTaskSensor,
)
from airflow.providers.google.cloud.triggers.cloud_composer import CloudComposerDAGRunTrigger
from airflow.providers.google.cloud.utils.composer import (
    check_dag_run_states_in_window,
    composer_dag_run_date_field,
    is_in_execution_window,
    parse_composer_airflow_datetime,
)

TEST_PROJECT_ID = "test_project_id"
TEST_OPERATION_NAME = "test_operation_name"
TEST_REGION = "region"
TEST_ENVIRONMENT_ID = "test_env_id"
TEST_COMPOSER_DAG_RUN_ID = "scheduled__2024-05-22T11:10:00+00:00"
TEST_DAG_RUNS_RESULT = lambda state, date_key, run_id_key: [
    {
        "dag_id": "test_dag_id",
        run_id_key: TEST_COMPOSER_DAG_RUN_ID,
        "state": state,
        date_key: "2024-05-22T11:10:00+00:00",
        "start_date": "2024-05-22T11:20:01.531988+00:00",
        "end_date": "2024-05-22T11:20:11.997479+00:00",
    }
]
TEST_EXEC_RESULT = lambda state, date_key: {
    "output": [{"line_number": 1, "content": json.dumps(TEST_DAG_RUNS_RESULT(state, date_key, "run_id"))}],
    "output_end": True,
    "exit_info": {"exit_code": 0, "error": ""},
}
TEST_GET_RESULT = lambda state, date_key: {
    "dag_runs": TEST_DAG_RUNS_RESULT(state, date_key, "dag_run_id"),
    "total_entries": 1,
}
TEST_COMPOSER_EXTERNAL_TASK_ID = "test_external_task_id"
TEST_COMPOSER_EXTERNAL_TASK_GROUP_ID = "test_external_task_group_id"
TEST_TASK_INSTANCES_RESULT = lambda state, date_key, task_id: [
    {
        "task_id": task_id,
        "dag_id": "test_dag_id",
        "state": state,
        date_key: "2024-05-22T11:10:00+00:00",
        "start_date": "2024-05-22T11:20:01.531988+00:00",
        "end_date": "2024-05-22T11:20:11.997479+00:00",
    }
]
TEST_GET_TASK_INSTANCES_RESULT = lambda state, date_key, task_id: {
    "task_instances": TEST_TASK_INSTANCES_RESULT(state, date_key, task_id),
    "total_entries": 1,
}


def build_dag_runs_result(composer_airflow_version: int, dag_runs: list[tuple[str, str]]) -> dict:
    date_key = "execution_date" if composer_airflow_version < 3 else "logical_date"
    return {
        "dag_runs": [
            {
                "dag_id": "test_dag_id",
                "dag_run_id": f"scheduled__{index}",
                "state": state,
                date_key: logical_date,
            }
            for index, (state, logical_date) in enumerate(dag_runs)
        ],
        "total_entries": len(dag_runs),
    }


class TestCloudComposerDAGRunSensor:
    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_wait_ready(self, mock_hook, composer_airflow_version, use_rest_api):
        mock_hook.return_value.wait_command_execution_result.return_value = TEST_EXEC_RESULT(
            "success", "execution_date" if composer_airflow_version < 3 else "logical_date"
        )
        mock_hook.return_value.get_dag_runs.return_value = TEST_GET_RESULT(
            "success", "execution_date" if composer_airflow_version < 3 else "logical_date"
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                allowed_states=["success"],
                use_rest_api=use_rest_api,
            )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_wait_not_ready(self, mock_hook, composer_airflow_version, use_rest_api):
        mock_hook.return_value.wait_command_execution_result.return_value = TEST_EXEC_RESULT(
            "running", "execution_date" if composer_airflow_version < 3 else "logical_date"
        )
        mock_hook.return_value.get_dag_runs.return_value = TEST_GET_RESULT(
            "running", "execution_date" if composer_airflow_version < 3 else "logical_date"
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                allowed_states=["success"],
                use_rest_api=use_rest_api,
            )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_dag_runs_empty(self, mock_hook, composer_airflow_version, use_rest_api):
        mock_hook.return_value.wait_command_execution_result.return_value = {
            "output": [{"line_number": 1, "content": json.dumps([])}],
            "output_end": True,
            "exit_info": {"exit_code": 0, "error": ""},
        }
        mock_hook.return_value.get_dag_runs.return_value = {
            "dag_runs": [],
            "total_entries": 0,
        }
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                allowed_states=["success"],
                use_rest_api=use_rest_api,
            )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_poke_returns_false_when_all_runs_outside_window(
        self, mock_hook, composer_airflow_version, use_rest_api
    ):
        mock_hook.return_value.get_dag_runs.return_value = build_dag_runs_result(
            composer_airflow_version,
            [("success", "2024-05-24T11:10:00+00:00")],
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                allowed_states=["success"],
                use_rest_api=use_rest_api,
            )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_poke_returns_true_when_at_least_one_in_window_and_allowed(
        self, mock_hook, composer_airflow_version, use_rest_api
    ):
        mock_hook.return_value.get_dag_runs.return_value = build_dag_runs_result(
            composer_airflow_version,
            [
                ("failed", "2024-05-24T11:10:00+00:00"),
                ("success", "2024-05-22T11:10:00+00:00"),
            ],
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                allowed_states=["success"],
                use_rest_api=use_rest_api,
            )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_poke_returns_false_when_in_window_run_not_allowed(
        self, mock_hook, composer_airflow_version, use_rest_api
    ):
        mock_hook.return_value.get_dag_runs.return_value = build_dag_runs_result(
            composer_airflow_version,
            [
                ("success", "2024-05-24T11:10:00+00:00"),
                ("failed", "2024-05-22T11:10:00+00:00"),
            ],
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                allowed_states=["success"],
                use_rest_api=use_rest_api,
            )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_poke_includes_run_exactly_at_start_boundary(
        self, mock_hook, composer_airflow_version, use_rest_api
    ):
        """Window is [start, end); schedule-aligned runs at start_date succeed."""
        mock_hook.return_value.get_dag_runs.return_value = build_dag_runs_result(
            composer_airflow_version,
            [
                ("success", "2024-05-22T00:00:00+00:00"),  # == start (default range: end-1d)
            ],
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                allowed_states=["success"],
                use_rest_api=use_rest_api,
            )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_poke_excludes_run_exactly_at_end_boundary(
        self, mock_hook, composer_airflow_version, use_rest_api
    ):
        """Window is [start, end); runs exactly at end_date keep waiting."""
        mock_hook.return_value.get_dag_runs.return_value = build_dag_runs_result(
            composer_airflow_version,
            [
                ("success", "2024-05-23T00:00:00+00:00"),  # == end
            ],
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                allowed_states=["success"],
                use_rest_api=use_rest_api,
            )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_poke_skips_null_logical_or_execution_date(self, mock_hook, composer_airflow_version):
        """AF3 may emit null logical_date; those runs must not crash or count."""
        date_key = "execution_date" if composer_airflow_version < 3 else "logical_date"
        mock_hook.return_value.get_dag_runs.return_value = {
            "dag_runs": [
                {
                    "dag_id": "test_dag_id",
                    "dag_run_id": "manual__null-date",
                    "state": "success",
                    date_key: None,
                },
                {
                    "dag_id": "test_dag_id",
                    "dag_run_id": "scheduled__1",
                    "state": "success",
                    date_key: "2024-05-22T11:10:00+00:00",
                },
            ],
            "total_entries": 2,
        }
        task = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_poke_treats_naive_api_datetimes_as_utc(self, mock_hook, composer_airflow_version):
        """Naive Composer API timestamps are compared as UTC (not worker local tz)."""
        date_key = "execution_date" if composer_airflow_version < 3 else "logical_date"
        mock_hook.return_value.get_dag_runs.return_value = {
            "dag_runs": [
                {
                    "dag_id": "test_dag_id",
                    "dag_run_id": "scheduled__naive",
                    "state": "success",
                    # No timezone suffix — must still match the UTC window.
                    date_key: "2024-05-22T11:10:00",
                }
            ],
            "total_entries": 1,
        }
        task = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_empty_allowed_states_defaults_to_success(self, mock_hook, composer_airflow_version):
        """Empty allowed_states must not treat every in-window run as failure."""
        mock_hook.return_value.get_dag_runs.return_value = build_dag_runs_result(
            composer_airflow_version,
            [("success", "2024-05-22T11:10:00+00:00")],
        )
        task = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            allowed_states=[],
        )
        task._composer_airflow_version = composer_airflow_version

        assert task.allowed_states == ["success"]
        assert task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_composer_dag_run_id_wait_ready(self, mock_hook, composer_airflow_version, use_rest_api):
        mock_hook.return_value.wait_command_execution_result.return_value = TEST_EXEC_RESULT(
            "success", "execution_date" if composer_airflow_version < 3 else "logical_date"
        )
        mock_hook.return_value.get_dag_runs.return_value = TEST_GET_RESULT(
            "success", "execution_date" if composer_airflow_version < 3 else "logical_date"
        )

        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                composer_dag_run_id=TEST_COMPOSER_DAG_RUN_ID,
                allowed_states=["success"],
                use_rest_api=use_rest_api,
            )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_composer_dag_run_id_wait_not_ready(self, mock_hook, composer_airflow_version, use_rest_api):
        mock_hook.return_value.wait_command_execution_result.return_value = TEST_EXEC_RESULT(
            "running", "execution_date" if composer_airflow_version < 3 else "logical_date"
        )
        mock_hook.return_value.get_dag_runs.return_value = TEST_GET_RESULT(
            "running", "execution_date" if composer_airflow_version < 3 else "logical_date"
        )

        with pytest.warns(AirflowProviderDeprecationWarning):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                composer_dag_run_id=TEST_COMPOSER_DAG_RUN_ID,
                allowed_states=["success"],
                use_rest_api=use_rest_api,
            )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})


class TestCloudComposerExternalTaskSensor:
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_wait_ready(self, mock_hook, composer_airflow_version):
        mock_hook.return_value.get_task_instances.return_value = TEST_GET_TASK_INSTANCES_RESULT(
            "success",
            "execution_date" if composer_airflow_version < 3 else "logical_date",
            TEST_COMPOSER_EXTERNAL_TASK_ID,
        )

        task = CloudComposerExternalTaskSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_external_dag_id="test_dag_id",
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_wait_not_ready(self, mock_hook, composer_airflow_version):
        mock_hook.return_value.get_task_instances.return_value = TEST_GET_TASK_INSTANCES_RESULT(
            "running",
            "execution_date" if composer_airflow_version < 3 else "logical_date",
            TEST_COMPOSER_EXTERNAL_TASK_ID,
        )

        task = CloudComposerExternalTaskSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_external_dag_id="test_dag_id",
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_task_instances_empty(self, mock_hook, composer_airflow_version):
        mock_hook.return_value.get_task_instances.return_value = {
            "task_instances": [],
            "total_entries": 0,
        }

        task = CloudComposerExternalTaskSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_external_dag_id="test_dag_id",
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_composer_external_task_id_wait_ready(self, mock_hook, composer_airflow_version):
        mock_hook.return_value.get_task_instances.return_value = TEST_GET_TASK_INSTANCES_RESULT(
            "success",
            "execution_date" if composer_airflow_version < 3 else "logical_date",
            TEST_COMPOSER_EXTERNAL_TASK_ID,
        )

        task = CloudComposerExternalTaskSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_external_dag_id="test_dag_id",
            composer_external_task_id=TEST_COMPOSER_EXTERNAL_TASK_ID,
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_composer_external_task_id_wait_not_ready(self, mock_hook, composer_airflow_version):
        mock_hook.return_value.get_task_instances.return_value = TEST_GET_TASK_INSTANCES_RESULT(
            "running",
            "execution_date" if composer_airflow_version < 3 else "logical_date",
            TEST_COMPOSER_EXTERNAL_TASK_ID,
        )

        task = CloudComposerExternalTaskSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_external_dag_id="test_dag_id",
            composer_external_task_id=TEST_COMPOSER_EXTERNAL_TASK_ID,
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_composer_external_task_group_id_wait_ready(self, mock_hook, composer_airflow_version):
        mock_hook.return_value.get_task_instances.return_value = TEST_GET_TASK_INSTANCES_RESULT(
            "success",
            "execution_date" if composer_airflow_version < 3 else "logical_date",
            f"{TEST_COMPOSER_EXTERNAL_TASK_GROUP_ID}.{TEST_COMPOSER_EXTERNAL_TASK_ID}",
        )

        task = CloudComposerExternalTaskSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_external_dag_id="test_dag_id",
            composer_external_task_group_id=TEST_COMPOSER_EXTERNAL_TASK_GROUP_ID,
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_composer_external_task_group_id_wait_not_ready(self, mock_hook, composer_airflow_version):
        mock_hook.return_value.get_task_instances.return_value = TEST_GET_TASK_INSTANCES_RESULT(
            "running",
            "execution_date" if composer_airflow_version < 3 else "logical_date",
            f"{TEST_COMPOSER_EXTERNAL_TASK_GROUP_ID}.{TEST_COMPOSER_EXTERNAL_TASK_ID}",
        )

        task = CloudComposerExternalTaskSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_external_dag_id="test_dag_id",
            composer_external_task_group_id=TEST_COMPOSER_EXTERNAL_TASK_GROUP_ID,
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, 0, 0, 0)})


class TestCloudComposerDAGRunSensorComposerDagRunIdPath:
    """Tests covering the run-id-pinning path added/exercised in PR #67052.

    These tests do NOT inject the deprecated ``use_rest_api`` kwarg, so they
    must not be wrapped in ``pytest.warns(AirflowProviderDeprecationWarning)``.
    """

    def test_composer_dag_run_id_field_is_templated(self):
        assert "composer_dag_run_id" in CloudComposerDAGRunSensor.template_fields

    def test_composer_dag_run_id_renders_from_jinja_xcom_pull(self):
        """Jinja-render the templated field exactly the way the example Dag
        does (``{{ ti.xcom_pull(task_ids='trigger_dag_run')['dag_run_id'] }}``).
        Use a real class for ``ti`` -- the Jinja sandbox refuses to call
        Mock attributes (jinja2.SecurityError: Mock is not safely callable).
        """

        class _FakeTI:
            def __init__(self):
                self.calls: list[tuple[tuple, dict]] = []

            def xcom_pull(self, *args, **kwargs):
                self.calls.append((args, kwargs))
                return {"dag_run_id": "manual__abc"}

        with DAG("test_dag", schedule=None, start_date=datetime(2024, 1, 1)):
            task = CloudComposerDAGRunSensor(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                region=TEST_REGION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id="test_dag_id",
                composer_dag_run_id="{{ ti.xcom_pull(task_ids='upstream')['dag_run_id'] }}",
                allowed_states=["success"],
            )
        ti = _FakeTI()
        task.render_template_fields(context={"ti": ti})
        assert task.composer_dag_run_id == "manual__abc"
        assert ti.calls == [((), {"task_ids": "upstream"})]

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_poke_succeeds_when_composer_dag_run_id_matches_allowed_run(
        self, mock_hook, composer_airflow_version
    ):
        date_key = "execution_date" if composer_airflow_version < 3 else "logical_date"
        mock_hook.return_value.get_dag_runs.return_value = {
            "dag_runs": [
                {
                    "dag_id": "test_dag_id",
                    "dag_run_id": "manual__abc",
                    "state": "success",
                    date_key: "2024-05-22T11:10:00+00:00",
                }
            ],
            "total_entries": 1,
        }
        task = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            composer_dag_run_id="manual__abc",
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_poke_returns_false_when_composer_dag_run_id_not_yet_present(
        self, mock_hook, composer_airflow_version
    ):
        date_key = "execution_date" if composer_airflow_version < 3 else "logical_date"
        mock_hook.return_value.get_dag_runs.return_value = {
            "dag_runs": [
                {
                    "dag_id": "test_dag_id",
                    "dag_run_id": "scheduled__not-the-one",
                    "state": "success",
                    date_key: "2024-05-22T11:10:00+00:00",
                }
            ],
            "total_entries": 1,
        }
        task = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            composer_dag_run_id="manual__abc",
            allowed_states=["success"],
        )
        task._composer_airflow_version = composer_airflow_version

        assert not task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerHook")
    def test_composer_dag_run_id_ignores_execution_range_in_poke(self, mock_hook, composer_airflow_version):
        """When both composer_dag_run_id and execution_range are set, the
        run-id path wins (and the windowed branch is not consulted). Pin a
        run whose logical_date is *outside* the window and assert success.
        """
        date_key = "execution_date" if composer_airflow_version < 3 else "logical_date"
        mock_hook.return_value.get_dag_runs.return_value = {
            "dag_runs": [
                {
                    "dag_id": "test_dag_id",
                    "dag_run_id": "manual__abc",
                    "state": "success",
                    date_key: "2099-01-01T00:00:00+00:00",
                }
            ],
            "total_entries": 1,
        }
        task = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            composer_dag_run_id="manual__abc",
            allowed_states=["success"],
            execution_range=[
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
            ],
        )
        task._composer_airflow_version = composer_airflow_version

        assert task.poke(context={"logical_date": datetime(2024, 5, 23, tzinfo=timezone.utc)})

    def test_example_cloud_composer_pins_dag_run_sensors_to_trigger_xcom(self):
        """Source-level guard against re-introducing the empty-window bug.

        The system-test example used to set ``execution_range=[datetime.now() - timedelta(1),
        datetime.now()]`` on the Dag-run sensors, which was evaluated at parse time before the
        Composer env existed and caused PR #61046 to be reverted. The current version pins each
        sensor to its upstream trigger's run-id via XCom. This test reads the example source and
        guards both invariants.
        """
        example = Path(__file__).resolve().parents[4] / (
            "system/google/cloud/composer/example_cloud_composer.py"
        )
        text = example.read_text()

        def extract_block(start_marker: str, end_marker: str) -> str:
            match = re.search(
                rf"# \[START {start_marker}\](.*?)# \[END {end_marker}\]",
                text,
                flags=re.DOTALL,
            )
            assert match is not None, f"markers {start_marker!r}/{end_marker!r} not found"
            return match.group(1)

        sync_block = extract_block("howto_sensor_dag_run", "howto_sensor_dag_run")
        defer_block = extract_block(
            "howto_sensor_dag_run_deferrable_mode",
            "howto_sensor_dag_run_deferrable_mode",
        )

        for label, block, upstream_task_id in (
            ("sync", sync_block, "trigger_dag_run"),
            ("deferrable", defer_block, "defer_trigger_dag_run"),
        ):
            assert "datetime.now(" not in block, (
                f"{label} dag_run_sensor block must not call datetime.now() "
                "(see PR #61046 / PR #67052 history)"
            )
            assert "execution_range" not in block, (
                f"{label} dag_run_sensor block must not set execution_range; "
                "use composer_dag_run_id pinning instead"
            )
            assert f"task_ids='{upstream_task_id}'" in block, (
                f"{label} dag_run_sensor must pull dag_run_id from task '{upstream_task_id}'"
            )
            assert "['dag_run_id']" in block, (
                f"{label} dag_run_sensor must subscript the trigger op's xcom for the 'dag_run_id' key"
            )


class TestComposerDagRunWindowHelpers:
    """Unit tests for shared Composer window/date helpers (E2/E3/E4/E6/E7)."""

    def test_date_field_af2_vs_af3(self):
        assert composer_dag_run_date_field(2) == "execution_date"
        assert composer_dag_run_date_field(3) == "logical_date"

    def test_parse_null_returns_none(self):
        assert parse_composer_airflow_datetime(None) is None
        assert parse_composer_airflow_datetime("") is None

    def test_parse_naive_as_utc(self):
        dt = parse_composer_airflow_datetime("2024-05-22T11:10:00")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.utcoffset().total_seconds() == 0
        assert dt == datetime(2024, 5, 22, 11, 10, 0, tzinfo=timezone.utc)

    def test_window_start_inclusive_end_exclusive(self):
        start = datetime(2024, 5, 22, tzinfo=timezone.utc)
        end = datetime(2024, 5, 23, tzinfo=timezone.utc)
        assert is_in_execution_window(start, start, end)
        assert is_in_execution_window(datetime(2024, 5, 22, 12, 0, 0, tzinfo=timezone.utc), start, end)
        assert not is_in_execution_window(end, start, end)
        assert not is_in_execution_window(datetime(2024, 5, 21, tzinfo=timezone.utc), start, end)

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    def test_check_skips_null_date_runs(self, composer_airflow_version):
        date_key = composer_dag_run_date_field(composer_airflow_version)
        dag_runs = [
            {"state": "failed", date_key: None},
            {"state": "success", date_key: "2024-05-22T12:00:00+00:00"},
        ]
        assert check_dag_run_states_in_window(
            dag_runs,
            start_date=datetime(2024, 5, 22, tzinfo=timezone.utc),
            end_date=datetime(2024, 5, 23, tzinfo=timezone.utc),
            allowed_states=["success"],
            composer_airflow_version=composer_airflow_version,
        )

    def test_empty_allowed_states_defaults_to_success(self):
        dag_runs = [{"state": "success", "logical_date": "2024-05-22T12:00:00+00:00"}]
        assert check_dag_run_states_in_window(
            dag_runs,
            start_date=datetime(2024, 5, 22, tzinfo=timezone.utc),
            end_date=datetime(2024, 5, 23, tzinfo=timezone.utc),
            allowed_states=[],
            composer_airflow_version=3,
        )
        assert not check_dag_run_states_in_window(
            [{"state": "failed", "logical_date": "2024-05-22T12:00:00+00:00"}],
            start_date=datetime(2024, 5, 22, tzinfo=timezone.utc),
            end_date=datetime(2024, 5, 23, tzinfo=timezone.utc),
            allowed_states=None,
            composer_airflow_version=3,
        )


class TestCloudComposerDAGRunSensorTriggerParity:
    """E8: sensor and trigger must produce identical verdicts for the same inputs."""

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @pytest.mark.parametrize(
        ("dag_runs_spec", "expected"),
        [
            # empty window (only out-of-range)
            ([("success", "2024-05-24T11:10:00+00:00")], False),
            # in-window allowed
            ([("success", "2024-05-22T11:10:00+00:00")], True),
            # in-window disallowed
            ([("failed", "2024-05-22T11:10:00+00:00")], False),
            # start boundary inclusive
            ([("success", "2024-05-22T00:00:00+00:00")], True),
            # end boundary exclusive
            ([("success", "2024-05-23T00:00:00+00:00")], False),
            # mixed: out-of-window failed + in-window success
            (
                [
                    ("failed", "2024-05-24T11:10:00+00:00"),
                    ("success", "2024-05-22T11:10:00+00:00"),
                ],
                True,
            ),
            # null date only -> empty window
            # handled separately below for null payload shape
        ],
    )
    def test_sensor_and_trigger_agree_on_window_verdicts(
        self, composer_airflow_version, dag_runs_spec, expected
    ):
        start = datetime(2024, 5, 22, tzinfo=timezone.utc)
        end = datetime(2024, 5, 23, tzinfo=timezone.utc)
        payload = build_dag_runs_result(composer_airflow_version, dag_runs_spec)
        dag_runs = payload["dag_runs"]

        sensor = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            allowed_states=["success"],
        )
        sensor._composer_airflow_version = composer_airflow_version
        sensor_verdict = sensor._check_dag_runs_states(dag_runs, start, end)

        trigger = CloudComposerDAGRunTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            start_date=start,
            end_date=end,
            allowed_states=["success"],
            composer_airflow_version=composer_airflow_version,
        )
        trigger_verdict = trigger._check_dag_runs_states(dag_runs, start, end)

        assert sensor_verdict is expected
        assert trigger_verdict is expected
        assert sensor_verdict is trigger_verdict

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    def test_sensor_and_trigger_agree_on_null_and_naive_dates(self, composer_airflow_version):
        start = datetime(2024, 5, 22, tzinfo=timezone.utc)
        end = datetime(2024, 5, 23, tzinfo=timezone.utc)
        date_key = composer_dag_run_date_field(composer_airflow_version)
        dag_runs = [
            {"dag_run_id": "n1", "state": "success", date_key: None},
            {"dag_run_id": "n2", "state": "success", date_key: "2024-05-22T11:10:00"},  # naive
        ]

        sensor = CloudComposerDAGRunSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            allowed_states=["success"],
        )
        sensor._composer_airflow_version = composer_airflow_version

        trigger = CloudComposerDAGRunTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id="test_dag_id",
            start_date=start,
            end_date=end,
            allowed_states=["success"],
            composer_airflow_version=composer_airflow_version,
        )

        sensor_verdict = sensor._check_dag_runs_states(dag_runs, start, end)
        trigger_verdict = trigger._check_dag_runs_states(dag_runs, start, end)
        assert sensor_verdict is True
        assert trigger_verdict is True
