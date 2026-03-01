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

import pytest

from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.google.cloud.sensors.cloud_sql import CloudSQLInstanceOperationsSensor

INSTANCE_NAME = "test-instance"
PROJECT_ID = "test-project"


class TestCloudSQLInstanceOperationsSensor:
    @mock.patch("airflow.providers.google.cloud.sensors.cloud_sql.CloudSQLHook")
    def test_poke_success_when_no_operations(self, mock_hook_class):
        mock_hook = mock.Mock()
        mock_hook.list_instance_operations.return_value = []
        mock_hook.project_id = PROJECT_ID
        mock_hook_class.return_value = mock_hook

        sensor = CloudSQLInstanceOperationsSensor(
            task_id="task",
            instance=INSTANCE_NAME,
            project_id=PROJECT_ID,
        )
        context = {"ti": mock.Mock(), "task": mock.MagicMock()}
        assert sensor.poke(context) is True

    @mock.patch("airflow.providers.google.cloud.sensors.cloud_sql.CloudSQLHook")
    def test_poke_success_when_all_done(self, mock_hook_class):
        mock_hook = mock.Mock()
        mock_hook.list_instance_operations.return_value = [
            {"name": "op1", "status": "DONE"},
        ]
        mock_hook.project_id = PROJECT_ID
        mock_hook_class.return_value = mock_hook

        sensor = CloudSQLInstanceOperationsSensor(
            task_id="task",
            instance=INSTANCE_NAME,
            project_id=PROJECT_ID,
        )
        context = {"ti": mock.Mock(), "task": mock.MagicMock()}
        assert sensor.poke(context) is True

    @mock.patch("airflow.providers.google.cloud.sensors.cloud_sql.CloudSQLHook")
    def test_poke_false_when_operation_running(self, mock_hook_class):
        mock_hook = mock.Mock()
        mock_hook.list_instance_operations.return_value = [
            {"name": "op1", "status": "RUNNING"},
        ]
        mock_hook_class.return_value = mock_hook

        sensor = CloudSQLInstanceOperationsSensor(
            task_id="task",
            instance=INSTANCE_NAME,
            project_id=PROJECT_ID,
        )
        context = {"ti": mock.Mock(), "task": mock.MagicMock()}
        assert sensor.poke(context) is False

    @mock.patch("airflow.providers.google.cloud.sensors.cloud_sql.CloudSQLHook")
    def test_execute_deferrable_defers_when_not_ready(self, mock_hook_class):
        mock_hook = mock.Mock()
        mock_hook.list_instance_operations.return_value = [{"name": "op1", "status": "RUNNING"}]
        mock_hook.project_id = PROJECT_ID
        mock_hook_class.return_value = mock_hook

        sensor = CloudSQLInstanceOperationsSensor(
            task_id="task",
            instance=INSTANCE_NAME,
            project_id=PROJECT_ID,
            deferrable=True,
        )
        context = {"ti": mock.Mock(), "task": mock.MagicMock()}
        with pytest.raises(TaskDeferred):
            sensor.execute(context)

    @mock.patch("airflow.providers.google.cloud.sensors.cloud_sql.CloudSQLHook")
    def test_execute_complete_raises_on_failure(self, mock_hook_class):
        sensor = CloudSQLInstanceOperationsSensor(
            task_id="task",
            instance=INSTANCE_NAME,
        )
        context = {"ti": mock.Mock(), "task": mock.MagicMock()}
        with pytest.raises(AirflowException, match="API failed"):
            sensor.execute_complete(context, {"status": "failed", "message": "API failed"})
