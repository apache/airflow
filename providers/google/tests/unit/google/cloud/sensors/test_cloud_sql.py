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

import httplib2
import pytest
from googleapiclient.errors import HttpError

from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.google.cloud.sensors.cloud_sql import CloudSQLNoOperationInProgressSensor
from airflow.providers.google.cloud.triggers.cloud_sql import CloudSQLNoOperationInProgressTrigger

SENSOR_PATH = "airflow.providers.google.cloud.sensors.cloud_sql.{}"
TASK_ID = "test_no_op_in_progress"
INSTANCE = "test-instance"
PROJECT_ID = "test-project"


def _http_error(status: int) -> HttpError:
    return HttpError(resp=httplib2.Response({"status": status}), content=b"error content")


class TestCloudSQLNoOperationInProgressSensor:
    @mock.patch(SENSOR_PATH.format("CloudSQLHook"))
    def test_poke_returns_true_when_no_in_progress_operations(self, mock_hook):
        mock_hook.return_value.list_operations.return_value = [
            {"name": "op1", "status": "DONE", "targetId": INSTANCE},
        ]
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id=TASK_ID, instance=INSTANCE, project_id=PROJECT_ID
        )
        assert sensor.poke(context={}) is True
        mock_hook.return_value.list_operations.assert_called_once_with(
            project_id=PROJECT_ID, instance=INSTANCE
        )

    @mock.patch(SENSOR_PATH.format("CloudSQLHook"))
    def test_poke_returns_false_when_operation_in_progress(self, mock_hook):
        mock_hook.return_value.list_operations.return_value = [
            {"name": "op1", "status": "RUNNING", "targetId": INSTANCE},
            {"name": "op2", "status": "PENDING", "targetId": INSTANCE},
        ]
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id=TASK_ID, instance=INSTANCE, project_id=PROJECT_ID
        )
        assert sensor.poke(context={}) is False

    @pytest.mark.parametrize("status", [403, 404])
    @mock.patch(SENSOR_PATH.format("CloudSQLHook"))
    def test_poke_fails_fast_on_403_404(self, mock_hook, status):
        mock_hook.return_value.list_operations.side_effect = _http_error(status)
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id=TASK_ID, instance=INSTANCE, project_id=PROJECT_ID
        )
        with pytest.raises(AirflowException, match="operations.list failed"):
            sensor.poke(context={})

    @mock.patch(SENSOR_PATH.format("CloudSQLHook"))
    def test_poke_reraises_other_http_errors(self, mock_hook):
        mock_hook.return_value.list_operations.side_effect = _http_error(500)
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id=TASK_ID, instance=INSTANCE, project_id=PROJECT_ID
        )
        with pytest.raises(HttpError):
            sensor.poke(context={})

    @mock.patch(SENSOR_PATH.format("CloudSQLHook"))
    def test_execute_defers_when_deferrable_and_not_idle(self, mock_hook):
        mock_hook.return_value.list_operations.return_value = [
            {"name": "op1", "status": "RUNNING", "targetId": INSTANCE},
        ]
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id=TASK_ID, instance=INSTANCE, project_id=PROJECT_ID, deferrable=True
        )
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context={})
        assert isinstance(exc.value.trigger, CloudSQLNoOperationInProgressTrigger)
        assert exc.value.method_name == "execute_complete"

    @mock.patch(SENSOR_PATH.format("CloudSQLHook"))
    def test_execute_does_not_defer_when_idle(self, mock_hook):
        mock_hook.return_value.list_operations.return_value = [
            {"name": "op1", "status": "DONE", "targetId": INSTANCE},
        ]
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id=TASK_ID, instance=INSTANCE, project_id=PROJECT_ID, deferrable=True
        )
        # Already idle -> the sensor returns without deferring.
        assert sensor.execute(context={}) is None

    @mock.patch(SENSOR_PATH.format("BaseSensorOperator.execute"))
    @mock.patch(SENSOR_PATH.format("CloudSQLHook"))
    def test_execute_non_deferrable_delegates_to_super(self, mock_hook, mock_super_execute):
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id=TASK_ID, instance=INSTANCE, project_id=PROJECT_ID, deferrable=False
        )
        sensor.execute(context={})
        mock_super_execute.assert_called_once()

    def test_execute_complete_success(self):
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id=TASK_ID, instance=INSTANCE, project_id=PROJECT_ID
        )
        # No exception is raised when the trigger reports success.
        assert sensor.execute_complete(context={}, event={"instance": INSTANCE, "status": "success"}) is None

    @pytest.mark.parametrize("status", ["failed", "error"])
    def test_execute_complete_failure_raises(self, status):
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id=TASK_ID, instance=INSTANCE, project_id=PROJECT_ID
        )
        with pytest.raises(AirflowException, match="boom"):
            sensor.execute_complete(context={}, event={"status": status, "message": "boom"})
