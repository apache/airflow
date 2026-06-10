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

from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.google.cloud.sensors.cloud_sql import CloudSQLNoOperationInProgressSensor
from airflow.providers.google.cloud.triggers.cloud_sql import CloudSQLNoOperationInProgressTrigger

PROJECT_ID = "test-project"
INSTANCE = "test-instance"
GCP_CONN_ID = "test-gcp-conn-id"
IMPERSONATION_CHAIN = ["impersonate@service.account"]
API_VERSION = "v1beta4"


class TestCloudSQLNoOperationInProgressSensor:
    def test_poke_returns_true_when_no_operations_are_running(self):
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id="wait_for_cloud_sql_slot",
            project_id=PROJECT_ID,
            instance=INSTANCE,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            api_version=API_VERSION,
        )

        with mock.patch("airflow.providers.google.cloud.sensors.cloud_sql.CloudSQLHook") as mock_hook:
            mock_hook.return_value.get_instance_operations.return_value = [
                {"name": "done-operation", "targetId": INSTANCE, "status": "DONE"}
            ]

            assert sensor.poke(context={}) is True

        mock_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_instance_operations.assert_called_once_with(
            project_id=PROJECT_ID, instance=INSTANCE
        )

    def test_poke_returns_false_when_operation_is_running_for_instance(self):
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id="wait_for_cloud_sql_slot",
            project_id=PROJECT_ID,
            instance=INSTANCE,
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
        )

        with mock.patch("airflow.providers.google.cloud.sensors.cloud_sql.CloudSQLHook") as mock_hook:
            mock_hook.return_value.get_instance_operations.return_value = [
                {"name": "running-operation", "targetId": INSTANCE, "status": "RUNNING"},
                {"name": "other-instance-operation", "targetId": "another-instance", "status": "RUNNING"},
            ]

            assert sensor.poke(context={}) is False

    def test_execute_defers_when_operation_is_running(self):
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id="wait_for_cloud_sql_slot",
            project_id=PROJECT_ID,
            instance=INSTANCE,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            api_version=API_VERSION,
            deferrable=True,
            poke_interval=30,
            timeout=120,
        )

        with mock.patch.object(sensor, "poke", return_value=False), pytest.raises(TaskDeferred) as exc:
            sensor.execute(context={})

        assert isinstance(exc.value.trigger, CloudSQLNoOperationInProgressTrigger)
        assert exc.value.method_name == "execute_complete"
        assert exc.value.timeout.total_seconds() == 120
        assert exc.value.trigger.project_id == PROJECT_ID
        assert exc.value.trigger.instance == INSTANCE
        assert exc.value.trigger.gcp_conn_id == GCP_CONN_ID
        assert exc.value.trigger.impersonation_chain == IMPERSONATION_CHAIN
        assert exc.value.trigger.api_version == API_VERSION
        assert exc.value.trigger.poke_interval == 30

    def test_execute_complete_raises_on_trigger_failure(self):
        sensor = CloudSQLNoOperationInProgressSensor(
            task_id="wait_for_cloud_sql_slot",
            project_id=PROJECT_ID,
            instance=INSTANCE,
        )

        with pytest.raises(RuntimeError, match="test failure"):
            sensor.execute_complete(context={}, event={"status": "failed", "message": "test failure"})
