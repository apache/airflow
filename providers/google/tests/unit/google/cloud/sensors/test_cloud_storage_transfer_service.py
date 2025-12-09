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
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import GcpTransferOperationStatus
from airflow.providers.google.cloud.sensors.cloud_storage_transfer_service import (
    CloudDataTransferServiceJobStatusSensor,
)
from airflow.providers.google.cloud.triggers.cloud_storage_transfer_service import (
    CloudStorageTransferServiceCheckJobStatusTrigger,
)

TEST_NAME = "transferOperations/transferJobs-123-456"
TEST_COUNTERS = {
    "bytesFoundFromSource": 512,
    "bytesCopiedToSink": 1024,
}
JOB_NAME = "job-name/123"


class TestGcpStorageTransferOperationWaitForJobStatusSensor:
    @mock.patch(
        "airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook"
    )
    def test_wait_for_status_success(self, mock_tool):
        operations = [
            {
                "name": TEST_NAME,
                "metadata": {
                    "status": GcpTransferOperationStatus.SUCCESS,
                    "counters": TEST_COUNTERS,
                },
            }
        ]
        mock_tool.return_value.list_transfer_operations.return_value = operations
        mock_tool.operations_contain_expected_statuses.return_value = True

        op = CloudDataTransferServiceJobStatusSensor(
            task_id="task-id",
            job_name=JOB_NAME,
            project_id="project-id",
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
        )

        context = {"ti": (mock.Mock(**{"xcom_push.return_value": None})), "task": mock.MagicMock()}
        result = op.poke(context)

        mock_tool.return_value.list_transfer_operations.assert_called_once_with(
            request_filter={"project_id": "project-id", "job_names": [JOB_NAME]}
        )
        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=operations, expected_statuses={GcpTransferOperationStatus.SUCCESS}
        )
        assert result

    @mock.patch(
        "airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook"
    )
    def test_wait_for_status_success_without_project_id(self, mock_tool):
        operations = [
            {
                "name": TEST_NAME,
                "metadata": {
                    "status": GcpTransferOperationStatus.SUCCESS,
                    "counters": TEST_COUNTERS,
                },
            }
        ]
        mock_tool.return_value.list_transfer_operations.return_value = operations
        mock_tool.operations_contain_expected_statuses.return_value = True
        mock_tool.return_value.project_id = "project-id"

        op = CloudDataTransferServiceJobStatusSensor(
            task_id="task-id",
            job_name=JOB_NAME,
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
        )

        context = {"ti": (mock.Mock(**{"xcom_push.return_value": None})), "task": mock.MagicMock()}
        result = op.poke(context)

        mock_tool.return_value.list_transfer_operations.assert_called_once_with(
            request_filter={"project_id": "project-id", "job_names": [JOB_NAME]}
        )
        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=operations, expected_statuses={GcpTransferOperationStatus.SUCCESS}
        )
        assert result

    @mock.patch(
        "airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook"
    )
    def test_wait_for_status_success_default_expected_status(self, mock_tool):
        op = CloudDataTransferServiceJobStatusSensor(
            task_id="task-id",
            job_name=JOB_NAME,
            project_id="project-id",
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
        )

        context = {"ti": (mock.Mock(**{"xcom_push.return_value": None})), "task": mock.MagicMock()}

        result = op.poke(context)

        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=mock.ANY, expected_statuses={GcpTransferOperationStatus.SUCCESS}
        )
        assert result

    @mock.patch(
        "airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook"
    )
    def test_wait_for_status_after_retry(self, mock_tool):
        operations_set = [
            [
                {
                    "name": TEST_NAME,
                    "metadata": {
                        "status": GcpTransferOperationStatus.SUCCESS,
                        "counters": TEST_COUNTERS,
                    },
                },
            ],
            [
                {
                    "name": TEST_NAME,
                    "metadata": {
                        "status": GcpTransferOperationStatus.SUCCESS,
                        "counters": TEST_COUNTERS,
                    },
                },
            ],
        ]

        mock_tool.return_value.list_transfer_operations.side_effect = operations_set
        mock_tool.operations_contain_expected_statuses.side_effect = [False, True]

        op = CloudDataTransferServiceJobStatusSensor(
            task_id="task-id",
            job_name=JOB_NAME,
            project_id="project-id",
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
        )

        context = {"ti": (mock.Mock(**{"xcom_push.return_value": None})), "task": mock.MagicMock()}

        result = op.poke(context)
        assert not result

        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=operations_set[0], expected_statuses={GcpTransferOperationStatus.SUCCESS}
        )
        mock_tool.operations_contain_expected_statuses.reset_mock()

        result = op.poke(context)
        assert result

        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=operations_set[1], expected_statuses={GcpTransferOperationStatus.SUCCESS}
        )

    @pytest.mark.parametrize(
        ("expected_status", "received_status"),
        [
            (GcpTransferOperationStatus.SUCCESS, {GcpTransferOperationStatus.SUCCESS}),
            ({GcpTransferOperationStatus.SUCCESS}, {GcpTransferOperationStatus.SUCCESS}),
            (
                {GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.SUCCESS},
                {GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.SUCCESS},
            ),
        ],
    )
    @mock.patch(
        "airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook"
    )
    def test_wait_for_status_normalize_status(self, mock_tool, expected_status, received_status):
        operations = [
            {
                "name": TEST_NAME,
                "metadata": {
                    "status": GcpTransferOperationStatus.SUCCESS,
                    "counters": TEST_COUNTERS,
                },
            }
        ]

        mock_tool.return_value.list_transfer_operations.return_value = operations
        mock_tool.operations_contain_expected_statuses.side_effect = [False, True]

        op = CloudDataTransferServiceJobStatusSensor(
            task_id="task-id",
            job_name=JOB_NAME,
            project_id="project-id",
            expected_statuses=expected_status,
        )

        context = {"ti": (mock.Mock(**{"xcom_push.return_value": None})), "task": mock.MagicMock()}

        result = op.poke(context)
        assert not result

        mock_tool.operations_contain_expected_statuses.assert_called_once_with(
            operations=operations, expected_statuses=received_status
        )

    @mock.patch(
        "airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook"
    )
    @mock.patch(
        "airflow.providers.google.cloud.sensors.cloud_storage_transfer_service"
        ".CloudDataTransferServiceJobStatusSensor.defer"
    )
    def test_job_status_sensor_finish_before_deferred(self, mock_defer, mock_hook):
        op = CloudDataTransferServiceJobStatusSensor(
            task_id="task-id",
            job_name=JOB_NAME,
            project_id="project-id",
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
            deferrable=True,
        )

        mock_hook.operations_contain_expected_statuses.return_value = True
        context = {"ti": (mock.Mock(**{"xcom_push.return_value": None})), "task": mock.MagicMock()}

        op.execute(context)
        assert not mock_defer.called

    @mock.patch(
        "airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.CloudDataTransferServiceHook"
    )
    def test_execute_deferred(self, mock_hook):
        op = CloudDataTransferServiceJobStatusSensor(
            task_id="task-id",
            job_name=JOB_NAME,
            project_id="project-id",
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
            deferrable=True,
        )

        mock_hook.operations_contain_expected_statuses.return_value = False
        context = {"ti": (mock.Mock(**{"xcom_push.return_value": None})), "task": mock.MagicMock()}

        with pytest.raises(TaskDeferred) as exc:
            op.execute(context)
        assert isinstance(exc.value.trigger, CloudStorageTransferServiceCheckJobStatusTrigger)

    def test_execute_deferred_failure(self):
        op = CloudDataTransferServiceJobStatusSensor(
            task_id="task-id",
            job_name=JOB_NAME,
            project_id="project-id",
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
            deferrable=True,
        )

        context = {"ti": (mock.Mock(**{"xcom_push.return_value": None})), "task": mock.MagicMock()}

        with pytest.raises(AirflowException):
            op.execute_complete(context=context, event={"status": "error", "message": "test failure message"})

    def test_execute_complete(self):
        op = CloudDataTransferServiceJobStatusSensor(
            task_id="task-id",
            job_name=JOB_NAME,
            project_id="project-id",
            expected_statuses=GcpTransferOperationStatus.SUCCESS,
            deferrable=True,
        )

        context = {"ti": (mock.Mock(**{"xcom_push.return_value": None})), "task": mock.MagicMock()}

        op.execute_complete(context=context, event={"status": "success", "operations": []})
