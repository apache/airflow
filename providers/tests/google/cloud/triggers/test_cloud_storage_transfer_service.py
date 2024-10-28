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
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.storage_transfer_v1 import TransferOperation

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    CloudDataTransferServiceAsyncHook,
)
from airflow.providers.google.cloud.triggers.cloud_storage_transfer_service import (
    CloudStorageTransferServiceCreateJobsTrigger,
)
from airflow.triggers.base import TriggerEvent

PROJECT_ID = "test-project"
GCP_CONN_ID = "google-cloud-default-id"
JOB_0 = "test-job-0"
JOB_1 = "test-job-1"
JOB_NAMES = [JOB_0, JOB_1]
LATEST_OPERATION_NAME_0 = "test-latest-operation-0"
LATEST_OPERATION_NAME_1 = "test-latest-operation-1"
LATEST_OPERATION_NAMES = [LATEST_OPERATION_NAME_0, LATEST_OPERATION_NAME_1]
POLL_INTERVAL = 2
CLASS_PATH = (
    "airflow.providers.google.cloud.triggers.cloud_storage_transfer_service"
    ".CloudStorageTransferServiceCreateJobsTrigger"
)
ASYNC_HOOK_CLASS_PATH = "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.CloudDataTransferServiceAsyncHook"


@pytest.fixture(scope="session")
def trigger():
    return CloudStorageTransferServiceCreateJobsTrigger(
        project_id=PROJECT_ID,
        job_names=JOB_NAMES,
        poll_interval=POLL_INTERVAL,
        gcp_conn_id=GCP_CONN_ID,
    )


def mock_jobs(names: list[str], latest_operation_names: list[str | None]):
    """Returns object that mocks asynchronous looping over mock jobs"""
    jobs = [mock.MagicMock(latest_operation_name=name) for name in latest_operation_names]
    for job, name in zip(jobs, names):
        job.name = name
    mock_obj = mock.MagicMock()
    mock_obj.__aiter__.return_value = iter(jobs)
    return mock_obj


def create_mock_operation(status: TransferOperation.Status, name: str) -> mock.MagicMock:
    _obj = mock.MagicMock(status=status)
    _obj.name = name
    return _obj


class TestCloudStorageTransferServiceCreateJobsTrigger:
    def test_serialize(self, trigger):
        class_path, serialized = trigger.serialize()

        assert class_path == CLASS_PATH
        assert serialized == {
            "project_id": PROJECT_ID,
            "job_names": JOB_NAMES,
            "poll_interval": POLL_INTERVAL,
            "gcp_conn_id": GCP_CONN_ID,
        }

    def test_get_async_hook(self, trigger):
        hook = trigger.get_async_hook()

        assert isinstance(hook, CloudDataTransferServiceAsyncHook)
        assert hook.project_id == PROJECT_ID

    @pytest.mark.asyncio
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_latest_operation")
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_jobs")
    async def test_run(self, get_jobs, get_latest_operation, trigger):
        get_jobs.return_value = mock_jobs(
            names=JOB_NAMES, latest_operation_names=LATEST_OPERATION_NAMES
        )
        get_latest_operation.side_effect = [
            create_mock_operation(
                status=TransferOperation.Status.SUCCESS, name="operation_" + job_name
            )
            for job_name in JOB_NAMES
        ]
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": f"Transfer jobs {JOB_0}, {JOB_1} completed successfully",
            }
        )
        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "status",
        [
            TransferOperation.Status.STATUS_UNSPECIFIED,
            TransferOperation.Status.IN_PROGRESS,
            TransferOperation.Status.PAUSED,
            TransferOperation.Status.QUEUED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_latest_operation", autospec=True)
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_jobs", autospec=True)
    async def test_run_poll_interval(
        self, get_jobs, get_latest_operation, mock_sleep, trigger, status
    ):
        get_jobs.side_effect = [
            mock_jobs(names=JOB_NAMES, latest_operation_names=LATEST_OPERATION_NAMES),
            mock_jobs(names=JOB_NAMES, latest_operation_names=LATEST_OPERATION_NAMES),
        ]
        get_latest_operation.side_effect = [
            create_mock_operation(status=status, name="operation_" + job_name)
            for job_name in JOB_NAMES
        ] + [
            create_mock_operation(
                status=TransferOperation.Status.SUCCESS, name="operation_" + job_name
            )
            for job_name in JOB_NAMES
        ]
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": f"Transfer jobs {JOB_0}, {JOB_1} completed successfully",
            }
        )

        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert actual_event == expected_event
        mock_sleep.assert_called_once_with(POLL_INTERVAL)

    @pytest.mark.parametrize(
        "latest_operations_names, expected_failed_job",
        [
            ([None, LATEST_OPERATION_NAME_1], JOB_0),
            ([LATEST_OPERATION_NAME_0, None], JOB_1),
        ],
    )
    @pytest.mark.asyncio
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_latest_operation")
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_jobs")
    async def test_run_error_job_has_no_latest_operation(
        self,
        get_jobs,
        get_latest_operation,
        trigger,
        latest_operations_names,
        expected_failed_job,
    ):
        get_jobs.return_value = mock_jobs(
            names=JOB_NAMES, latest_operation_names=latest_operations_names
        )
        get_latest_operation.side_effect = [
            create_mock_operation(
                status=TransferOperation.Status.SUCCESS, name="operation_" + job_name
            )
            if job_name
            else None
            for job_name in latest_operations_names
        ]
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Transfer job {expected_failed_job} has no latest operation.",
            }
        )

        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "job_statuses, failed_operation, expected_status",
        [
            (
                [TransferOperation.Status.ABORTED, TransferOperation.Status.SUCCESS],
                LATEST_OPERATION_NAME_0,
                "ABORTED",
            ),
            (
                [TransferOperation.Status.FAILED, TransferOperation.Status.SUCCESS],
                LATEST_OPERATION_NAME_0,
                "FAILED",
            ),
            (
                [TransferOperation.Status.SUCCESS, TransferOperation.Status.ABORTED],
                LATEST_OPERATION_NAME_1,
                "ABORTED",
            ),
            (
                [TransferOperation.Status.SUCCESS, TransferOperation.Status.FAILED],
                LATEST_OPERATION_NAME_1,
                "FAILED",
            ),
        ],
    )
    @pytest.mark.asyncio
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_latest_operation")
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_jobs")
    async def test_run_error_one_job_failed_or_aborted(
        self,
        get_jobs,
        get_latest_operation,
        trigger,
        job_statuses,
        failed_operation,
        expected_status,
    ):
        get_jobs.return_value = mock_jobs(
            names=JOB_NAMES, latest_operation_names=LATEST_OPERATION_NAMES
        )
        get_latest_operation.side_effect = [
            create_mock_operation(status=status, name=operation_name)
            for status, operation_name in zip(job_statuses, LATEST_OPERATION_NAMES)
        ]
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Transfer operation {failed_operation} failed with status {expected_status}",
            }
        )

        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_latest_operation")
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_jobs")
    async def test_run_get_jobs_airflow_exception(
        self, get_jobs, get_latest_operation, trigger
    ):
        expected_error_message = "Mock error message"
        get_jobs.side_effect = AirflowException(expected_error_message)

        get_latest_operation.side_effect = [
            create_mock_operation(
                status=TransferOperation.Status.SUCCESS, name="operation_" + job_name
            )
            for job_name in JOB_NAMES
        ]
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": expected_error_message,
            }
        )

        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_latest_operation")
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_jobs")
    async def test_run_get_latest_operation_airflow_exception(
        self, get_jobs, get_latest_operation, trigger
    ):
        get_jobs.return_value = mock_jobs(
            names=JOB_NAMES, latest_operation_names=LATEST_OPERATION_NAMES
        )
        expected_error_message = "Mock error message"
        get_latest_operation.side_effect = AirflowException(expected_error_message)

        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": expected_error_message,
            }
        )

        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_latest_operation")
    @mock.patch(ASYNC_HOOK_CLASS_PATH + ".get_jobs")
    async def test_run_get_latest_operation_google_api_call_error(
        self, get_jobs, get_latest_operation, trigger
    ):
        get_jobs.return_value = mock_jobs(
            names=JOB_NAMES, latest_operation_names=LATEST_OPERATION_NAMES
        )
        error_message = "Mock error message"
        get_latest_operation.side_effect = GoogleAPICallError(error_message)

        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"{None} {error_message}",
            }
        )

        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert actual_event == expected_event
