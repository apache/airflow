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

import json
from types import SimpleNamespace
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from google.cloud.storage_transfer_v1.types.transfer_types import TransferOperation

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    CloudDataTransferServiceAsyncHook,
    GcpTransferOperationStatus,
)

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

TEST_PROJECT_ID = "project-id"
TRANSFER_HOOK_PATH = "airflow.providers.google.cloud.hooks.cloud_storage_transfer_service"


@pytest.fixture
def hook_async():
    with mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseAsyncHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    ):
        yield CloudDataTransferServiceAsyncHook()


class TestCloudDataTransferServiceAsyncHook:
    @pytest.mark.asyncio
    @mock.patch(f"{TRANSFER_HOOK_PATH}.CloudDataTransferServiceAsyncHook.get_conn")
    @mock.patch(f"{TRANSFER_HOOK_PATH}.StorageTransferServiceAsyncClient")
    async def test_get_conn(self, mock_async_client, mock_get_conn):
        expected_value = "Async Hook"
        mock_async_client.return_value = expected_value
        mock_get_conn.return_value = expected_value

        hook = CloudDataTransferServiceAsyncHook(project_id=TEST_PROJECT_ID)

        conn_0 = await hook.get_conn()
        assert conn_0 == expected_value

        conn_1 = await hook.get_conn()
        assert conn_1 == expected_value
        assert id(conn_0) == id(conn_1)

    @pytest.mark.asyncio
    @mock.patch(f"{TRANSFER_HOOK_PATH}.CloudDataTransferServiceAsyncHook.get_conn")
    @mock.patch(f"{TRANSFER_HOOK_PATH}.ListTransferJobsRequest")
    async def test_get_jobs(self, mock_list_jobs_request, mock_get_conn):
        expected_jobs = AsyncMock()
        mock_get_conn.return_value.list_transfer_jobs.side_effect = AsyncMock(return_value=expected_jobs)

        expected_request = mock.MagicMock()
        mock_list_jobs_request.return_value = expected_request

        hook = CloudDataTransferServiceAsyncHook(project_id=TEST_PROJECT_ID)
        job_names = ["Job0", "Job1"]
        jobs = await hook.get_jobs(job_names=job_names)

        assert jobs == expected_jobs
        mock_list_jobs_request.assert_called_once_with(
            filter=json.dumps(dict(project_id=TEST_PROJECT_ID, job_names=job_names))
        )
        mock_get_conn.return_value.list_transfer_jobs.assert_called_once_with(request=expected_request)

    @pytest.mark.asyncio
    @mock.patch(f"{TRANSFER_HOOK_PATH}.CloudDataTransferServiceAsyncHook.get_conn")
    @mock.patch(f"{TRANSFER_HOOK_PATH}.TransferOperation.deserialize")
    async def test_get_last_operation(self, mock_deserialize, mock_conn, hook_async):
        latest_operation_name = "Mock operation name"
        operation_metadata_value = "Mock metadata value"

        get_operation = AsyncMock()
        get_operation.return_value = mock.MagicMock(metadata=mock.MagicMock(value=operation_metadata_value))
        mock_conn.return_value.transport.operations_client.get_operation = get_operation

        expected_operation = mock.MagicMock()
        mock_deserialize.return_value = expected_operation

        operation = await hook_async.get_latest_operation(
            job=mock.MagicMock(latest_operation_name=latest_operation_name)
        )

        get_operation.assert_called_once_with(latest_operation_name)
        mock_deserialize.assert_called_once_with(operation_metadata_value)
        assert operation == expected_operation

    @pytest.mark.asyncio
    @mock.patch(f"{TRANSFER_HOOK_PATH}.CloudDataTransferServiceAsyncHook.get_conn")
    @mock.patch(f"{TRANSFER_HOOK_PATH}.TransferOperation.deserialize")
    async def test_get_last_operation_none(self, mock_deserialize, mock_conn, hook_async):
        latest_operation_name = None
        expected_operation = None

        get_operation = mock.MagicMock()
        mock_conn.return_value.transport.operations_client.get_operation = get_operation

        operation = await hook_async.get_latest_operation(
            job=mock.MagicMock(latest_operation_name=latest_operation_name)
        )

        get_operation.assert_not_called()
        mock_deserialize.assert_not_called()
        assert operation == expected_operation

    @pytest.mark.asyncio
    @mock.patch(f"{TRANSFER_HOOK_PATH}.CloudDataTransferServiceAsyncHook.get_conn")
    @mock.patch(f"{TRANSFER_HOOK_PATH}.MessageToDict")
    async def test_list_transfer_operations(self, message_to_dict, mock_conn, hook_async):
        expected = [{"name": "op1"}, {"name": "op2"}]
        message_to_dict.side_effect = expected

        op_with_pb = SimpleNamespace(_pb=mock.sentinel.pb1)
        op_without_pb = object()

        first_page = mock.MagicMock(next_page_token="token", operations=[op_with_pb])
        second_page = mock.MagicMock(next_page_token=None, operations=[op_without_pb])
        mock_conn.return_value.list_operations.side_effect = [first_page, second_page]

        actual = await hook_async.list_transfer_operations(
            request_filter={"project_id": TEST_PROJECT_ID},
        )

        assert actual == expected
        assert mock_conn.return_value.list_operations.call_count == 2
        assert message_to_dict.call_args_list == [
            mock.call(mock.sentinel.pb1, preserving_proto_field_name=True, use_integers_for_enums=True),
            mock.call(op_without_pb, preserving_proto_field_name=True, use_integers_for_enums=True),
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("statuses", "expected_statuses"),
        [
            ([GcpTransferOperationStatus.ABORTED], (GcpTransferOperationStatus.IN_PROGRESS,)),
            (
                [GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.ABORTED],
                (GcpTransferOperationStatus.IN_PROGRESS,),
            ),
            (
                [GcpTransferOperationStatus.PAUSED, GcpTransferOperationStatus.ABORTED],
                (GcpTransferOperationStatus.IN_PROGRESS,),
            ),
        ],
    )
    async def test_operations_contain_expected_statuses_red_path(self, statuses, expected_statuses):
        def to_name(x):
            return x.name if hasattr(x, "name") else x

        def proto_int(name: str) -> int:
            return int(getattr(TransferOperation.Status, name))

        operations = [{"metadata": {"status": proto_int(to_name(s))}} for s in statuses]

        expected_names = tuple(to_name(s) for s in expected_statuses)

        with pytest.raises(
            AirflowException,
            match=f"An unexpected operation status was encountered. Expected: {', '.join(expected_names)}",
        ):
            await CloudDataTransferServiceAsyncHook.operations_contain_expected_statuses(
                operations,
                GcpTransferOperationStatus.IN_PROGRESS,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("statuses", "expected_statuses"),
        [
            ([GcpTransferOperationStatus.ABORTED], GcpTransferOperationStatus.ABORTED),
            (
                [GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.ABORTED],
                GcpTransferOperationStatus.ABORTED,
            ),
            (
                [GcpTransferOperationStatus.PAUSED, GcpTransferOperationStatus.ABORTED],
                GcpTransferOperationStatus.ABORTED,
            ),
            ([GcpTransferOperationStatus.ABORTED], (GcpTransferOperationStatus.ABORTED,)),
            (
                [GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.ABORTED],
                (GcpTransferOperationStatus.ABORTED,),
            ),
            (
                [GcpTransferOperationStatus.PAUSED, GcpTransferOperationStatus.ABORTED],
                (GcpTransferOperationStatus.ABORTED,),
            ),
        ],
    )
    async def test_operations_contain_expected_statuses_green_path(self, statuses, expected_statuses):
        to_name = lambda x: x.name if hasattr(x, "name") else x
        name_to_proto_int = lambda name: int(getattr(TransferOperation.Status, name))

        operations = [{"metadata": {"status": name_to_proto_int(to_name(s))}} for s in statuses]

        if isinstance(expected_statuses, (list, tuple, set)):
            expected_norm = {to_name(s) for s in expected_statuses}
        else:
            expected_norm = to_name(expected_statuses)

        result = await CloudDataTransferServiceAsyncHook.operations_contain_expected_statuses(
            operations, expected_norm
        )
        assert result is True

    @pytest.mark.asyncio
    @mock.patch(f"{TRANSFER_HOOK_PATH}.CloudDataTransferServiceAsyncHook.get_conn")
    @mock.patch(f"{TRANSFER_HOOK_PATH}.RunTransferJobRequest")
    async def test_run_transfer_job(self, mock_run_transfer_job_request, mock_get_conn):
        expected_job_result = AsyncMock()
        mock_get_conn.return_value.run_transfer_job.side_effect = AsyncMock(return_value=expected_job_result)

        expected_request = mock.MagicMock()
        mock_run_transfer_job_request.return_value = expected_request

        hook = CloudDataTransferServiceAsyncHook(project_id=TEST_PROJECT_ID)
        job_name = "Job0"
        jobs = await hook.run_transfer_job(job_name=job_name)

        assert jobs == expected_job_result
        mock_run_transfer_job_request.assert_called_once_with(project_id=TEST_PROJECT_ID, job_name=job_name)
        mock_get_conn.return_value.run_transfer_job.assert_called_once_with(request=expected_request)
