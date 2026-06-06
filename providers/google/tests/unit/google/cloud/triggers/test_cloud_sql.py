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

import asyncio
import logging
from unittest import mock

import httplib2
import pytest
from googleapiclient.errors import HttpError

from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook
from airflow.providers.google.cloud.triggers.cloud_sql import (
    CloudSQLExportTrigger,
    CloudSQLNoOperationInProgressTrigger,
)
from airflow.triggers.base import TriggerEvent

INSTANCE = "test-instance"
NO_OP_CLASSPATH = "airflow.providers.google.cloud.triggers.cloud_sql.CloudSQLNoOperationInProgressTrigger"

CLASSPATH = "airflow.providers.google.cloud.triggers.cloud_sql.CloudSQLExportTrigger"
TASK_ID = "test_task"
TEST_POLL_INTERVAL = 10
TEST_GCP_CONN_ID = "test-project"
HOOK_STR = "airflow.providers.google.cloud.hooks.cloud_sql.{}"
PROJECT_ID = "test_project_id"
OPERATION_NAME = "test_operation_name"
OPERATION_URL = (
    f"https://sqladmin.googleapis.com/sql/v1beta4/projects/{PROJECT_ID}/operations/{OPERATION_NAME}"
)
API_VERSION = "v1test"


@pytest.fixture
def trigger():
    return CloudSQLExportTrigger(
        operation_name=OPERATION_NAME,
        project_id=PROJECT_ID,
        impersonation_chain=None,
        gcp_conn_id=TEST_GCP_CONN_ID,
        poke_interval=TEST_POLL_INTERVAL,
        api_version=API_VERSION,
    )


@pytest.fixture
def sync_hook_mock():
    mock_obj = mock.MagicMock(spec=CloudSQLHook)
    with mock.patch(
        HOOK_STR.format("CloudSQLAsyncHook.get_sync_hook"), new_callable=mock.AsyncMock
    ) as patched_get_sync_hook:
        patched_get_sync_hook.return_value = mock_obj
        yield mock_obj


class TestCloudSQLExportTrigger:
    def test_async_export_trigger_serialization_should_execute_successfully(self, trigger, sync_hook_mock):
        """
        Asserts that the CloudSQLExportTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = trigger.serialize()
        assert classpath == CLASSPATH
        assert kwargs == {
            "operation_name": OPERATION_NAME,
            "project_id": PROJECT_ID,
            "impersonation_chain": None,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "poke_interval": TEST_POLL_INTERVAL,
            "api_version": API_VERSION,
        }

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation"))
    async def test_async_export_trigger_on_success_should_execute_successfully(
        self, mock_get_operation, trigger, sync_hook_mock
    ):
        """
        Tests the CloudSQLExportTrigger only fires once the job execution reaches a successful state.
        """
        mock_get_operation.return_value = {
            "status": "DONE",
            "name": OPERATION_NAME,
        }
        generator = trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent(
                {
                    "operation_name": OPERATION_NAME,
                    "status": "success",
                }
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation"))
    async def test_async_export_trigger_running_should_execute_successfully(
        self, mock_get_operation, trigger, sync_hook_mock, caplog
    ):
        """
        Test that CloudSQLExportTrigger does not fire while a job is still running.
        """
        # Ensure execution for default universe
        sync_hook_mock.is_default_universe.return_value = True
        mock_get_operation.return_value = {
            "status": "RUNNING",
            "name": OPERATION_NAME,
        }
        caplog.set_level(logging.INFO)
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False

        assert f"Operation status is RUNNING, sleeping for {TEST_POLL_INTERVAL} seconds." in caplog.text

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation"))
    async def test_async_export_trigger_error_should_execute_successfully(
        self, mock_get_operation, trigger, sync_hook_mock
    ):
        """
        Test that CloudSQLExportTrigger fires the correct event in case of an error.
        """
        mock_get_operation.return_value = {
            "status": "DONE",
            "name": OPERATION_NAME,
            "error": {"message": "test_error"},
        }

        expected_event = {
            "operation_name": OPERATION_NAME,
            "status": "error",
            "message": "test_error",
        }

        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent(expected_event) == actual

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation"))
    async def test_async_export_trigger_exception_should_execute_successfully(
        self, mock_get_operation, trigger, sync_hook_mock
    ):
        """
        Test that CloudSQLExportTrigger fires the correct event in case of an error.
        """
        mock_get_operation.side_effect = Exception("Test exception")

        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "failed", "message": "Test exception"}) == actual

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation"))
    async def test_async_export_trigger_executes_successfully_in_custom_universe(
        self, mock_async_get_op, trigger, sync_hook_mock
    ):
        """
        Test non-default universe trigger correct execution.
        """
        sync_hook_mock.is_default_universe.return_value = False
        sync_hook_mock.get_operation.return_value = {
            "status": "RUNNING",
            "name": OPERATION_NAME,
        }

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.1)

        sync_hook_mock.is_default_universe.assert_called_once()
        sync_hook_mock.get_operation.assert_called_once_with(
            project_id=trigger.project_id, operation_name=trigger.operation_name
        )
        # Verify the default universe branch not being called
        mock_async_get_op.assert_not_called()
        task.cancel()


@pytest.fixture
def no_op_trigger():
    return CloudSQLNoOperationInProgressTrigger(
        instance=INSTANCE,
        project_id=PROJECT_ID,
        impersonation_chain=None,
        gcp_conn_id=TEST_GCP_CONN_ID,
        poke_interval=TEST_POLL_INTERVAL,
        api_version=API_VERSION,
    )


class TestCloudSQLNoOperationInProgressTrigger:
    def test_serialization(self, no_op_trigger, sync_hook_mock):
        classpath, kwargs = no_op_trigger.serialize()
        assert classpath == NO_OP_CLASSPATH
        assert kwargs == {
            "instance": INSTANCE,
            "project_id": PROJECT_ID,
            "impersonation_chain": None,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "poke_interval": TEST_POLL_INTERVAL,
            "api_version": API_VERSION,
        }

    @pytest.mark.asyncio
    async def test_run_success_when_no_operation_in_progress(self, no_op_trigger, sync_hook_mock):
        sync_hook_mock.list_operations.return_value = [
            {"name": "op1", "status": "DONE", "targetId": INSTANCE},
        ]
        actual = await no_op_trigger.run().asend(None)
        assert actual == TriggerEvent({"instance": INSTANCE, "status": "success"})

    @pytest.mark.asyncio
    async def test_run_success_when_only_terminal_operations(self, no_op_trigger, sync_hook_mock):
        # Operations in terminal states (DONE) do not block; the trigger keys off the non-terminal set.
        sync_hook_mock.list_operations.return_value = [
            {"name": "op-done", "status": "DONE", "targetId": INSTANCE},
            {"name": "op-unknown", "status": "UNKNOWN", "targetId": INSTANCE},
        ]
        actual = await no_op_trigger.run().asend(None)
        assert actual == TriggerEvent({"instance": INSTANCE, "status": "success"})

    @pytest.mark.asyncio
    async def test_run_sleeps_while_operation_in_progress(self, no_op_trigger, sync_hook_mock, caplog):
        sync_hook_mock.list_operations.return_value = [
            {"name": "op1", "status": "RUNNING", "targetId": INSTANCE},
        ]
        caplog.set_level(logging.INFO)
        task = asyncio.create_task(no_op_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is False
        assert f"in progress on instance {INSTANCE}" in caplog.text

        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [403, 404])
    async def test_run_fails_fast_on_403_404(self, status, no_op_trigger, sync_hook_mock):
        sync_hook_mock.list_operations.side_effect = HttpError(
            resp=httplib2.Response({"status": status}), content=b"denied or missing"
        )
        actual = await no_op_trigger.run().asend(None)
        assert actual.payload["status"] == "failed"

    @pytest.mark.asyncio
    async def test_run_fails_on_generic_exception(self, no_op_trigger, sync_hook_mock):
        sync_hook_mock.list_operations.side_effect = Exception("boom")
        actual = await no_op_trigger.run().asend(None)
        assert actual == TriggerEvent({"status": "failed", "message": "boom"})
