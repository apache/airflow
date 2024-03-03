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
from unittest import mock as async_mock

import pytest

from airflow.providers.google.cloud.triggers.cloud_sql import CloudSQLExportTrigger
from airflow.triggers.base import TriggerEvent

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


@pytest.fixture
def trigger():
    return CloudSQLExportTrigger(
        operation_name=OPERATION_NAME,
        project_id=PROJECT_ID,
        impersonation_chain=None,
        gcp_conn_id=TEST_GCP_CONN_ID,
        poke_interval=TEST_POLL_INTERVAL,
    )


class TestCloudSQLExportTrigger:
    def test_async_export_trigger_serialization_should_execute_successfully(self, trigger):
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
        }

    @pytest.mark.asyncio
    @async_mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation"))
    async def test_async_export_trigger_on_success_should_execute_successfully(
        self, mock_get_operation, trigger
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
    @async_mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation"))
    async def test_async_export_trigger_running_should_execute_successfully(
        self, mock_get_operation, trigger, caplog
    ):
        """
        Test that CloudSQLExportTrigger does not fire while a job is still running.
        """

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
    @async_mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation"))
    async def test_async_export_trigger_error_should_execute_successfully(self, mock_get_operation, trigger):
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
    @async_mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation"))
    async def test_async_export_trigger_exception_should_execute_successfully(
        self, mock_get_operation, trigger
    ):
        """
        Test that CloudSQLExportTrigger fires the correct event in case of an error.
        """
        mock_get_operation.side_effect = Exception("Test exception")

        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "failed", "message": "Test exception"}) == actual
