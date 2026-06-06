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
from unittest import mock

import pytest

from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook
from airflow.providers.google.cloud.triggers.cloud_sql import CloudSQLNoOperationInProgressTrigger
from airflow.triggers.base import TriggerEvent

CLASSPATH = "airflow.providers.google.cloud.triggers.cloud_sql.CloudSQLNoOperationInProgressTrigger"
HOOK_STR = "airflow.providers.google.cloud.hooks.cloud_sql.{}"
PROJECT_ID = "test-project"
INSTANCE = "test-instance"
GCP_CONN_ID = "test-gcp-conn-id"
IMPERSONATION_CHAIN = ["impersonate@service.account"]
API_VERSION = "v1beta4"
POKE_INTERVAL = 10


@pytest.fixture
def no_operation_trigger():
    return CloudSQLNoOperationInProgressTrigger(
        project_id=PROJECT_ID,
        instance=INSTANCE,
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
        poke_interval=POKE_INTERVAL,
        api_version=API_VERSION,
    )


@pytest.fixture
def sync_hook_mock():
    mock_obj = mock.MagicMock(spec=CloudSQLHook)
    with mock.patch(
        HOOK_STR.format("CloudSQLAsyncHook.get_sync_hook"), new_callable=mock.AsyncMock
    ) as patched:
        patched.return_value = mock_obj
        yield mock_obj


class TestCloudSQLNoOperationInProgressTrigger:
    def test_serialize(self, no_operation_trigger):
        classpath, kwargs = no_operation_trigger.serialize()

        assert classpath == CLASSPATH
        assert kwargs == {
            "project_id": PROJECT_ID,
            "instance": INSTANCE,
            "gcp_conn_id": GCP_CONN_ID,
            "impersonation_chain": IMPERSONATION_CHAIN,
            "poke_interval": POKE_INTERVAL,
            "api_version": API_VERSION,
        }

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_instance_operations"))
    async def test_fires_success_when_no_instance_operations_are_running(
        self, mock_get_instance_operations, no_operation_trigger, sync_hook_mock
    ):
        sync_hook_mock.is_default_universe.return_value = True
        mock_get_instance_operations.return_value = [
            {"name": "done-operation", "targetId": INSTANCE, "status": "DONE"},
            {"name": "other-instance-operation", "targetId": "another-instance", "status": "RUNNING"},
        ]

        generator = no_operation_trigger.run()
        actual = await generator.asend(None)

        assert (
            TriggerEvent({"status": "success", "message": f"No operations are running for {INSTANCE}"})
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_instance_operations"))
    async def test_keeps_waiting_when_instance_operation_is_running(
        self, mock_get_instance_operations, no_operation_trigger, sync_hook_mock
    ):
        sync_hook_mock.is_default_universe.return_value = True
        mock_get_instance_operations.return_value = [
            {"name": "running-operation", "targetId": INSTANCE, "status": "RUNNING"}
        ]

        task = asyncio.create_task(no_operation_trigger.run().__anext__())
        await asyncio.sleep(0.1)

        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_instance_operations"))
    async def test_fires_failed_on_exception(
        self, mock_get_instance_operations, no_operation_trigger, sync_hook_mock
    ):
        sync_hook_mock.is_default_universe.return_value = True
        mock_get_instance_operations.side_effect = Exception("test exception")

        generator = no_operation_trigger.run()
        actual = await generator.asend(None)

        assert TriggerEvent({"status": "failed", "message": "test exception"}) == actual
