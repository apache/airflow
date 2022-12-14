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
import sys
from asyncio import Future

import pytest
from google.cloud.container_v1.types import Operation

from airflow.providers.google.cloud.triggers.kubernetes_engine import GKEOperationTrigger
from airflow.triggers.base import TriggerEvent

if sys.version_info < (3, 8):
    from asynctest import mock
else:
    from unittest import mock


OPERATION_NAME = "test-operation-name"
PROJECT_ID = "test-project-id"
LOCATION = "us-central1-c"
GCP_CONN_ID = "test-non-existing-project-id"
DELEGATE_TO = "test-delegate-to"
IMPERSONATION_CHAIN = ["impersonate", "this", "test"]
POLL_INTERVAL = 5
TRIGGER_PATH = "airflow.providers.google.cloud.triggers.kubernetes_engine.GKEOperationTrigger"
EXC_MSG = "test error msg"


@pytest.fixture
def trigger():
    return GKEOperationTrigger(
        operation_name=OPERATION_NAME,
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        delegate_to=DELEGATE_TO,
        impersonation_chain=IMPERSONATION_CHAIN,
        poll_interval=POLL_INTERVAL,
    )


@pytest.fixture()
def async_get_operation_result():
    def func(**kwargs):
        m = mock.MagicMock()
        m.configure_mock(**kwargs)
        f = Future()
        f.set_result(m)
        return f

    return func


class TestGKEOperationTrigger:
    def test_serialize(self, trigger):
        classpath, trigger_init_kwargs = trigger.serialize()
        assert classpath == TRIGGER_PATH
        assert trigger_init_kwargs == {
            "operation_name": OPERATION_NAME,
            "project_id": PROJECT_ID,
            "location": LOCATION,
            "gcp_conn_id": GCP_CONN_ID,
            "delegate_to": DELEGATE_TO,
            "impersonation_chain": IMPERSONATION_CHAIN,
            "poll_interval": POLL_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._get_hook")
    async def test_run_loop_return_success_event(self, mock_hook, trigger, async_get_operation_result):
        mock_hook.return_value.get_operation.return_value = async_get_operation_result(
            name=OPERATION_NAME,
            status=Operation.Status.DONE,
        )

        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": "Operation is successfully ended.",
                "operation_name": OPERATION_NAME,
            }
        )
        actual_event = await (trigger.run()).asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._get_hook")
    async def test_run_loop_return_failed_event_status_unspecified(
        self,
        mock_hook,
        trigger,
        async_get_operation_result,
    ):
        mock_hook.return_value.get_operation.return_value = async_get_operation_result(
            name=OPERATION_NAME,
            status=Operation.Status.STATUS_UNSPECIFIED,
        )

        expected_event = TriggerEvent(
            {
                "status": "failed",
                "message": f"Operation has failed with status: {Operation.Status.STATUS_UNSPECIFIED}",
            }
        )
        actual_event = await (trigger.run()).asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._get_hook")
    async def test_run_loop_return_failed_event_status_aborting(
        self,
        mock_hook,
        trigger,
        async_get_operation_result,
    ):
        mock_hook.return_value.get_operation.return_value = async_get_operation_result(
            name=OPERATION_NAME,
            status=Operation.Status.ABORTING,
        )

        expected_event = TriggerEvent(
            {
                "status": "failed",
                "message": f"Operation has failed with status: {Operation.Status.ABORTING}",
            }
        )
        actual_event = await (trigger.run()).asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._get_hook")
    async def test_run_loop_return_error_event(self, mock_hook, trigger, async_get_operation_result):
        mock_hook.return_value.get_operation.side_effect = Exception(EXC_MSG)

        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": EXC_MSG,
            }
        )
        actual_event = await (trigger.run()).asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._get_hook")
    async def test_run_loop_return_waiting_event_pending_status(
        self,
        mock_hook,
        trigger,
        async_get_operation_result,
        caplog,
    ):
        mock_hook.return_value.get_operation.return_value = async_get_operation_result(
            name=OPERATION_NAME,
            status=Operation.Status.PENDING,
        )

        caplog.set_level(logging.INFO)

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert "Operation is still running."
        assert f"Sleeping for {POLL_INTERVAL}s..."

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._get_hook")
    async def test_run_loop_return_waiting_event_running_status(
        self,
        mock_hook,
        trigger,
        async_get_operation_result,
        caplog,
    ):
        mock_hook.return_value.get_operation.return_value = async_get_operation_result(
            name=OPERATION_NAME,
            status=Operation.Status.RUNNING,
        )

        caplog.set_level(logging.INFO)

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert "Operation is still running."
        assert f"Sleeping for {POLL_INTERVAL}s..."
