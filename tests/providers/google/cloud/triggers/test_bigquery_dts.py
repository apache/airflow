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

import pytest
from google.cloud.bigquery_datatransfer_v1 import TransferState

from airflow.providers.google.cloud.triggers.bigquery_dts import BigQueryDataTransferRunTrigger
from airflow.triggers.base import TriggerEvent
from tests.providers.google.cloud.utils.compat import async_mock

PROJECT_ID = "test-project-id"
CONFIG_ID = "test-config-id"
RUN_ID = "test-run-id"
POLL_INTERVAL = 10
GCP_CONN_ID = "google-cloud-default-id"
LOCATION = "us-central1"
IMPERSONATION_CHAIN = ["test", "chain"]


@pytest.fixture
def trigger():
    trigger = BigQueryDataTransferRunTrigger(
        project_id=PROJECT_ID,
        config_id=CONFIG_ID,
        run_id=RUN_ID,
        poll_interval=POLL_INTERVAL,
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    return trigger


class TestBigQueryDataTransferRunTrigger:
    TRIGGER_MODULE_PATH = "airflow.providers.google.cloud.triggers.bigquery_dts"
    TRIGGER_PATH = f"{TRIGGER_MODULE_PATH}.BigQueryDataTransferRunTrigger"

    def test_serialize(self, trigger):
        classpath, kwargs = trigger.serialize()
        assert classpath == self.TRIGGER_PATH
        assert kwargs == {
            "project_id": PROJECT_ID,
            "config_id": CONFIG_ID,
            "run_id": RUN_ID,
            "poll_interval": POLL_INTERVAL,
            "gcp_conn_id": GCP_CONN_ID,
            "location": LOCATION,
            "impersonation_chain": IMPERSONATION_CHAIN,
        }

    @pytest.mark.parametrize(
        "attr, expected_value",
        [
            ("gcp_conn_id", GCP_CONN_ID),
            ("location", LOCATION),
            ("impersonation_chain", IMPERSONATION_CHAIN),
        ],
    )
    def test_get_async_hook(self, attr, expected_value, trigger):
        hook = trigger._get_async_hook()
        actual_value = hook._hook_kwargs.get(attr)
        assert actual_value is not None
        assert actual_value == expected_value

    @pytest.mark.asyncio
    @async_mock.patch(f"{TRIGGER_MODULE_PATH}.AsyncBiqQueryDataTransferServiceHook.get_transfer_run")
    async def test_run_returns_success_event(self, mock_hook, trigger):
        mock_hook.return_value = async_mock.MagicMock(state=TransferState.SUCCEEDED)
        expected_event = TriggerEvent(
            {
                "run_id": RUN_ID,
                "status": "success",
                "message": "Job completed",
                "config_id": CONFIG_ID,
            }
        )
        actual_event = await (trigger.run()).asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @async_mock.patch(f"{TRIGGER_MODULE_PATH}.AsyncBiqQueryDataTransferServiceHook.get_transfer_run")
    async def test_run_returns_failed_event(self, mock_hook, trigger):
        mock_hook.return_value = async_mock.MagicMock(state=TransferState.FAILED)
        expected_event = TriggerEvent(
            {
                "status": "failed",
                "run_id": RUN_ID,
                "message": "Job has failed",
            }
        )
        actual_event = await (trigger.run()).asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @async_mock.patch(f"{TRIGGER_MODULE_PATH}.AsyncBiqQueryDataTransferServiceHook.get_transfer_run")
    async def test_run_returns_exception_event(self, mock_hook, trigger):
        error_msg = "test error msg"
        mock_hook.side_effect = Exception(error_msg)
        expected_event = TriggerEvent(
            {
                "status": "failed",
                "message": f"Trigger failed with exception: {error_msg}",
            }
        )
        actual_event = await (trigger.run()).asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @async_mock.patch(f"{TRIGGER_MODULE_PATH}.AsyncBiqQueryDataTransferServiceHook.get_transfer_run")
    async def test_run_returns_cancelled_event(self, mock_hook, trigger):
        mock_hook.return_value = async_mock.MagicMock(state=TransferState.CANCELLED)
        expected_event = TriggerEvent(
            {
                "status": "cancelled",
                "run_id": RUN_ID,
                "message": "Job was cancelled",
            }
        )
        actual_event = await (trigger.run()).asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @async_mock.patch(f"{TRIGGER_MODULE_PATH}.AsyncBiqQueryDataTransferServiceHook.get_transfer_run")
    async def test_run_loop_is_still_running(self, mock_hook, trigger, caplog):
        mock_hook.return_value = async_mock.MagicMock(state=TransferState.RUNNING)

        caplog.set_level(logging.INFO)

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert f"Current job status is: {TransferState.RUNNING}"
        assert f"Sleeping for {POLL_INTERVAL} seconds."
