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

import pytest

from airflow.providers.microsoft.azure.triggers.wasb import (
    WasbBlobSensorTrigger,
    WasbPrefixSensorTrigger,
)
from airflow.triggers.base import TriggerEvent

TEST_DATA_STORAGE_BLOB_NAME = "test_blob_providers_team.txt"
TEST_DATA_STORAGE_CONTAINER_NAME = "test-container-providers-team"
TEST_DATA_STORAGE_BLOB_PREFIX = TEST_DATA_STORAGE_BLOB_NAME[:10]
TEST_WASB_CONN_ID = "wasb_default"
POKE_INTERVAL = 5.0

pytestmark = pytest.mark.db_test


class TestWasbBlobSensorTrigger:
    TRIGGER = WasbBlobSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poke_interval=POKE_INTERVAL,
    )

    def test_serialization(self):
        """
        Asserts that the WasbBlobSensorTrigger correctly serializes its arguments
        and classpath.
        """

        classpath, kwargs = self.TRIGGER.serialize()
        assert (
            classpath
            == "airflow.providers.microsoft.azure.triggers.wasb.WasbBlobSensorTrigger"
        )
        assert kwargs == {
            "container_name": TEST_DATA_STORAGE_CONTAINER_NAME,
            "blob_name": TEST_DATA_STORAGE_BLOB_NAME,
            "wasb_conn_id": TEST_WASB_CONN_ID,
            "poke_interval": POKE_INTERVAL,
            "public_read": False,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "blob_exists",
        [True, False],
    )
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.wasb.WasbAsyncHook.check_for_blob_async"
    )
    async def test_running(self, mock_check_for_blob, blob_exists):
        """
        Test if the task is run in trigger successfully.
        """
        mock_check_for_blob.return_value = blob_exists

        task = asyncio.create_task(self.TRIGGER.run().__anext__())

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.wasb.WasbAsyncHook.check_for_blob_async"
    )
    async def test_success(self, mock_check_for_blob):
        """Tests the success state for that the WasbBlobSensorTrigger."""
        mock_check_for_blob.return_value = True

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was returned
        assert task.done() is True
        asyncio.get_event_loop().stop()

        message = f"Blob {TEST_DATA_STORAGE_BLOB_NAME} found in container {TEST_DATA_STORAGE_CONTAINER_NAME}."
        assert task.result() == TriggerEvent({"status": "success", "message": message})

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.wasb.WasbAsyncHook.check_for_blob_async"
    )
    async def test_waiting_for_blob(self, mock_check_for_blob, caplog):
        """Tests the WasbBlobSensorTrigger sleeps waiting for the blob to arrive."""
        mock_check_for_blob.side_effect = [False, True]
        caplog.set_level(logging.INFO)

        with mock.patch.object(self.TRIGGER.log, "info"):
            task = asyncio.create_task(self.TRIGGER.run().__anext__())

        await asyncio.sleep(POKE_INTERVAL + 0.5)

        if not task.done():
            message = (
                f"Blob {TEST_DATA_STORAGE_BLOB_NAME} not available yet in container {TEST_DATA_STORAGE_CONTAINER_NAME}."
                f" Sleeping for {POKE_INTERVAL} seconds"
            )
            assert message in caplog.text
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.wasb.WasbAsyncHook.check_for_blob_async"
    )
    async def test_trigger_exception(self, mock_check_for_blob):
        """Tests the WasbBlobSensorTrigger yields an error event if there is an exception."""
        mock_check_for_blob.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


class TestWasbPrefixSensorTrigger:
    TRIGGER = WasbPrefixSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_PREFIX,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poke_interval=POKE_INTERVAL,
        check_options={"delimiter": "/", "include": None},
    )

    def test_serialization(self):
        """
        Asserts that the WasbPrefixSensorTrigger correctly serializes its arguments and classpath."""

        classpath, kwargs = self.TRIGGER.serialize()
        assert (
            classpath
            == "airflow.providers.microsoft.azure.triggers.wasb.WasbPrefixSensorTrigger"
        )
        assert kwargs == {
            "container_name": TEST_DATA_STORAGE_CONTAINER_NAME,
            "prefix": TEST_DATA_STORAGE_BLOB_PREFIX,
            "wasb_conn_id": TEST_WASB_CONN_ID,
            "public_read": False,
            "check_options": {
                "delimiter": "/",
                "include": None,
            },
            "poke_interval": POKE_INTERVAL,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_exists",
        [True, False],
    )
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.wasb.WasbAsyncHook.check_for_prefix_async"
    )
    async def test_running(self, mock_check_for_prefix, prefix_exists):
        """Test if the task is run in trigger successfully."""
        mock_check_for_prefix.return_value = prefix_exists

        task = asyncio.create_task(self.TRIGGER.run().__anext__())

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.wasb.WasbAsyncHook.check_for_prefix_async"
    )
    async def test_success(self, mock_check_for_prefix):
        """Tests the success state for that the WasbPrefixSensorTrigger."""
        mock_check_for_prefix.return_value = True

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was returned
        assert task.done() is True
        asyncio.get_event_loop().stop()

        message = f"Prefix {TEST_DATA_STORAGE_BLOB_PREFIX} found in container {TEST_DATA_STORAGE_CONTAINER_NAME}."
        assert task.result() == TriggerEvent({"status": "success", "message": message})

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.wasb.WasbAsyncHook.check_for_prefix_async"
    )
    async def test_waiting_for_blob(self, mock_check_for_prefix):
        """Tests the WasbPrefixSensorTrigger sleeps waiting for the blob to arrive."""
        mock_check_for_prefix.side_effect = [False, True]

        with mock.patch.object(self.TRIGGER.log, "info") as mock_log_info:
            task = asyncio.create_task(self.TRIGGER.run().__anext__())

        await asyncio.sleep(POKE_INTERVAL + 0.5)

        if not task.done():
            message = (
                f"Prefix {TEST_DATA_STORAGE_BLOB_PREFIX} not available yet in container "
                f"{TEST_DATA_STORAGE_CONTAINER_NAME}. Sleeping for {POKE_INTERVAL} seconds"
            )
            mock_log_info.assert_called_once_with(message)
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.wasb.WasbAsyncHook.check_for_prefix_async"
    )
    async def test_trigger_exception(self, mock_check_for_prefix):
        """Tests the WasbPrefixSensorTrigger yields an error event if there is an exception."""
        mock_check_for_prefix.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
