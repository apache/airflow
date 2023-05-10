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
from datetime import datetime

import pytest
from gcloud.aio.storage import Bucket, Storage

from airflow.providers.google.cloud.hooks.gcs import GCSAsyncHook
from airflow.providers.google.cloud.triggers.gcs import (
    GCSBlobTrigger,
    GCSCheckBlobUpdateTimeTrigger,
    GCSPrefixBlobTrigger,
)
from airflow.triggers.base import TriggerEvent
from tests.providers.google.cloud.utils.compat import AsyncMock, async_mock

TEST_BUCKET = "TEST_BUCKET"
TEST_OBJECT = "TEST_OBJECT"
TEST_PREFIX = "TEST_PREFIX"
TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"
TEST_POLLING_INTERVAL = 3.0
TEST_HOOK_PARAMS = {}
TEST_TS_OBJECT = datetime.utcnow()


@pytest.fixture
def trigger():
    return GCSBlobTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        poke_interval=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )


class TestGCSBlobTrigger:
    def test_gcs_blob_trigger_serialization(self, trigger):
        """
        Asserts that the GCSBlobTrigger correctly serializes its arguments
        and classpath.
        """

        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.gcs.GCSBlobTrigger"
        assert kwargs == {
            "bucket": TEST_BUCKET,
            "object_name": TEST_OBJECT,
            "poke_interval": TEST_POLLING_INTERVAL,
            "google_cloud_conn_id": TEST_GCP_CONN_ID,
            "hook_params": TEST_HOOK_PARAMS,
        }

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
    async def test_gcs_blob_trigger_success(self, mock_object_exists, trigger):
        """
        Tests that the GCSBlobTrigger is success case
        """
        mock_object_exists.return_value = "success"

        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success", "message": "success"}) == actual

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
    async def test_gcs_blob_trigger_pending(self, mock_object_exists, trigger):
        """
        Test that GCSBlobTrigger is in loop if file isn't found.
        """
        mock_object_exists.return_value = "pending"

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
    async def test_gcs_blob_trigger_exception(self, mock_object_exists, trigger):
        """
        Tests the GCSBlobTrigger does fire if there is an exception.
        """
        mock_object_exists.side_effect = AsyncMock(side_effect=Exception("Test exception"))

        task = [i async for i in trigger.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exists,response",
        [
            (True, "success"),
            (False, "pending"),
        ],
    )
    async def test_object_exists(self, exists, response, trigger):
        """
        Tests to check if a particular object in Google Cloud Storage
        is found or not
        """
        hook = AsyncMock(GCSAsyncHook)
        storage = AsyncMock(Storage)
        hook.get_storage_client.return_value = storage
        bucket = AsyncMock(Bucket)
        storage.get_bucket.return_value = bucket
        bucket.blob_exists.return_value = exists

        res = await trigger._object_exists(hook, TEST_BUCKET, TEST_OBJECT)
        assert res == response
        bucket.blob_exists.assert_called_once_with(blob_name=TEST_OBJECT)


class TestGCSPrefixBlobTrigger:
    TRIGGER = GCSPrefixBlobTrigger(
        bucket=TEST_BUCKET,
        prefix=TEST_PREFIX,
        poke_interval=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )

    def test_gcs_prefix_blob_trigger_serialization(self):
        """
        Asserts that the GCSPrefixBlobTrigger correctly serializes its arguments
        and classpath.
        """

        classpath, kwargs = self.TRIGGER.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.gcs.GCSPrefixBlobTrigger"
        assert kwargs == {
            "bucket": TEST_BUCKET,
            "prefix": TEST_PREFIX,
            "poke_interval": TEST_POLLING_INTERVAL,
            "google_cloud_conn_id": TEST_GCP_CONN_ID,
            "hook_params": TEST_HOOK_PARAMS,
        }

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.google.cloud.triggers.gcs.GCSPrefixBlobTrigger" "._list_blobs_with_prefix"
    )
    async def test_gcs_prefix_blob_trigger_success(self, mock_list_blobs_with_prefixs):
        """
        Tests that the GCSPrefixBlobTrigger is success case
        """
        mock_list_blobs_with_prefixs.return_value = ["success"]

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent({"status": "success", "message": "Successfully completed", "matches": ["success"]})
            == actual
        )

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.google.cloud.triggers.gcs.GCSPrefixBlobTrigger" "._list_blobs_with_prefix"
    )
    async def test_gcs_prefix_blob_trigger_exception(self, mock_list_blobs_with_prefixs):
        """
        Tests the GCSPrefixBlobTrigger does fire if there is an exception.
        """
        mock_list_blobs_with_prefixs.side_effect = AsyncMock(side_effect=Exception("Test exception"))

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.google.cloud.triggers.gcs.GCSPrefixBlobTrigger" "._list_blobs_with_prefix"
    )
    async def test_gcs_prefix_blob_trigger_pending(self, mock_list_blobs_with_prefixs):
        """
        Test that GCSPrefixBlobTrigger is in loop if file isn't found.
        """
        mock_list_blobs_with_prefixs.return_value = []

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    async def test_list_blobs_with_prefix(self):
        """
        Tests to check if a particular object in Google Cloud Storage
        is found or not
        """
        hook = AsyncMock(GCSAsyncHook)
        storage = AsyncMock(Storage)
        hook.get_storage_client.return_value = storage
        bucket = AsyncMock(Bucket)
        storage.get_bucket.return_value = bucket
        bucket.list_blobs.return_value = ["test_string"]

        res = await self.TRIGGER._list_blobs_with_prefix(hook, TEST_BUCKET, TEST_PREFIX)
        assert res == ["test_string"]
        bucket.list_blobs.assert_called_once_with(prefix=TEST_PREFIX)


class TestGCSCheckBlobUpdateTimeTrigger:
    TRIGGER = GCSCheckBlobUpdateTimeTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        target_date=TEST_TS_OBJECT,
        poke_interval=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )

    def test_gcs_blob_update_trigger_serialization(self):
        """
        Asserts that the GCSCheckBlobUpdateTimeTrigger correctly serializes its arguments
        and classpath.
        """

        classpath, kwargs = self.TRIGGER.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.gcs.GCSCheckBlobUpdateTimeTrigger"
        assert kwargs == {
            "bucket": TEST_BUCKET,
            "object_name": TEST_OBJECT,
            "target_date": TEST_TS_OBJECT,
            "poke_interval": TEST_POLLING_INTERVAL,
            "google_cloud_conn_id": TEST_GCP_CONN_ID,
            "hook_params": TEST_HOOK_PARAMS,
        }

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.google.cloud.triggers.gcs.GCSCheckBlobUpdateTimeTrigger._is_blob_updated_after"
    )
    async def test_gcs_blob_update_trigger_success(self, mock_blob_updated):
        """
        Tests success case GCSCheckBlobUpdateTimeTrigger
        """
        mock_blob_updated.return_value = True, {"status": "success", "message": "success"}

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success", "message": "success"}) == actual

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.google.cloud.triggers.gcs.GCSCheckBlobUpdateTimeTrigger._is_blob_updated_after"
    )
    async def test_gcs_blob_update_trigger_pending(self, mock_blob_updated):
        """
        Test that GCSCheckBlobUpdateTimeTrigger is in loop till file isn't updated.
        """
        mock_blob_updated.return_value = False, {"status": "pending", "message": "pending"}

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.google.cloud.triggers.gcs.GCSCheckBlobUpdateTimeTrigger._is_blob_updated_after"
    )
    async def test_gcs_blob_update_trigger_exception(self, mock_object_exists):
        """
        Tests the GCSCheckBlobUpdateTimeTrigger does fire if there is an exception.
        """
        mock_object_exists.side_effect = AsyncMock(side_effect=Exception("Test exception"))

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "blob_object_update_datetime, ts_object, expected_response",
        [
            (
                "2022-03-07T10:05:43.535Z",
                datetime(2022, 1, 1, 1, 1, 1),
                (True, {"status": "success", "message": "success"}),
            ),
            (
                "2022-03-07T10:05:43.535Z",
                datetime(2022, 3, 8, 1, 1, 1),
                (False, {"status": "pending", "message": "pending"}),
            ),
        ],
    )
    async def test_is_blob_updated_after(self, blob_object_update_datetime, ts_object, expected_response):
        """
        Tests to check if a particular object in Google Cloud Storage
        is found or not
        """
        hook = AsyncMock(GCSAsyncHook)
        storage = AsyncMock(Storage)
        hook.get_storage_client.return_value = storage
        bucket = AsyncMock(Bucket)
        storage.get_bucket.return_value = bucket
        bucket.get_blob.return_value.updated = blob_object_update_datetime
        trigger = GCSCheckBlobUpdateTimeTrigger(
            bucket=TEST_BUCKET,
            object_name=TEST_OBJECT,
            target_date=ts_object,
            poke_interval=TEST_POLLING_INTERVAL,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            hook_params=TEST_HOOK_PARAMS,
        )
        res = await trigger._is_blob_updated_after(hook, TEST_BUCKET, TEST_OBJECT, ts_object)
        assert res == expected_response

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "blob_object, expected_response",
        [
            (
                None,
                (
                    True,
                    {"status": "error", "message": "Object (TEST_OBJECT) not found in Bucket (TEST_BUCKET)"},
                ),
            ),
        ],
    )
    async def test_is_blob_updated_after_with_none(self, blob_object, expected_response):
        """
        Tests to check if a particular object in Google Cloud Storage
        is found or not
        """
        hook = AsyncMock(GCSAsyncHook)
        storage = AsyncMock(Storage)
        hook.get_storage_client.return_value = storage
        bucket = AsyncMock(Bucket)
        storage.get_bucket.return_value = bucket
        bucket.get_blob.return_value = blob_object

        res = await self.TRIGGER._is_blob_updated_after(hook, TEST_BUCKET, TEST_OBJECT, TEST_TS_OBJECT)
        assert res == expected_response
