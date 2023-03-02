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

import pytest
from gcloud.aio.storage import Bucket, Storage

from airflow.providers.google.cloud.hooks.gcs import GCSAsyncHook
from airflow.providers.google.cloud.triggers.gcs import GCSBlobTrigger
from airflow.triggers.base import TriggerEvent
from tests.providers.google.cloud.utils.compat import AsyncMock, async_mock

TEST_BUCKET = "TEST_BUCKET"
TEST_OBJECT = "TEST_OBJECT"
TEST_PREFIX = "TEST_PREFIX"
TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"
TEST_POLLING_INTERVAL = 3.0
TEST_HOOK_PARAMS = {}


def test_gcs_blob_trigger_serialization():
    """
    Asserts that the GCSBlobTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )
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
async def test_gcs_blob_trigger_success(mock_object_exists):
    """
    Tests that the GCSBlobTrigger is success case
    """
    mock_object_exists.return_value = "success"

    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success", "message": "success"}) == actual


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
async def test_gcs_blob_trigger_pending(mock_object_exists):
    """
    Test that GCSBlobTrigger is in loop if file isn't found.
    """
    mock_object_exists.return_value = "pending"

    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
async def test_gcs_blob_trigger_exception(mock_object_exists):
    """
    Tests the GCSBlobTrigger does fire if there is an exception.
    """
    mock_object_exists.side_effect = AsyncMock(side_effect=Exception("Test exception"))
    trigger = GCSBlobTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        poke_interval=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )
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
async def test_object_exists(exists, response):
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
    trigger = GCSBlobTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        poke_interval=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )
    res = await trigger._object_exists(hook, TEST_BUCKET, TEST_OBJECT)
    assert res == response
    bucket.blob_exists.assert_called_once_with(blob_name=TEST_OBJECT)
