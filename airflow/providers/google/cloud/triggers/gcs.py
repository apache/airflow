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

import asyncio
from datetime import datetime
from typing import Any, AsyncIterator

from aiohttp import ClientSession

from airflow.providers.google.cloud.hooks.gcs import GCSAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone


class GCSBlobTrigger(BaseTrigger):
    """
    A trigger that fires and it finds the requested file or folder present in the given bucket.

    :param bucket: the bucket in the google cloud storage where the objects are residing.
    :param object_name: the file or folder present in the bucket
    :param google_cloud_conn_id: reference to the Google Connection
    :param poke_interval: polling period in seconds to check for file/folder
    :param hook_params: Extra config params to be passed to the underlying hook.
            Should match the desired hook constructor params.
    """

    def __init__(
        self,
        bucket: str,
        object_name: str,
        poke_interval: float,
        google_cloud_conn_id: str,
        hook_params: dict[str, Any],
    ):
        super().__init__()
        self.bucket = bucket
        self.object_name = object_name
        self.poke_interval = poke_interval
        self.google_cloud_conn_id: str = google_cloud_conn_id
        self.hook_params = hook_params

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes GCSBlobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.gcs.GCSBlobTrigger",
            {
                "bucket": self.bucket,
                "object_name": self.object_name,
                "poke_interval": self.poke_interval,
                "google_cloud_conn_id": self.google_cloud_conn_id,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the relevant file/folder is found."""
        try:
            hook = self._get_async_hook()
            while True:
                res = await self._object_exists(
                    hook=hook, bucket_name=self.bucket, object_name=self.object_name
                )
                if res == "success":
                    yield TriggerEvent({"status": "success", "message": res})
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    def _get_async_hook(self) -> GCSAsyncHook:
        return GCSAsyncHook(gcp_conn_id=self.google_cloud_conn_id, **self.hook_params)

    async def _object_exists(self, hook: GCSAsyncHook, bucket_name: str, object_name: str) -> str:
        """
        Checks for the existence of a file in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the blob_name to check in the Google cloud
            storage bucket.
        """
        async with ClientSession() as s:
            client = await hook.get_storage_client(s)
            bucket = client.get_bucket(bucket_name)
            object_response = await bucket.blob_exists(blob_name=object_name)
            if object_response:
                return "success"
            return "pending"


class GCSCheckBlobUpdateTimeTrigger(BaseTrigger):
    """
    A trigger that makes an async call to GCS to check whether the object is updated in a bucket.

    :param bucket: google cloud storage bucket name cloud storage where the objects are residing.
    :param object_name: the file or folder present in the bucket
    :param target_date: context datetime to compare with blob object updated time
    :param poke_interval: polling period in seconds to check for file/folder
    :param google_cloud_conn_id: reference to the Google Connection
    :param hook_params: dict object that has delegate_to and impersonation_chain
    """

    def __init__(
        self,
        bucket: str,
        object_name: str,
        target_date: datetime,
        poke_interval: float,
        google_cloud_conn_id: str,
        hook_params: dict[str, Any],
    ):
        super().__init__()
        self.bucket = bucket
        self.object_name = object_name
        self.target_date = target_date
        self.poke_interval = poke_interval
        self.google_cloud_conn_id: str = google_cloud_conn_id
        self.hook_params = hook_params

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes GCSCheckBlobUpdateTimeTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.gcs.GCSCheckBlobUpdateTimeTrigger",
            {
                "bucket": self.bucket,
                "object_name": self.object_name,
                "target_date": self.target_date,
                "poke_interval": self.poke_interval,
                "google_cloud_conn_id": self.google_cloud_conn_id,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the object updated time is greater than target datetime"""
        try:
            hook = self._get_async_hook()
            while True:
                status, res = await self._is_blob_updated_after(
                    hook=hook,
                    bucket_name=self.bucket,
                    object_name=self.object_name,
                    target_date=self.target_date,
                )
                if status:
                    yield TriggerEvent(res)
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> GCSAsyncHook:
        return GCSAsyncHook(gcp_conn_id=self.google_cloud_conn_id, **self.hook_params)

    async def _is_blob_updated_after(
        self, hook: GCSAsyncHook, bucket_name: str, object_name: str, target_date: datetime
    ) -> tuple[bool, dict[str, Any]]:
        """
        Checks if the object in the bucket is updated.

        :param hook: GCSAsyncHook Hook class
        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the blob_name to check in the Google cloud
            storage bucket.
        :param target_date: context datetime to compare with blob object updated time
        """
        async with ClientSession() as session:
            client = await hook.get_storage_client(session)
            bucket = client.get_bucket(bucket_name)
            blob = await bucket.get_blob(blob_name=object_name)
            if blob is None:
                res = {
                    "message": f"Object ({object_name}) not found in Bucket ({bucket_name})",
                    "status": "error",
                }
                return True, res

            blob_updated_date = blob.updated  # type: ignore[attr-defined]
            blob_updated_time = datetime.strptime(blob_updated_date, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=timezone.utc
            )  # Blob updated time is in string format so converting the string format
            # to datetime object to compare the last updated time

            if blob_updated_time is not None:
                if not target_date.tzinfo:
                    target_date = target_date.replace(tzinfo=timezone.utc)
                self.log.info("Verify object date: %s > %s", blob_updated_time, target_date)
                if blob_updated_time > target_date:
                    return True, {"status": "success", "message": "success"}
            return False, {"status": "pending", "message": "pending"}


class GCSPrefixBlobTrigger(GCSBlobTrigger):
    """
    Looks for objects in bucket matching a prefix.
    If none found, sleep for interval and check again. Otherwise, return matches.

    :param bucket: the bucket in the google cloud storage where the objects are residing.
    :param prefix: The prefix of the blob_names to match in the Google cloud storage bucket
    :param google_cloud_conn_id: reference to the Google Connection
    :param poke_interval: polling period in seconds to check
    :param hook_params: Extra config params to be passed to the underlying hook.
            Should match the desired hook constructor params.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str,
        poke_interval: float,
        google_cloud_conn_id: str,
        hook_params: dict[str, Any],
    ):
        super().__init__(
            bucket=bucket,
            object_name=prefix,
            poke_interval=poke_interval,
            google_cloud_conn_id=google_cloud_conn_id,
            hook_params=hook_params,
        )
        self.prefix = prefix

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes GCSPrefixBlobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.gcs.GCSPrefixBlobTrigger",
            {
                "bucket": self.bucket,
                "prefix": self.prefix,
                "poke_interval": self.poke_interval,
                "google_cloud_conn_id": self.google_cloud_conn_id,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the matches are found for the given prefix on the bucket."""
        try:
            hook = self._get_async_hook()
            while True:
                self.log.info(
                    "Checking for existence of blobs with prefix  %s in bucket %s", self.prefix, self.bucket
                )
                res = await self._list_blobs_with_prefix(
                    hook=hook, bucket_name=self.bucket, prefix=self.prefix
                )
                if len(res) > 0:
                    yield TriggerEvent(
                        {"status": "success", "message": "Successfully completed", "matches": res}
                    )
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    async def _list_blobs_with_prefix(self, hook: GCSAsyncHook, bucket_name: str, prefix: str) -> list[str]:
        """
        Returns names of blobs which match the given prefix for a given bucket.

        :param hook: The async hook to use for listing the blobs
        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param prefix: The prefix of the blob_names to match in the Google cloud
            storage bucket.
        """
        async with ClientSession() as session:
            client = await hook.get_storage_client(session)
            bucket = client.get_bucket(bucket_name)
            object_response = await bucket.list_blobs(prefix=prefix)
            return object_response
