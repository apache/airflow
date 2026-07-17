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
import os
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from aiohttp import ClientSession

from airflow.providers.google.cloud.hooks.gcs import GCSAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone


class GCSBlobTrigger(BaseTrigger):
    """
    A trigger that fires and it finds the requested file or folder present in the given bucket.

    :param bucket: the bucket in the google cloud storage where the objects are residing.
    :param object_name: the file or folder present in the bucket
    :param use_glob: if true object_name is interpreted as glob
    :param google_cloud_conn_id: reference to the Google Connection
    :param poke_interval: polling period in seconds to check for file/folder
    :param hook_params: Extra config params to be passed to the underlying hook.
            Should match the desired hook constructor params.
    """

    def __init__(
        self,
        bucket: str,
        object_name: str,
        use_glob: bool,
        poke_interval: float,
        google_cloud_conn_id: str,
        hook_params: dict[str, Any],
    ):
        super().__init__()
        self.bucket = bucket
        self.object_name = object_name
        self.use_glob = use_glob
        self.poke_interval = poke_interval
        self.google_cloud_conn_id: str = google_cloud_conn_id
        self.hook_params = hook_params

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize GCSBlobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.gcs.GCSBlobTrigger",
            {
                "bucket": self.bucket,
                "object_name": self.object_name,
                "use_glob": self.use_glob,
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
                    return
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> GCSAsyncHook:
        return GCSAsyncHook(gcp_conn_id=self.google_cloud_conn_id, **self.hook_params)

    async def _object_exists(self, hook: GCSAsyncHook, bucket_name: str, object_name: str) -> str:
        """
        Check for the existence of a file in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the blob_name to check in the Google cloud
            storage bucket.
        """
        async with ClientSession() as s:
            client = await hook.get_storage_client(s)
            bucket = client.get_bucket(bucket_name)
            if self.use_glob:
                list_blobs_response = await bucket.list_blobs(match_glob=object_name)
                if len(list_blobs_response) > 0:
                    return "success"
            else:
                blob_exists_response = await bucket.blob_exists(blob_name=object_name)
                if blob_exists_response:
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
    :param hook_params: dict object that has impersonation_chain
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
        """Serialize GCSCheckBlobUpdateTimeTrigger arguments and classpath."""
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
        """Loop until the object updated time is greater than target datetime."""
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
                    return
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> GCSAsyncHook:
        return GCSAsyncHook(gcp_conn_id=self.google_cloud_conn_id, **self.hook_params)

    async def _is_blob_updated_after(
        self, hook: GCSAsyncHook, bucket_name: str, object_name: str, target_date: datetime
    ) -> tuple[bool, dict[str, Any]]:
        """
        Check if the object in the bucket is updated.

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
            use_glob=False,
        )
        self.prefix = prefix

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize GCSPrefixBlobTrigger arguments and classpath."""
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
                    return
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    async def _list_blobs_with_prefix(self, hook: GCSAsyncHook, bucket_name: str, prefix: str) -> list[str]:
        """
        Return names of blobs which match the given prefix for a given bucket.

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


class GCSUploadSessionTrigger(GCSPrefixBlobTrigger):
    """
    Return Trigger Event if the inactivity period has passed with no increase in the number of objects.

    :param bucket: The Google Cloud Storage bucket where the objects are expected.
    :param prefix: The name of the prefix to check in the Google cloud storage bucket.
    :param poke_interval: polling period in seconds to check
    :param inactivity_period: The total seconds of inactivity to designate
        an upload session is over. Note, this mechanism is not real time and
        this operator may not return until a interval after this period
        has passed with no additional objects sensed.
    :param min_objects: The minimum number of objects needed for upload session
        to be considered valid.
    :param previous_objects: The set of object ids found during the last poke.
    :param allow_delete: Should this sensor consider objects being deleted
        between intervals valid behavior. If true a warning message will be logged
        when this happens. If false an error will be raised.
    :param google_cloud_conn_id: The connection ID to use when connecting
        to Google Cloud Storage.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str,
        poke_interval: float,
        google_cloud_conn_id: str,
        hook_params: dict[str, Any],
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: set[str] | None = None,
        allow_delete: bool = True,
    ):
        super().__init__(
            bucket=bucket,
            prefix=prefix,
            poke_interval=poke_interval,
            google_cloud_conn_id=google_cloud_conn_id,
            hook_params=hook_params,
        )
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_objects = previous_objects or set()
        self.inactivity_seconds = 0.0
        self.allow_delete = allow_delete
        self.last_activity_time: datetime | None = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize GCSUploadSessionTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.gcs.GCSUploadSessionTrigger",
            {
                "bucket": self.bucket,
                "prefix": self.prefix,
                "poke_interval": self.poke_interval,
                "google_cloud_conn_id": self.google_cloud_conn_id,
                "hook_params": self.hook_params,
                "inactivity_period": self.inactivity_period,
                "min_objects": self.min_objects,
                "previous_objects": self.previous_objects,
                "allow_delete": self.allow_delete,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until no new files or deleted files in list blob for the inactivity_period."""
        try:
            hook = self._get_async_hook()
            while True:
                list_blobs = await self._list_blobs_with_prefix(
                    hook=hook, bucket_name=self.bucket, prefix=self.prefix
                )
                res = self._is_bucket_updated(set(list_blobs))
                if res["status"] in ("success", "error"):
                    yield TriggerEvent(res)
                    return
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_time(self) -> datetime:
        """
        Get current local date and time.

        This is just a wrapper of datetime.datetime.now to simplify mocking in the unittests.
        """
        return datetime.now()

    def _is_bucket_updated(self, current_objects: set[str]) -> dict[str, str]:
        """
        Check whether new objects have been uploaded and the inactivity_period has passed; update the state.

        :param current_objects: set of object ids in bucket during last check.
        """
        current_num_objects = len(current_objects)
        if current_objects > self.previous_objects:
            # When new objects arrived, reset the inactivity_seconds
            # and update previous_objects for the next check interval.
            self.log.info(
                "New objects found at %s resetting last_activity_time.",
                os.path.join(self.bucket, self.prefix),
            )
            self.log.debug("New objects: %s", "\n".join(current_objects - self.previous_objects))
            self.last_activity_time = self._get_time()
            self.inactivity_seconds = 0
            self.previous_objects = current_objects
            return {"status": "pending"}

        if self.previous_objects - current_objects:
            # During the last interval check objects were deleted.
            if self.allow_delete:
                self.previous_objects = current_objects
                self.last_activity_time = self._get_time()
                self.log.warning(
                    "%s Objects were deleted during the last interval."
                    " Updating the file counter and resetting last_activity_time.",
                    self.previous_objects - current_objects,
                )
                return {"status": "pending"}
            return {
                "status": "error",
                "message": "Illegal behavior: objects were deleted in between check intervals",
            }
        if self.last_activity_time:
            self.inactivity_seconds = (self._get_time() - self.last_activity_time).total_seconds()
        else:
            # Handles the first check where last inactivity time is None.
            self.last_activity_time = self._get_time()
            self.inactivity_seconds = 0

        if self.inactivity_seconds >= self.inactivity_period:
            path = os.path.join(self.bucket, self.prefix)

            if current_num_objects >= self.min_objects:
                success_message = (
                    "SUCCESS: Sensor found %s objects at %s. Waited at least %s "
                    "seconds, with no new objects dropped."
                )
                self.log.info(success_message, current_num_objects, path, self.inactivity_seconds)
                return {
                    "status": "success",
                    "message": success_message % (current_num_objects, path, self.inactivity_seconds),
                }

            error_message = "FAILURE: Inactivity Period passed, not enough objects found in %s"
            self.log.error(error_message, path)
            return {"status": "error", "message": error_message % path}
        return {"status": "pending"}
