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
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from functools import cached_property
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.triggers.base import BaseEventTrigger, BaseTrigger, TriggerEvent


class S3KeyTrigger(BaseTrigger):
    """
    S3KeyTrigger is fired as deferred class with params to run the task in trigger worker.

    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param bucket_key:  The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :param aws_conn_id: reference to the s3 connection
    :param use_regex: whether to use regex to check bucket
    :param metadata_keys: List of head_object attributes to gather and send to ``check_fn``.
        Acceptable values: Any top level attribute returned by s3.head_object. Specify * to return
        all available attributes.
        Default value: "Size".
        If the requested attribute is not found, the key is still included and the value is None.
    :param hook_params: params for hook its optional
    """

    def __init__(
        self,
        bucket_name: str,
        bucket_key: str | list[str],
        wildcard_match: bool = False,
        aws_conn_id: str | None = "aws_default",
        poke_interval: float = 5.0,
        should_check_fn: bool = False,
        use_regex: bool = False,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
        metadata_keys: list[str] | None = None,
        **hook_params: Any,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.hook_params = hook_params
        self.poke_interval = poke_interval
        self.should_check_fn = should_check_fn
        self.use_regex = use_regex
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config
        self.metadata_keys = metadata_keys if metadata_keys else ["Size", "Key"]

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize S3KeyTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.s3.S3KeyTrigger",
            {
                "bucket_name": self.bucket_name,
                "bucket_key": self.bucket_key,
                "wildcard_match": self.wildcard_match,
                "aws_conn_id": self.aws_conn_id,
                "hook_params": self.hook_params,
                "poke_interval": self.poke_interval,
                "should_check_fn": self.should_check_fn,
                "use_regex": self.use_regex,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
                "metadata_keys": self.metadata_keys,
            },
        )

    @cached_property
    def hook(self) -> S3Hook:
        return S3Hook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make an asynchronous connection using S3HookAsync."""
        try:
            async with await self.hook.get_async_conn() as client:
                while True:
                    if await self.hook.check_key_async(
                        client, self.bucket_name, self.bucket_key, self.wildcard_match, self.use_regex
                    ):
                        if self.should_check_fn:
                            raw_objects = await self.hook.get_files_async(
                                client, self.bucket_name, self.bucket_key, self.wildcard_match
                            )
                            files = []
                            for f in raw_objects:
                                metadata = {}
                                obj = await self.hook.get_head_object_async(
                                    client=client, key=f, bucket_name=self.bucket_name
                                )
                                if obj is None:
                                    return

                                if "*" in self.metadata_keys:
                                    metadata = obj
                                else:
                                    for mk in self.metadata_keys:
                                        if mk == "Size":
                                            metadata[mk] = obj.get("ContentLength")
                                        else:
                                            metadata[mk] = obj.get(mk, None)
                                metadata["Key"] = f
                                files.append(metadata)
                            await asyncio.sleep(self.poke_interval)
                            yield TriggerEvent({"status": "running", "files": files})
                        else:
                            yield TriggerEvent({"status": "success"})
                        return

                    self.log.info("Sleeping for %s seconds", self.poke_interval)
                    await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class S3KeyEventTrigger(BaseEventTrigger):
    """
    Watch an S3 prefix for new or updated keys and fire an event per matched key.

    Intended for use as an :class:`~airflow.sdk.AssetWatcher` trigger: each poll lists keys under
    ``prefix`` with a ``LastModified`` after the stored watermark, fires one ``TriggerEvent`` per
    matched key, then advances the watermark to the latest ``LastModified`` seen.

    :param bucket_name: name of the S3 bucket to watch.
    :param prefix: key prefix to watch. Supports a trailing ``*`` wildcard when ``wildcard_match``
        is ``True``.
    :param wildcard_match: whether ``prefix`` should be interpreted as a wildcard pattern.
    :param start_after_last_key: whether to additionally use the last matched key as S3's
        ``StartAfter`` cursor on the next poll. Only safe when the bucket's key naming convention
        is lexicographically sortable by upload time (e.g. date-prefixed paths); otherwise a new
        key that sorts alphabetically before the last-seen key would be silently skipped.
    :param aws_conn_id: reference to the S3 connection.
    :param poke_interval: seconds to sleep between polls when no new keys are found.
    :param region_name: AWS region name.
    :param verify: whether to verify SSL certificates for the S3 connection.
    :param botocore_config: additional botocore config to pass to the underlying S3 hook.
    :param metadata_keys: list of S3 object attributes to include in each event's ``file`` payload.
        Specify ``["*"]`` to include all available attributes. Defaults to ``["Size", "Key", "LastModified"]``.
    :param hook_params: additional params to pass to the underlying S3 hook.
    """

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        wildcard_match: bool = False,
        start_after_last_key: bool = False,
        aws_conn_id: str | None = "aws_default",
        poke_interval: float = 30.0,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
        metadata_keys: list[str] | None = None,
        **hook_params: Any,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.wildcard_match = wildcard_match
        self.start_after_last_key = start_after_last_key
        self.aws_conn_id = aws_conn_id
        self.hook_params = hook_params
        self.poke_interval = poke_interval
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config
        self.metadata_keys = metadata_keys if metadata_keys else ["Size", "Key", "LastModified"]

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize S3KeyEventTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.s3.S3KeyEventTrigger",
            {
                "bucket_name": self.bucket_name,
                "prefix": self.prefix,
                "wildcard_match": self.wildcard_match,
                "start_after_last_key": self.start_after_last_key,
                "aws_conn_id": self.aws_conn_id,
                "hook_params": self.hook_params,
                "poke_interval": self.poke_interval,
                "should_check_fn": self.should_check_fn,
                "use_regex": self.use_regex,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
                "metadata_keys": self.metadata_keys,
            },
        )

    @cached_property
    def hook(self) -> S3Hook:
        return S3Hook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    @staticmethod
    def fix_max_key(keys: list[dict]) -> dict:
        """Return the key dict with the latest LastModified value."""
        return max(keys, key=lambda k: k["LastModified"])

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Fire TriggerEvent's if there are new files in the S3 bucket."""
        # Retrieve the Asset state to store and retrieve watermarking information
        asset_state_store = self.asset_state_store
        stored_from_datetime = asset_state_store.get("from_datetime")
        from_datetime: datetime | None = None  # Look for files from this datetime onwards

        # Assume that the last stored datetime is where we should be looking from
        if stored_from_datetime is not None:
            from_datetime = datetime.fromisoformat(stored_from_datetime)

            if from_datetime.tzinfo is None:
                from_datetime = from_datetime.replace(tzinfo=timezone.utc)

        # This is the alphabetical key that would be the "starting point" for new files, if specified by user
        start_after_key = asset_state_store.get("start_after_key") if self.start_after_last_key else None

        while True:
            upserted_files = await self.hook.list_keys_async(
                bucket_name=self.bucket_name,
                prefix=self.prefix,
                delimiter=None,
                page_size=None,
                max_items=None,
                start_after_key=start_after_key,
                from_datetime=from_datetime,
                to_datetime=None,  # No cap, always looking up until the present
                object_filter=None,
                apply_wildcard=self.wildcard_match,
            )

            if upserted_files:
                for f in upserted_files:
                    # Create the "file" payload that is going to be returned
                    file_metadata = (
                        f if "*" in self.metadata_keys else {k: f[k] for k in self.metadata_keys if k in f}
                    )

                    if "LastModified" in file_metadata:
                        file_metadata["LastModified"] = (
                            file_metadata["LastModified"].astimezone(timezone.utc).isoformat()
                        )

                    yield TriggerEvent({"status": "success", "file": file_metadata})

                # Update the from_datetime value to use next time around when filtering
                max_key: dict = self.fix_max_key(upserted_files)
                new_from_datetime: datetime = max_key["LastModified"]
                asset_state_store.set("from_datetime", new_from_datetime.astimezone(timezone.utc).isoformat())

                # Update the start_after_key
                if self.start_after_last_key:
                    new_start_after_key = max_key.get("Key")
                    asset_state_store.set("start_after_key", new_start_after_key)

                return

            await asyncio.sleep(self.poke_interval)


class S3KeysUnchangedTrigger(BaseTrigger):
    """
    S3KeysUnchangedTrigger is fired as deferred class with params to run the task in trigger worker.

    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param prefix: The prefix being waited on. Relative path from bucket root level.
    :param inactivity_period: The total seconds of inactivity to designate
        keys unchanged. Note, this mechanism is not real time and
        this operator may not return until a poke_interval after this period
        has passed with no additional objects sensed.
    :param min_objects: The minimum number of objects needed for keys unchanged
        sensor to be considered valid.
    :param inactivity_seconds: reference to the seconds of inactivity
    :param previous_objects: The set of object ids found during the last poke.
    :param allow_delete: Should this sensor consider objects being deleted
    :param aws_conn_id: reference to the s3 connection
    :param last_activity_time: last modified or last active time
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
    :param hook_params: params for hook its optional
    """

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        inactivity_seconds: int = 0,
        previous_objects: set[str] | None = None,
        allow_delete: bool = True,
        aws_conn_id: str | None = "aws_default",
        last_activity_time: datetime | None = None,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
        **hook_params: Any,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.prefix = prefix
        if inactivity_period < 0:
            raise ValueError("inactivity_period must be non-negative")
        if not previous_objects:
            previous_objects = set()
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_objects = previous_objects
        self.inactivity_seconds = inactivity_seconds
        self.allow_delete = allow_delete
        self.aws_conn_id = aws_conn_id
        self.last_activity_time = last_activity_time
        self.polling_period_seconds = 0
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config
        self.hook_params = hook_params

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize S3KeysUnchangedTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.s3.S3KeysUnchangedTrigger",
            {
                "bucket_name": self.bucket_name,
                "prefix": self.prefix,
                "inactivity_period": self.inactivity_period,
                "min_objects": self.min_objects,
                "previous_objects": self.previous_objects,
                "inactivity_seconds": self.inactivity_seconds,
                "allow_delete": self.allow_delete,
                "aws_conn_id": self.aws_conn_id,
                "last_activity_time": self.last_activity_time,
                "hook_params": self.hook_params,
                "polling_period_seconds": self.polling_period_seconds,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
            },
        )

    @cached_property
    def hook(self) -> S3Hook:
        return S3Hook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make an asynchronous connection using S3Hook."""
        try:
            async with await self.hook.get_async_conn() as client:
                while True:
                    result = await self.hook.is_keys_unchanged_async(
                        client=client,
                        bucket_name=self.bucket_name,
                        prefix=self.prefix,
                        inactivity_period=self.inactivity_period,
                        min_objects=self.min_objects,
                        previous_objects=self.previous_objects,
                        inactivity_seconds=self.inactivity_seconds,
                        allow_delete=self.allow_delete,
                        last_activity_time=self.last_activity_time,
                    )
                    if result.get("status") in ("success", "error"):
                        yield TriggerEvent(result)
                        return
                    elif result.get("status") == "pending":
                        self.previous_objects = result.get("previous_objects", set())
                        self.last_activity_time = result.get("last_activity_time")
                        self.inactivity_seconds = result.get("inactivity_seconds", 0)
                    await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
