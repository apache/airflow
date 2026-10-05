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
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.triggers import BaseEventTrigger
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from datetime import datetime

# Key under which the last-reported object fingerprint is persisted in the asset state store.
WATERMARK_KEY = "etag"


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


class S3KeyUpdateTrigger(BaseEventTrigger):
    """
    Fire an event whenever a single S3 object is updated.

    Polls ``head_object`` for ``bucket_key`` and emits an event when the object's ``ETag``
    differs from the one the trigger last reported, which makes an upload to that key usable
    as a scheduling signal::

        from airflow.sdk import Asset, AssetWatcher

        report = Asset(
            "daily_report",
            watchers=[
                AssetWatcher(
                    name="daily_report_updates",
                    trigger=S3KeyUpdateTrigger(bucket_name="my-bucket", bucket_key="reports/daily.csv"),
                )
            ],
        )


        @dag(schedule=[report])
        def downstream(): ...

    The last-reported ``ETag`` is persisted in the asset state store, so a triggerer restart
    does not re-emit an unchanged object. On the first poll — and after a restart when no
    watermark was kept — the current object is itself the first event, so ``previous_etag`` is
    ``None``. A task that must not run twice for one upload keys on ``etag``.

    The event carries ``bucket_name``, ``bucket_key``, ``etag``, ``previous_etag``,
    ``last_modified`` (ISO-8601) and ``size``. A missing key is not an error: the trigger stays
    silent and keeps polling until the object appears.

    Updates are detected by ``ETag`` (the object's content fingerprint), so re-uploading
    byte-identical content does not fire. For a versioned bucket where every upload must fire,
    enable bucket versioning and the ``ETag`` still changes per upload.

    :param bucket_name: Name of the S3 bucket.
    :param bucket_key: Key of the object to watch.
    :param aws_conn_id: Reference to the S3 connection.
    :param poke_interval: Seconds between polls.
    :param region_name: AWS region for the hook.
    :param verify: Whether to verify SSL certificates for the S3 connection.
    :param botocore_config: Configuration dictionary for the underlying botocore client.
    :param last_seen_etag: ETag already reported. Leave unset to treat the current object as
        the first event.
    :param hook_params: Additional parameters passed to the hook.
    """

    def __init__(
        self,
        *,
        bucket_name: str,
        bucket_key: str,
        aws_conn_id: str | None = "aws_default",
        poke_interval: float = 60,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
        last_seen_etag: str | None = None,
        hook_params: dict | None = None,
    ) -> None:
        super().__init__()
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.aws_conn_id = aws_conn_id
        self.poke_interval = poke_interval
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config
        self.last_seen_etag = last_seen_etag
        self.hook_params = hook_params or {}

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize S3KeyUpdateTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.s3.S3KeyUpdateTrigger",
            {
                "bucket_name": self.bucket_name,
                "bucket_key": self.bucket_key,
                "aws_conn_id": self.aws_conn_id,
                "poke_interval": self.poke_interval,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
                "last_seen_etag": self.last_seen_etag,
                "hook_params": self.hook_params,
            },
        )

    @cached_property
    def hook(self) -> S3Hook:
        return S3Hook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
            **self.hook_params,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the object and emit an event each time its ETag changes."""
        # serialize() is captured once when the trigger row is created, so a value mutated on
        # self is lost when the triggerer restarts and the current object would be re-emitted as
        # an update. The watermark survives that; the kwarg only seeds the first run.
        store = getattr(self, "asset_state_store", None)
        if store is not None:
            stored = await asyncio.to_thread(store.get, WATERMARK_KEY)
            if stored is not None:
                self.last_seen_etag = stored

        async with await self.hook.get_async_conn() as client:
            while True:
                head = await self.hook.get_head_object_async(
                    client=client, key=self.bucket_key, bucket_name=self.bucket_name
                )
                if head is not None:
                    etag = head.get("ETag")
                    if etag is not None and etag != self.last_seen_etag:
                        previous, self.last_seen_etag = self.last_seen_etag, etag
                        if store is not None:
                            await asyncio.to_thread(store.set, WATERMARK_KEY, etag)
                        last_modified = head.get("LastModified")
                        yield TriggerEvent(
                            {
                                "bucket_name": self.bucket_name,
                                "bucket_key": self.bucket_key,
                                "etag": etag,
                                "previous_etag": previous,
                                "last_modified": last_modified.isoformat()
                                if last_modified is not None
                                else None,
                                "size": head.get("ContentLength"),
                            }
                        )
                await asyncio.sleep(self.poke_interval)
