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

from airflow.exceptions import AirflowException
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
                            s3_objects = await self.hook.get_files_async(
                                client, self.bucket_name, self.bucket_key, self.wildcard_match
                            )
                            await asyncio.sleep(self.poke_interval)
                            yield TriggerEvent({"status": "running", "files": s3_objects})
                        else:
                            yield TriggerEvent({"status": "success"})
                        return

                    self.log.info("Sleeping for %s seconds", self.poke_interval)
                    await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class S3KeyUpsertedTrigger(BaseEventTrigger):
    """
    S3KeyUpsertedTrigger is fired as a deferred class with params to run the task in the trigger worker when
    a certain key in an S3 object is changed.

    :param fail_if_missing: if True and key does not exist, an exception will be raised
    """

    def __init__(
        self,
        bucket_name: str,
        bucket_key: str,
        wildcard_match: bool = False,
        aws_conn_id: str | None = "aws_default",
        from_datetime: str | datetime | None = None,
        poke_interval: float = 5.0,
        use_regex: bool = False,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
        fail_on_missing: bool = False,  # TODO: Probably should get removed?
        is_incremental: bool = False,
        process_namespace: str | None = None,
        process_name: str | None = None,
        **kwargs
    ):
        # TODO: Add validation for the bucket_name and bucket_key
        # TODO: Add use_regex and should_check_fn
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.poke_interval = poke_interval
        self.use_regex = use_regex  # TODO: Check this out
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config
        self.fail_on_missing = fail_on_missing
        self.is_incremental = is_incremental

        if self.is_incremental:
            if not (process_namespace and process_name):
                raise AirflowException(
                    "If is_incremental is True, process_namespace and process_name cannot be empty"
            )

        self.process_namespace = process_namespace
        self.process_name = process_name

        self.from_datetime = datetime.fromisoformat(from_datetime) \
            if isinstance(from_datetime, str) \
            else from_datetime

        super().__init__(**kwargs)

    # Will eventually be a serialized instance of this object
    def serialize(self):
        return (
            "airflow.providers.amazon.aws.triggers.s3.S3KeyUpsertedTrigger",
            {
                "bucket_name": self.bucket_name,
                "bucket_key": self.bucket_key,
                "wildcard_match": self.wildcard_match,
                "aws_conn_id": self.aws_conn_id,
                "from_datetime": self.from_datetime.isoformat() \
                    if isinstance(self.from_datetime, datetime) \
                    else self.from_datetime,
                "poke_interval": self.poke_interval,
                "use_regex": self.use_regex,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
            },
        )

    def store_process_state(self, key, value):
        # TODO: This would need to actually "POST" something
        return {
            "process_namespace": self.process_namespace,
            "process_name": self.process_name,
            "process_key": key,
            "process_value": value,
        }

    def get_process_state(self, key, default_value) -> datetime:
        # TODO: This would need to actually "GET" something
        _ = {
            "process_namespace": self.process_namespace,
            "process_name": self.process_name,
            "process_key": key
        }
        return datetime(2025, 1, 1)

    @cached_property
    def hook(self) -> S3Hook:
        return S3Hook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    async def run(self):
        try:
            async with await self.hook.get_async_conn() as client:
                while True:
                    # Check to see if the key exists
                    if await self.hook.check_key_async(
                        client=client,
                        bucket=self.bucket_name,
                        bucket_keys=self.bucket_key,
                        wildcard_match=self.wildcard_match,
                        # not including regex, as it's not available in list_keys
                    ):
                        if self.is_incremental:
                            from_datetime: datetime = self.get_process_state(
                                key="last_check_datetime",
                                default_value=self.from_datetime
                            )
                        else:
                            # If it's not incremental, then it's always going to load using the
                            # self.from_datetime as the lower bound
                            from_datetime = self.from_datetime

                        # The goal here is to be safe and eliminate the risk of potentially missing a file
                        # that's landed. With this approach, a timestamp is actually captured before the
                        # operation takes place. This way, there isn't the possibility of a gap between the
                        # time that the bucket was last checked for changes and the time a new timestamp was
                        # captured
                        _safe_to_datetime = datetime.now(timezone.utc)

                        # If they do, then get those keys (and information)
                        keys_changed: list = self.hook.list_keys(
                            bucket_name=self.bucket_name,
                            prefix=self.bucket_key,
                            from_datetime=from_datetime,
                            to_datetime=_safe_to_datetime,
                            apply_wildcard=self.wildcard_match,  # TODO: Change this
                        )

                        # Regardless if there are or are not keys that have changed, last_activity_time
                        # should still be updated
                        if self.is_incremental:
                            self.store_process_state(key="last_check_datetime", value=_safe_to_datetime)

                        # TODO: Remove this
                        # This will eventually need to go, but maintains parity with pre-persistence logic
                        self.from_datetime = _safe_to_datetime

                        if len(keys_changed) == 0:
                            self.log.info("Sleeping for %s seconds", self.poke_interval)
                            await asyncio.sleep(self.poke_interval)
                            continue

                        # Eventually, we'll want to return more of a rich payload, such that it can be used
                        # by downstream Tasks
                        for key in keys_changed:
                            yield TriggerEvent({
                                "status": "success",
                                "key": key,
                                # "from_datetime": self.from_datetime.isoformat(),
                            })

                        return  # Since we are persisting state, we can now return

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
