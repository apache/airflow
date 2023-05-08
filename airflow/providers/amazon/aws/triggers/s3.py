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
from typing import Any, AsyncIterator, Callable

from airflow.providers.amazon.aws.hooks.s3 import S3AsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class S3KeyTrigger(BaseTrigger):
    """
    S3KeyTrigger is fired as deferred class with params to run the task in trigger worker

    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param bucket_key:  The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :param aws_conn_id: reference to the s3 connection
    :param hook_params: params for hook its optional
    :param check_fn: Function that receives the list of the S3 objects,
        and returns a boolean
    """

    def __init__(
        self,
        bucket_name: str,
        bucket_key: str | list[str],
        wildcard_match: bool = False,
        check_fn: Callable[..., bool] | None = None,
        aws_conn_id: str = "aws_default",
        poke_interval: float = 5.0,
        **hook_params: Any,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.check_fn = check_fn
        self.aws_conn_id = aws_conn_id
        self.hook_params = hook_params
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize S3KeyTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.s3.S3KeyTrigger",
            {
                "bucket_name": self.bucket_name,
                "bucket_key": self.bucket_key,
                "wildcard_match": self.wildcard_match,
                "check_fn": self.check_fn,
                "aws_conn_id": self.aws_conn_id,
                "hook_params": self.hook_params,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make an asynchronous connection using S3HookAsync."""
        try:
            hook = self._get_async_hook()
            async with await hook.get_client_async() as client:
                while True:
                    if await hook.check_key(client, self.bucket_name, self.bucket_key, self.wildcard_match):
                        if self.check_fn is None:
                            yield TriggerEvent({"status": "success"})
                        else:
                            s3_objects = await hook.get_files(
                                client, self.bucket_name, self.bucket_key, self.wildcard_match
                            )
                            yield TriggerEvent({"status": "success", "s3_objects": s3_objects})
                    await asyncio.sleep(self.poke_interval)

        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> S3AsyncHook:
        return S3AsyncHook(aws_conn_id=self.aws_conn_id, verify=self.hook_params.get("verify"))
