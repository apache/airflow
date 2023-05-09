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
import re
from functools import cached_property
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import process_files
from airflow.triggers.base import BaseTrigger, TriggerEvent


class S3KeyTrigger(BaseTrigger):
    """Trigger for S3KeySensor"""

    def __init__(
        self,
        *,
        bucket_key: str | list[str],
        bucket_name: str | None = None,
        wildcard_match: bool = False,
        aws_conn_id: str = "aws_default",
        verify: str | bool | None = None,
        poll_interval: int = 60,
    ):
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.s3.S3KeyTrigger",
            {
                "bucket_key": self.bucket_key,
                "bucket_name": self.bucket_name,
                "wildcard_match": self.wildcard_match,
                "aws_conn_id": self.aws_conn_id,
                "verify": self.verify,
                "poll_interval": self.poll_interval,
            },
        )

    @cached_property
    def hook(self) -> S3Hook:
        return S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

    async def poke(self):
        if isinstance(self.bucket_key, str):
            self.bucket_keys = [self.bucket_key]
        else:
            self.bucket_keys = self.bucket_key

        wildcard_keys = []
        obj = []
        bucket_key_names = []
        for i in range(len(self.bucket_keys)):
            bucket_key_names.append(
                S3Hook.get_s3_bucket_key(self.bucket_name, self.bucket_keys[i], "bucket_name", "bucket_key")
            )
            bucket_name = bucket_key_names[i][0]
            key = bucket_key_names[i][1]
            self.log.info("Poking for key : s3://%s/%s", bucket_name, key)
            if self.wildcard_match:
                prefix = re.split(r"[\[\*\?]", key, 1)[0]
                wildcard_keys.append(await self.hook.get_file_metadata_async(prefix, bucket_name))
            else:
                obj.append(await self.hook.head_object_async(key, bucket_name))

        response = process_files(
            self.bucket_keys, self.wildcard_match, wildcard_keys, obj, None, bucket_key_names
        )
        return [all(response[0]), response[1]]

    async def run(self):
        while True:
            response = await self.poke()
            if response[0]:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": "S3KeyTrigger success",
                        "files_list": response[1],
                    }
                )
            else:
                await asyncio.sleep(int(self.poll_interval))
