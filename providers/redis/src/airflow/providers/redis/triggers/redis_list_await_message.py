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
from typing import Any

from airflow.providers.redis.hooks.redis import RedisHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AwaitMessageFromListTrigger(BaseTrigger):
    """
    A trigger that waits for a message on Redis lists using BRPOP.

    Unlike pub/sub, list-based messaging provides:
    - **Durability**: messages persist in the list until consumed
    - **Exactly-once delivery**: BRPOP atomically removes and returns the message
    - **Priority queues**: when multiple lists are provided, they are checked in order

    :param lists: The Redis list name(s) to monitor. When multiple lists are provided,
        they are checked in order (first list has highest priority).
    :param redis_conn_id: The connection ID to use, defaults to "redis_default"
    :param timeout: BRPOP timeout in seconds. 0 means block indefinitely (default: 0)
    """

    def __init__(
        self,
        lists: list[str] | str,
        redis_conn_id: str = "redis_default",
        timeout: int = 0,
    ) -> None:
        self.lists = [lists] if isinstance(lists, str) else lists
        self.redis_conn_id = redis_conn_id
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.redis.triggers.redis_list_await_message.AwaitMessageFromListTrigger",
            {
                "lists": self.lists,
                "redis_conn_id": self.redis_conn_id,
                "timeout": self.timeout,
            },
        )

    async def run(self):
        hook = RedisHook(redis_conn_id=self.redis_conn_id)

        async with hook.get_async_conn() as redis_conn:
            while True:
                result = await redis_conn.brpop(self.lists, timeout=self.timeout)

                if result is not None:
                    list_name, message = result
                    # Ensure string values (redis.asyncio with decode_responses=True
                    # should already return strings, but handle bytes defensively)
                    if isinstance(list_name, bytes):
                        list_name = list_name.decode("utf-8")
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")

                    yield TriggerEvent({"list": list_name, "data": message})
                    break

                if self.timeout > 0:
                    # BRPOP returned None due to timeout, retry
                    await asyncio.sleep(0)
