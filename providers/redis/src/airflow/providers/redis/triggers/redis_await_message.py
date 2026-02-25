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

from asgiref.sync import sync_to_async

from airflow.providers.redis.hooks.redis import RedisHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AwaitMessageTrigger(BaseTrigger):
    """
    A trigger that waits for a message matching specific criteria to arrive in Redis.

    The behavior of this trigger is as follows:
    - poll the Redis pubsub for a message, if no message returned, sleep

    :param channels: The channels that should be searched for messages
    :param redis_conn_id: The connection object to use, defaults to "redis_default"
    :param poll_interval: How long the trigger should sleep after reaching the end of the Redis log
        (seconds), defaults to 60
    """

    def __init__(
        self,
        channels: list[str] | str,
        redis_conn_id: str = "redis_default",
        poll_interval: float = 60,
    ) -> None:
        self.channels = channels
        self.redis_conn_id = redis_conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.redis.triggers.redis_await_message.AwaitMessageTrigger",
            {
                "channels": self.channels,
                "redis_conn_id": self.redis_conn_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        hook = RedisHook(redis_conn_id=self.redis_conn_id).get_conn().pubsub()
        hook.subscribe(self.channels)

        async_get_message = sync_to_async(hook.get_message)
        while True:
            message = await async_get_message()

            if message and message["type"] == "message":
                if "channel" in message and isinstance(message["channel"], bytes):
                    message["channel"] = message["channel"].decode("utf-8")
                if "data" in message and isinstance(message["data"], bytes):
                    message["data"] = message["data"].decode("utf-8")
                yield TriggerEvent(message)
                break
            await asyncio.sleep(self.poll_interval)
