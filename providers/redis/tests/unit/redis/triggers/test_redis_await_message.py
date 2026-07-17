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
from unittest.mock import patch

import pytest

from airflow.providers.redis.triggers.redis_await_message import AwaitMessageTrigger


class TestAwaitMessageTrigger:
    def test_trigger_serialization(self):
        trigger = AwaitMessageTrigger(
            channels=["test_channel"],
            redis_conn_id="redis_default",
            poll_interval=30,
        )

        assert isinstance(trigger, AwaitMessageTrigger)

        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.redis.triggers.redis_await_message.AwaitMessageTrigger"
        assert kwargs == dict(
            channels=["test_channel"],
            redis_conn_id="redis_default",
            poll_interval=30,
        )

    @patch("airflow.providers.redis.hooks.redis.RedisHook.get_conn")
    @pytest.mark.asyncio
    async def test_trigger_run_succeed(self, mock_redis_conn):
        trigger = AwaitMessageTrigger(
            channels="test",
            redis_conn_id="redis_default",
            poll_interval=0.0001,
        )

        mock_redis_conn().pubsub().get_message.return_value = {
            "type": "message",
            "channel": "test",
            "data": "d1",
        }

        trigger_gen = trigger.run()
        task = asyncio.create_task(trigger_gen.__anext__())
        event = await task
        assert task.done() is True
        assert event.payload["data"] == "d1"
        assert event.payload["channel"] == "test"
        asyncio.get_event_loop().stop()

    @patch("airflow.providers.redis.hooks.redis.RedisHook.get_conn")
    @pytest.mark.asyncio
    async def test_trigger_run_succeed_with_bytes(self, mock_redis_conn):
        trigger = AwaitMessageTrigger(
            channels="test",
            redis_conn_id="redis_default",
            poll_interval=0.0001,
        )

        mock_redis_conn().pubsub().get_message.return_value = {
            "type": "message",
            "channel": b"test",
            "data": b"d1",
        }

        trigger_gen = trigger.run()
        task = asyncio.create_task(trigger_gen.__anext__())
        event = await task
        assert task.done() is True
        assert event.payload["data"] == "d1"
        assert event.payload["channel"] == "test"
        asyncio.get_event_loop().stop()

    @patch("airflow.providers.redis.hooks.redis.RedisHook.get_conn")
    @pytest.mark.asyncio
    async def test_trigger_run_fail(self, mock_redis_conn):
        trigger = AwaitMessageTrigger(
            channels="test",
            redis_conn_id="redis_default",
            poll_interval=0.01,
        )

        mock_redis_conn().pubsub().get_message.return_value = {
            "type": "subscribe",
            "channel": "test",
            "data": "d1",
        }

        trigger_gen = trigger.run()
        task = asyncio.create_task(trigger_gen.__anext__())
        await asyncio.sleep(1.0)
        assert task.done() is False
        task.cancel()
        asyncio.get_event_loop().stop()
