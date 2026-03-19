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
from unittest.mock import AsyncMock, patch

import pytest

from airflow.providers.redis.triggers.redis_list_await_message import AwaitMessageFromListTrigger


class TestAwaitMessageFromListTrigger:
    def test_trigger_serialization(self):
        trigger = AwaitMessageFromListTrigger(
            lists=["test_list"],
            redis_conn_id="redis_default",
            timeout=10,
        )

        assert isinstance(trigger, AwaitMessageFromListTrigger)

        classpath, kwargs = trigger.serialize()

        assert (
            classpath
            == "airflow.providers.redis.triggers.redis_list_await_message.AwaitMessageFromListTrigger"
        )
        assert kwargs == {
            "lists": ["test_list"],
            "redis_conn_id": "redis_default",
            "timeout": 10,
        }

    def test_trigger_serialization_string_list(self):
        """Test that a single string list name is converted to a list."""
        trigger = AwaitMessageFromListTrigger(lists="single_list")
        _, kwargs = trigger.serialize()
        assert kwargs["lists"] == ["single_list"]

    @patch("airflow.providers.redis.hooks.redis.RedisHook.get_async_conn")
    @pytest.mark.asyncio
    async def test_trigger_run_succeed(self, mock_async_conn):
        mock_redis = AsyncMock()
        mock_redis.brpop.return_value = ("test_list", "test_data")
        mock_redis.aclose = AsyncMock()

        mock_async_conn.return_value.__aenter__ = AsyncMock(return_value=mock_redis)
        mock_async_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        trigger = AwaitMessageFromListTrigger(
            lists=["test_list"],
            redis_conn_id="redis_default",
            timeout=10,
        )

        trigger_gen = trigger.run()
        task = asyncio.create_task(trigger_gen.__anext__())
        event = await task
        assert task.done() is True
        assert event.payload["data"] == "test_data"
        assert event.payload["list"] == "test_list"

    @patch("airflow.providers.redis.hooks.redis.RedisHook.get_async_conn")
    @pytest.mark.asyncio
    async def test_trigger_run_with_bytes(self, mock_async_conn):
        mock_redis = AsyncMock()
        mock_redis.brpop.return_value = (b"test_list", b"test_data")
        mock_redis.aclose = AsyncMock()

        mock_async_conn.return_value.__aenter__ = AsyncMock(return_value=mock_redis)
        mock_async_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        trigger = AwaitMessageFromListTrigger(
            lists=["test_list"],
            redis_conn_id="redis_default",
            timeout=10,
        )

        trigger_gen = trigger.run()
        task = asyncio.create_task(trigger_gen.__anext__())
        event = await task
        assert task.done() is True
        assert event.payload["data"] == "test_data"
        assert event.payload["list"] == "test_list"

    @patch("airflow.providers.redis.hooks.redis.RedisHook.get_async_conn")
    @pytest.mark.asyncio
    async def test_trigger_run_multiple_lists(self, mock_async_conn):
        """Test that trigger checks multiple lists (priority queue behavior)."""
        mock_redis = AsyncMock()
        mock_redis.brpop.return_value = ("high_priority", "urgent_message")
        mock_redis.aclose = AsyncMock()

        mock_async_conn.return_value.__aenter__ = AsyncMock(return_value=mock_redis)
        mock_async_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        trigger = AwaitMessageFromListTrigger(
            lists=["high_priority", "low_priority"],
            redis_conn_id="redis_default",
        )

        trigger_gen = trigger.run()
        task = asyncio.create_task(trigger_gen.__anext__())
        event = await task
        assert event.payload["list"] == "high_priority"
        assert event.payload["data"] == "urgent_message"
        mock_redis.brpop.assert_called_once_with(["high_priority", "low_priority"], timeout=0)
