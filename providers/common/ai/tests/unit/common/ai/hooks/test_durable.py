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
import functools
import inspect
from unittest.mock import MagicMock

from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.providers.common.ai.mixins.durable import DurableAgentMixin


class TestDurableAgentMixinCachedCallable:
    def test_cached_callable_saves_and_returns(self):
        storage = MagicMock(spec=DurableStorage)
        counter = MagicMock(spec=DurableStepCounter)
        counter.next_step.return_value = 1
        counter.cached_tool = 0
        storage.load_tool_result.return_value = (False, None)

        calls = []

        def fn():
            calls.append(1)
            return "computed"

        wrapped = DurableAgentMixin._cached_callable(fn, storage, counter)
        result = wrapped()

        assert result == "computed"
        assert calls == [1]
        storage.save_tool_result.assert_called_once_with("tool_step_1", "computed")

    def test_cached_callable_replays_on_hit(self):
        storage = MagicMock(spec=DurableStorage)
        counter = MagicMock(spec=DurableStepCounter)
        counter.replayed_tool = 0
        counter.next_step.return_value = 1
        storage.load_tool_result.return_value = (True, "cached_value")

        calls = []

        def fn():
            calls.append(1)
            return "computed"

        wrapped = DurableAgentMixin._cached_callable(fn, storage, counter)
        result = wrapped()

        assert result == "cached_value"
        assert calls == []
        assert counter.replayed_tool == 1
        storage.save_tool_result.assert_not_called()

    def test_cached_callable_preserves_async_function_behavior(self):
        storage = MagicMock(spec=DurableStorage)
        counter = MagicMock(spec=DurableStepCounter)
        counter.cached_tool = 0
        counter.next_step.return_value = 1
        storage.load_tool_result.return_value = (False, None)

        async def fn(value):
            return value * 2

        wrapped = DurableAgentMixin._cached_callable(fn, storage, counter)

        assert inspect.iscoroutinefunction(wrapped)
        assert asyncio.run(wrapped(3)) == 6
        storage.save_tool_result.assert_called_once_with("tool_step_1", 6)
        assert counter.cached_tool == 1

    def test_cached_callable_preserves_async_partial_behavior(self):
        storage = MagicMock(spec=DurableStorage)
        counter = MagicMock(spec=DurableStepCounter)
        counter.cached_tool = 0
        counter.next_step.return_value = 1
        storage.load_tool_result.return_value = (False, None)

        async def fn(prefix, value):
            return f"{prefix}:{value}"

        wrapped = DurableAgentMixin._cached_callable(functools.partial(fn, "prod"), storage, counter)

        assert inspect.iscoroutinefunction(wrapped)
        assert asyncio.run(wrapped("cpu")) == "prod:cpu"
        storage.save_tool_result.assert_called_once_with("tool_step_1", "prod:cpu")
        assert counter.cached_tool == 1

    def test_cached_callable_preserves_async_callable_object_behavior(self):
        storage = MagicMock(spec=DurableStorage)
        counter = MagicMock(spec=DurableStepCounter)
        counter.cached_tool = 0
        counter.next_step.return_value = 1
        storage.load_tool_result.return_value = (False, None)

        class Lookup:
            async def __call__(self, value):
                return value.upper()

        wrapped = DurableAgentMixin._cached_callable(Lookup(), storage, counter)

        assert inspect.iscoroutinefunction(wrapped)
        assert asyncio.run(wrapped("abc")) == "ABC"
        storage.save_tool_result.assert_called_once_with("tool_step_1", "ABC")
        assert counter.cached_tool == 1
