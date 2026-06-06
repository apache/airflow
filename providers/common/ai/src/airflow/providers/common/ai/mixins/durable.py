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
"""Durable cache helpers for agent hooks."""

from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.providers.common.ai.utils.callables import is_async_callable

if TYPE_CHECKING:
    from airflow.providers.common.ai.hooks.base import DurableContext


@dataclass
class DurableState:
    """Durable cache storage and counters for one agent run."""

    storage: DurableStorage
    counter: DurableStepCounter


class DurableAgentMixin:
    """Reusable durable step-cache helpers for hooks that support durable execution."""

    def _init_durable(self, ctx: DurableContext) -> DurableState:
        """
        Create and return durable state for *ctx*.

        Hooks call this inside ``_build_agent`` when
        :attr:`~airflow.providers.common.ai.hooks.base.AgentRunRequest.durable_context` is set.
        """
        storage = DurableStorage(
            dag_id=ctx.dag_id,
            task_id=ctx.task_id,
            run_id=ctx.run_id,
            map_index=ctx.map_index,
        )
        return DurableState(storage=storage, counter=DurableStepCounter())

    @staticmethod
    def _cached_callable(
        fn: Callable[..., Any],
        storage: DurableStorage,
        counter: DurableStepCounter,
    ) -> Callable[..., Any]:
        """Wrap *fn* to cache its result in *storage* using a monotonic step counter."""
        if is_async_callable(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args, **kwargs):
                step = counter.next_step()
                key = f"tool_step_{step}"
                found, cached = storage.load_tool_result(key)
                if found:
                    counter.replayed_tool += 1
                    return cached
                result = await fn(*args, **kwargs)
                storage.save_tool_result(key, result)
                counter.cached_tool += 1
                return result

            return async_wrapper

        @functools.wraps(fn)
        def sync_wrapper(*args, **kwargs):
            step = counter.next_step()
            key = f"tool_step_{step}"
            found, cached = storage.load_tool_result(key)
            if found:
                counter.replayed_tool += 1
                return cached
            result = fn(*args, **kwargs)
            storage.save_tool_result(key, result)
            counter.cached_tool += 1
            return result

        return sync_wrapper
