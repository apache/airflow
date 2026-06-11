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
"""Caching toolset wrapper for durable execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog
from pydantic_ai.toolsets.wrapper import WrapperToolset

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import ToolsetTool

    from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
    from airflow.providers.common.ai.durable.storage import DurableStorage

log = structlog.get_logger(logger_name="task")


@dataclass
class CachingToolset(WrapperToolset[Any]):
    """
    Wraps a toolset to cache tool call results in ObjectStorage for durable execution.

    On each ``call_tool()`` invocation, checks if a cached result exists for
    the current step index. If so, returns the cached result without executing
    the tool. Otherwise, executes the tool and caches the result.

    The step index is grabbed before the first ``await``, so parallel tool
    calls via ``asyncio.gather`` get deterministic indices (tasks start
    executing their synchronous preamble in creation order).
    """

    storage: DurableStorage = field(repr=False)
    counter: DurableStepCounter = field(repr=False)

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: Any,
        tool: ToolsetTool[Any],
    ) -> Any:
        # Grab step index BEFORE any await -- ensures deterministic ordering
        # even when multiple tool calls run concurrently via asyncio.gather.
        step = self.counter.next_step()
        key = f"tool_step_{step}"

        found, cached = self.storage.load_tool_result(key)
        if found:
            self.counter.replayed_tool += 1
            log.debug("Durable: replayed cached tool result", step=step, tool=name)
            return cached

        result = await self.wrapped.call_tool(name, tool_args, ctx, tool)
        self.storage.save_tool_result(key, result)
        self.counter.cached_tool += 1
        log.debug("Durable: cached tool result", step=step, tool=name)
        return result
