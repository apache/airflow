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

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX
from airflow.providers.common.ai.durable.fingerprint import fingerprint_tool_call

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import ToolsetTool

    from airflow.providers.common.ai.durable.base import DurableStorageProtocol
    from airflow.providers.common.ai.durable.step_counter import DurableStepCounter

log = structlog.get_logger(logger_name="task")


@dataclass
class CachingToolset(WrapperToolset[Any]):
    """
    Wraps a toolset to cache tool call results in ObjectStorage for durable execution.

    On each ``call_tool()`` invocation, checks if a cached result exists for
    the current step index and was produced by the same call (same tool name,
    arguments, and model-issued ``tool_call_id`` -- compared via fingerprint).
    If so, returns the cached result without executing the tool. Otherwise,
    executes the tool and caches the result. A fingerprint mismatch means the
    conversation diverged from the previous attempt; the stale entry is
    discarded and the tool runs live.

    The step index is grabbed before the first ``await``, so parallel tool
    calls via ``asyncio.gather`` get deterministic indices (tasks start
    executing their synchronous preamble in creation order).
    """

    storage: DurableStorageProtocol = field(repr=False)
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
        key = f"{DURABLE_KEY_PREFIX}tool_step_{step}"
        fingerprint = fingerprint_tool_call(name, tool_args, ctx.tool_call_id)

        found, cached, cached_fingerprint = self.storage.load_tool_result(key)
        if found:
            if cached_fingerprint == fingerprint:
                self.counter.replayed_tool += 1
                log.debug("Durable: replayed cached tool result", step=step, tool=name)
                return cached
            log.warning(
                "Durable: cached tool result does not match the current tool call; "
                "re-running the tool instead of replaying",
                step=step,
                tool=name,
                reason=(
                    "entry predates fingerprinting or the call could not be fingerprinted"
                    if fingerprint is None or cached_fingerprint is None
                    else "the conversation diverged from the previous attempt"
                ),
            )

        result = await self.wrapped.call_tool(name, tool_args, ctx, tool)
        self.storage.save_tool_result(key, result, fingerprint=fingerprint)
        self.counter.cached_tool += 1
        log.debug("Durable: cached tool result", step=step, tool=name)
        return result
