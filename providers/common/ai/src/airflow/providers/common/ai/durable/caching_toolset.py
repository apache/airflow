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

from pydantic_ai.toolsets.wrapper import WrapperToolset

from airflow.providers.common.ai.durable.base import build_tool_step_key
from airflow.providers.common.ai.durable.fingerprint import fingerprint_tool_call
from airflow.providers.common.ai.utils.task_logger import get_task_logger
from airflow.providers.common.ai.utils.tool_metrics import record_tool_call
from airflow.providers.common.ai.utils.toolset_base import AirflowToolset

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool

    from airflow.providers.common.ai.durable.base import DurableStorageProtocol
    from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger
    from airflow.providers.common.ai.durable.step_counter import DurableStepCounter

log = get_task_logger()


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
    discarded and the tool runs live. A call that cannot be fingerprinted is
    neither replayed nor cached: an entry stored without a fingerprint could
    never be verified on a later attempt.

    The step index is grabbed before the first ``await``, so parallel tool
    calls via ``asyncio.gather`` get deterministic indices (tasks start
    executing their synchronous preamble in creation order).

    With a ``replay_usage`` ledger, a replayed call does not count toward the
    run's ``tool_calls`` (see
    :class:`~airflow.providers.common.ai.durable.replay_usage.ReplayUsageLedger`).
    """

    storage: DurableStorageProtocol = field(repr=False)
    counter: DurableStepCounter = field(repr=False)
    replay_usage: ReplayUsageLedger | None = field(default=None, repr=False)

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

        # The toolset a tool came from may declare that a completed call must not be served from
        # cache, because the call acted on a system Airflow cannot observe (a managed agent, for
        # instance). ``tool.toolset`` survives every pydantic-ai wrapper, so the check is per tool
        # and one such toolset inside a combined one does not stop its siblings from replaying.
        # The step still counts so later steps keep their keys.
        if not getattr(_innermost(tool.toolset), "replayable", True):
            log.debug("Durable: toolset is not replayable; running the tool", step=step, tool=name)
            if self.replay_usage is not None:
                self.replay_usage.record_live_tool_call(step)
            return await self.wrapped.call_tool(name, tool_args, ctx, tool)

        key = build_tool_step_key(step)
        fingerprint = fingerprint_tool_call(name, tool_args, ctx.tool_call_id, step=step)
        found, cached, cached_fingerprint = self.storage.load_tool_result(key)
        if found:
            if fingerprint is not None and cached_fingerprint == fingerprint:
                self.counter.replayed_tool += 1
                log.debug("Durable: replayed cached tool result", step=step, tool=name)
                if self.replay_usage is not None:
                    self.replay_usage.record_tool_replay(step)
                leaf = _innermost(self.wrapped)
                if not isinstance(leaf, AirflowToolset):
                    # Inside a combined or dynamic toolset, the tool knows which one it came from.
                    leaf = _innermost(tool.toolset)
                if isinstance(leaf, AirflowToolset):
                    record_tool_call(type(leaf).__name__, "replayed")
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

        if self.replay_usage is not None:
            self.replay_usage.record_live_tool_call(step)
        result = await self.wrapped.call_tool(name, tool_args, ctx, tool)
        # As for model responses, a call that cannot be fingerprinted is not written,
        # and counts as skipped.
        if fingerprint is not None and self.storage.save_tool_result(key, result, fingerprint=fingerprint):
            self.counter.cached_tool += 1
            log.debug("Durable: cached tool result", step=step, tool=name)
        else:
            self.counter.skipped_tools.append(name)
            # Named here rather than only in the end-of-run summary: this warning is
            # logged on every path, including the failed attempt that Airflow retries.
            log.warning(
                "Durable: tool result not cached; a retry runs this tool again, "
                "and may re-run the steps after it",
                step=step,
                tool=name,
            )
        return result


def _innermost(toolset: AbstractToolset[Any]) -> AbstractToolset[Any]:
    """Return the toolset under any wrappers, such as the masking wrapper AgentOperator adds."""
    while isinstance(toolset, WrapperToolset):
        toolset = toolset.wrapped
    return toolset
