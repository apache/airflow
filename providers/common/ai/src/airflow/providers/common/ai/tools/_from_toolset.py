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
"""Expose a pydantic-ai toolset's tools as framework-neutral :class:`AirflowTool` objects."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import threading
import time
from collections.abc import Hashable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError
from pydantic_ai import RunContext
from pydantic_ai.exceptions import ModelRetry, ToolFailed
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult
from airflow.providers.common.ai.utils.coroutines import run_coroutine_sync

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool


# Set by a framework adapter around each call: which agent run and which model turn the call
# belongs to, where the framework says. The retry budget counts by them.
_call_scope: ContextVar[tuple[Hashable | None, Hashable | None]] = ContextVar(
    "common_ai_tool_call_scope", default=(None, None)
)


@contextmanager
def tool_call_scope(*, run: Hashable | None, turn: Hashable | None = None) -> Iterator[None]:
    """
    Tell the tools which agent run and which model turn the calls inside the block belong to.

    Each tool's retry budget starts over when the run changes, and counts at most one failure
    per turn, as pydantic-ai does. Without a turn, calls that run at the same time count as one.
    """
    token = _call_scope.set((run, turn))
    try:
        yield
    finally:
        _call_scope.reset(token)


def airflow_tools_from_toolset(toolset: AbstractToolset[Any], *, deps: Any = None) -> list[AirflowTool]:
    """
    Return one :class:`AirflowTool` per tool in ``toolset``.

    Each tool validates its arguments with the toolset's own validator and awaits the
    toolset's ``call_tool`` on the caller's event loop, so the toolset's behaviour
    (connection resolution, SQL validation, ``allowed_tables``, bounded results) is
    unchanged. The errors pydantic-ai feeds back to the model inside its own runs are
    fed back the same way here: argument validation failures and ``ModelRetry`` become
    error results, bounded by the tool's ``max_retries``, and ``ToolFailed`` becomes an
    error result without using up that budget. Anything else raised by the toolset
    propagates. Tools the toolset marks ``sequential`` run one at a time, in the order
    the calls were made, as they do in a pydantic-ai run.

    Outside a pydantic-ai run there is no live ``RunContext``, so an inert one with a
    placeholder model is passed. That is enough for toolsets whose ``call_tool`` ignores
    the context, which is true of every toolset this provider exposes this way.

    :param toolset: The pydantic-ai toolset to expose.
    :param deps: Exposed to the toolset as ``ctx.deps``.
    """
    ctx: RunContext[Any] = RunContext(deps=deps, model=TestModel(), usage=RunUsage())
    toolset_tools = run_coroutine_sync(toolset.get_tools(ctx))
    in_order = _InOrder()
    return [_as_airflow_tool(toolset, name, tool, ctx, in_order) for name, tool in toolset_tools.items()]


class _InOrder:
    """
    Run calls one at a time, in the order they were made, across threads and event loops.

    A call takes its place in line before its first await, which is the order a framework
    started it in. A call that has to wait awaits a future of its own, which the call ahead of
    it completes when it is done. The future is thread-safe, since frameworks may make calls
    from different event loops, and waiting holds no thread, so a long line of waiting calls
    cannot starve the thread pool the running call needs.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_ticket = 0
        self._serving = 0
        self._turns: dict[int, concurrent.futures.Future[None]] = {}
        self._abandoned: set[int] = set()

    def take(self) -> int:
        with self._lock:
            ticket = self._next_ticket
            self._next_ticket += 1
            if ticket != self._serving:
                self._turns[ticket] = concurrent.futures.Future()
            return ticket

    async def wait(self, ticket: int) -> None:
        with self._lock:
            turn = self._turns.get(ticket)
        if turn is None:
            return
        try:
            await asyncio.wrap_future(turn)
        except BaseException:
            self.done(ticket)
            raise

    def done(self, ticket: int) -> None:
        with self._lock:
            if self._serving != ticket:
                # Cancelled while waiting: skip its turn when it comes.
                self._abandoned.add(ticket)
                self._turns.pop(ticket, None)
                return
            self._serving += 1
            while self._serving in self._abandoned:
                self._abandoned.discard(self._serving)
                self._serving += 1
            upcoming = self._turns.pop(self._serving, None)
        if upcoming is not None:
            # A call cancelled at this moment has a cancelled future; its own done() moves on.
            with contextlib.suppress(concurrent.futures.InvalidStateError):
                upcoming.set_result(None)


class _RetryBudget:
    """
    Consecutive failures the model may correct, counted the way pydantic-ai counts them.

    pydantic-ai counts at most one failure per tool per model turn, and starts every run
    with a fresh count. The run and the turn come from :func:`tool_call_scope` where the
    framework's adapter sets it. Without a turn, a failure of a call that ran alongside the
    last counted failure is not counted again, since frameworks run a turn's calls at once.
    """

    def __init__(self, max_retries: int) -> None:
        self.max_retries = max_retries
        self._failures = 0
        self._run: Hashable | None = None
        self._failed_turn: Hashable | None = None
        self._counted_until = float("-inf")

    def start(self, run: Hashable | None) -> None:
        """Start the count over when a call belongs to a new run."""
        if run is not None and run != self._run:
            self._run = run
            self._failures = 0
            self._failed_turn = None

    def succeeded(self, turn: Hashable | None) -> None:
        # A success does not clear a failure of the same turn.
        if turn is None or turn != self._failed_turn:
            self._failures = 0

    def failed(self, started: float, turn: Hashable | None) -> bool:
        """Record a failed call that started at ``started``; return whether the budget is spent."""
        new = turn != self._failed_turn if turn is not None else started >= self._counted_until
        if new:
            self._failures += 1
            self._failed_turn = turn
            self._counted_until = time.monotonic()
        if self._failures <= self.max_retries:
            return False
        # Start over, so a tool list reused without a run from the adapter is not already spent.
        self._failures = 0
        return True


def _as_airflow_tool(
    toolset: AbstractToolset[Any],
    name: str,
    tool: ToolsetTool[Any],
    ctx: RunContext[Any],
    in_order: _InOrder,
) -> AirflowTool:
    budget = _RetryBudget(tool.max_retries)

    def correctable(started: float, turn: Hashable | None, message: str) -> ToolResult:
        if budget.failed(started, turn):
            raise ToolCallError(f"{name} kept failing after {budget.max_retries} correction(s): {message}")
        return ToolResult(content=message, is_error=True)

    async def call(arguments: dict[str, Any], started: float, turn: Hashable | None) -> ToolResult:
        try:
            validated = tool.args_validator.validate_python(arguments)
        except ValidationError as e:
            return correctable(started, turn, str(e))
        # A ValidationError raised by the tool itself is not the model's to fix: the call
        # may already have had a side effect, so it propagates rather than inviting a retry.
        try:
            result = await toolset.call_tool(name, validated, ctx, tool)
        except ModelRetry as e:
            return correctable(started, turn, e.message)
        except ToolFailed as e:
            return ToolResult(content=e.message, is_error=True)
        budget.succeeded(turn)
        return ToolResult(content=result)

    async def function(arguments: dict[str, Any]) -> ToolResult:
        started = time.monotonic()
        run, turn = _call_scope.get()
        budget.start(run)
        if not tool.tool_def.sequential:
            # Validation does not await, so without this a call the framework started alongside
            # this one would not have started yet when this one fails, and the budget would
            # count both failures of one turn.
            await asyncio.sleep(0)
            return await call(arguments, started, turn)
        ticket = in_order.take()
        await in_order.wait(ticket)
        try:
            return await call(arguments, started, turn)
        finally:
            in_order.done(ticket)

    return AirflowTool(
        name=name,
        description=tool.tool_def.description or name,
        parameters=tool.tool_def.parameters_json_schema,
        function=function,
    )
