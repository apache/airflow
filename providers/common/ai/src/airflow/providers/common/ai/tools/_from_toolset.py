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
import logging
import math
import threading
import time
from collections.abc import Hashable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, TypedDict, TypeGuard

from pydantic import ValidationError
from pydantic_ai import RunContext
from pydantic_ai.exceptions import ModelRetry, ToolFailed
from pydantic_ai.models.test import TestModel
from pydantic_ai.toolsets.approval_required import ApprovalRequiredToolset
from pydantic_ai.toolsets.filtered import FilteredToolset
from pydantic_ai.toolsets.prefixed import PrefixedToolset
from pydantic_ai.toolsets.renamed import RenamedToolset
from pydantic_ai.toolsets.wrapper import WrapperToolset
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult
from airflow.providers.common.ai.utils.coroutines import run_coroutine_sync

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool

log = logging.getLogger(__name__)


class RetryCount(TypedDict):
    """The corrections a tool had used when a call finished, and when it finished."""

    failures: int
    # Wall-clock time, not monotonic: a retry compares it with times recorded by earlier tries,
    # which may have run on other workers.
    finished: float


@dataclass(slots=True)
class ToolCallScope:
    """Which agent run and model turn a tool call belongs to, and what it did to the tool's retry budget."""

    run: Hashable | None = None
    turn: Hashable | None = None
    # The tool function the adapter calls. Only its own count is recorded, not that of a toolset
    # tool a custom function calls in turn.
    function: Callable[[dict[str, Any]], Awaitable[ToolResult]] | None = None
    # Set by a call that failed or succeeded; None when it left the count alone, such as a ToolFailed.
    counted: RetryCount | None = None
    # Set when the call failed once more than the budget allows, which is the model's doing.
    limit_reached: bool = False


# Set by a framework adapter around each call. The retry budget counts by its run and turn.
_call_scope: ContextVar[ToolCallScope | None] = ContextVar("common_ai_tool_call_scope", default=None)


@contextmanager
def tool_call_scope(
    *,
    run: Hashable | None,
    turn: Hashable | None = None,
    function: Callable[[dict[str, Any]], Awaitable[ToolResult]] | None = None,
) -> Iterator[ToolCallScope]:
    """
    Tell the tools which agent run and which model turn the calls inside the block belong to.

    Each tool's retry budget starts over when the run changes, and counts at most one failure
    per turn, as pydantic-ai does. Without a turn, calls that run at the same time count as one.

    Given the ``function`` of the tool the block calls, the block yields the count that call left
    in the tool's retry budget, so durable execution can record it with the call's result and
    restore it through ``retry_count_restorer`` when a retry replays that result.
    """
    scope = ToolCallScope(run=run, turn=turn, function=function)
    token = _call_scope.set(scope)
    try:
        yield scope
    finally:
        _call_scope.reset(token)


# Set on the function of each tool this module builds: restores a count an earlier attempt recorded.
_RESTORE_RETRY_COUNT = "_airflow_restore_retry_count"


def retry_count_restorer(tool: AirflowTool) -> Callable[[Hashable, RetryCount], Awaitable[None]] | None:
    """
    Return what restores a count an earlier attempt recorded into ``tool``'s retry budget.

    It takes the agent run the replay belongs to and the count, and never runs the tool.
    ``None`` for a tool with no retry budget, such as one with a function of its own.
    """
    return getattr(tool.function, _RESTORE_RETRY_COUNT, None)


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
    the context, which is true of every toolset this provider exposes this way. Its
    ``max_retries`` is the pydantic-ai agent's default of one, the budget a tool gets
    when its toolset takes it from the run. Each call then gets its own copy carrying
    that tool's ``retry`` count and ``max_retries``, as inside a pydantic-ai run.

    :param toolset: The pydantic-ai toolset to expose.
    :param deps: Exposed to the toolset as ``ctx.deps``.
    """
    ctx = _build_inert_context(deps)
    toolset_tools = run_coroutine_sync(toolset.get_tools(ctx))
    in_order = _InOrder()
    return [_as_airflow_tool(toolset, name, tool, ctx, in_order) for name, tool in toolset_tools.items()]


def _build_inert_context(deps: Any = None) -> RunContext[Any]:
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage(), max_retries=1)


# The wrappers that only choose which tools the model is offered, and under what name.
_NAMING_WRAPPERS = (FilteredToolset, PrefixedToolset, RenamedToolset)


def _is_naming_wrapper(
    toolset: object,
) -> TypeGuard[FilteredToolset[Any] | PrefixedToolset[Any] | RenamedToolset[Any]]:
    # Exact types: the tools reached around a wrapper skip its call_tool, which a subclass may override.
    return type(toolset) in _NAMING_WRAPPERS


def resolve_airflow_tools(source: object) -> list[AirflowTool]:
    """
    Return the tools of a toolset a framework adapter was given.

    A source with an ``airflow_tools()`` method is asked for them. A toolset wrapped with
    ``.filtered()``, ``.prefixed()`` or ``.renamed()``, nested in any order, gives the innermost
    toolset's own ``airflow_tools()``, so they behave as they do without the wrappers. The
    wrappers' own ``get_tools`` decides which of them the model is offered and under what name,
    given the same inert ``RunContext`` :func:`airflow_tools_from_toolset` passes. A ``.filtered()``
    function therefore runs once, when the adapter is built, rather than before every model turn
    as in a pydantic-ai run.

    :raises TypeError: when ``source`` is any other wrapper, such as ``.approval_required()``,
        or there is no ``airflow_tools()`` method under its wrappers.
    :raises ValueError: when a ``.renamed()`` map sends an offered name to a name the toolset does
        not have, such as a map written backwards.
    """
    # Looked up, not checked against ToolProvider: since Python 3.12 a Protocol check misses a
    # method that a mock or proxy only provides through __getattr__.
    own = getattr(source, "airflow_tools", None)
    if callable(own):
        return list(own())
    if not _is_naming_wrapper(source):
        raise TypeError(_describe_unsupported_toolset(source))
    renames: list[PrefixedToolset[Any] | RenamedToolset[Any]] = []
    inner: AbstractToolset[Any] = source
    while _is_naming_wrapper(inner):
        if not isinstance(inner, FilteredToolset):
            renames.append(inner)
        inner = inner.wrapped
    provide = getattr(inner, "airflow_tools", None)
    if not callable(provide):
        raise TypeError(_describe_unsupported_toolset(inner))
    by_name = {tool.name: tool for tool in provide()}
    offered = run_coroutine_sync(source.get_tools(_build_inert_context()))
    tools = []
    for name in offered:
        # Outermost wrapper first, the order their ``call_tool`` methods undo it in.
        original = name
        for wrapper in renames:
            if isinstance(wrapper, PrefixedToolset):
                original = original.removeprefix(f"{wrapper.prefix}_")
            else:
                original = wrapper.name_map.get(original, original)
        if original not in by_name:
            raise ValueError(
                f"The tool offered as {name!r} maps back to {original!r}, which "
                f"{type(inner).__name__} does not have. .renamed() maps each new name to the "
                "toolset's own name, as in .renamed({'run_sql': 'query'})."
            )
        tools.append(replace(by_name[original], name=name))
    return tools


def _describe_unsupported_toolset(toolset: object) -> str:
    if isinstance(toolset, ApprovalRequiredToolset):
        return (
            "AirflowTools cannot carry ApprovalRequiredToolset (.approval_required()) to another "
            "framework's agent: only a Pydantic AI run pauses for the approval it asks for. Pass the "
            "toolset without it and use the framework's own tool approval, such as needs_approval "
            "in the OpenAI Agents SDK, whose guide says how it behaves in a task, or run the agent "
            "with AgentOperator."
        )

    kind = type(toolset).__name__
    if isinstance(toolset, WrapperToolset):
        return (
            f"AirflowTools cannot read through {kind}. Only .filtered(), .prefixed() and .renamed() "
            "carry over to another framework's agent; pass the toolset without it."
        )
    return (
        f"AirflowTools cannot take a {kind}: it has no airflow_tools() method. Pass AirflowTool "
        "objects or an Airflow toolset such as SQLToolset, which .filtered(), .prefixed() and "
        ".renamed() may wrap."
    )


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
        # When the call whose count this is finished, live or in an earlier attempt, and whether a
        # call has counted live in this run.
        self._finished = float("-inf")
        self._live = False

    @property
    def failures(self) -> int:
        """Consecutive failures counted so far, what pydantic-ai passes a tool as ``ctx.retry``."""
        return self._failures

    def start(self, run: Hashable | None) -> None:
        """Start the count over when a call belongs to a new run."""
        if run is not None and run != self._run:
            self._run = run
            self._failures = 0
            self._failed_turn = None
            self._finished = float("-inf")
            self._live = False

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

    def take(self) -> RetryCount:
        """Return the count a call that just failed or succeeded left, for durable execution to record."""
        # Later than every count restored before it even when this worker's clock is behind the
        # one an earlier attempt ran on, so a later retry orders the attempts' counts as they ran.
        # A live call that counts before a replayed one of its turn restores is ordered by the
        # clocks alone, which holds while workers' clocks differ by less than the time between tries.
        self._finished = max(time.time(), math.nextafter(self._finished, math.inf))
        self._live = True
        return {"failures": self._failures, "finished": self._finished}

    def restore(self, count: RetryCount) -> None:
        """
        Take a count an earlier attempt recorded, unless a call that finished later left the current one.

        A replay reaches calls in the order they started, but the attempt counted them in the
        order they finished, which differs for calls that ran alongside each other. A call that
        runs live finished after any an earlier attempt recorded.
        """
        if count["failures"]:
            # A call running alongside this failure fails in the same turn, as it did live.
            self._counted_until = time.monotonic()
        if not self._live and count["finished"] > self._finished:
            self._finished = count["finished"]
            self._failures = count["failures"]


def _as_airflow_tool(
    toolset: AbstractToolset[Any],
    name: str,
    tool: ToolsetTool[Any],
    ctx: RunContext[Any],
    in_order: _InOrder,
) -> AirflowTool:
    budget = _RetryBudget(tool.max_retries)

    def correctable(started: float, scope: ToolCallScope, message: str) -> ToolResult:
        if budget.failed(started, scope.turn):
            scope.limit_reached = True
            raise ToolCallError(f"{name} kept failing after {budget.max_retries} correction(s): {message}")
        scope.counted = budget.take()
        return ToolResult(content=message, is_error=True)

    async def restore(run: Hashable, count: RetryCount) -> None:
        budget.start(run)
        # Let the calls the framework started alongside this one start first, so a failure among
        # them counts in the same turn as the one this count ends on.
        await asyncio.sleep(0)
        restored = budget.failures
        budget.restore(count)
        if budget.failures != restored:
            log.info(
                "Tool %s: an earlier attempt left %d of its %d correction(s) used",
                name,
                budget.failures,
                budget.max_retries,
            )

    async def call(arguments: dict[str, Any], started: float, scope: ToolCallScope) -> ToolResult:
        try:
            validated = tool.args_validator.validate_python(arguments)
        except ValidationError as e:
            return correctable(started, scope, str(e))
        # A ValidationError raised by the tool itself is not the model's to fix: the call
        # may already have had a side effect, so it propagates rather than inviting a retry.
        # Per call, as pydantic-ai's ToolManager does, so ``ctx.retry`` and ``ctx.last_attempt``
        # mean the same here as inside a pydantic-ai run.
        call_ctx = replace(ctx, retry=budget.failures, max_retries=tool.max_retries)
        try:
            result = await toolset.call_tool(name, validated, call_ctx, tool)
        except ModelRetry as e:
            return correctable(started, scope, e.message)
        except ToolFailed as e:
            return ToolResult(content=e.message, is_error=True)
        budget.succeeded(scope.turn)
        scope.counted = budget.take()
        return ToolResult(content=result)

    async def function(arguments: dict[str, Any]) -> ToolResult:
        started = time.monotonic()
        scope = _call_scope.get() or ToolCallScope()
        if scope.function is not function:
            # Called by another tool, or by an adapter that records nothing: count by the scope's run
            # and turn, but keep this call's count out of the scope the adapter reads.
            scope = ToolCallScope(run=scope.run, turn=scope.turn)
        budget.start(scope.run)
        if not tool.tool_def.sequential:
            # Validation does not await, so without this a call the framework started alongside
            # this one would not have started yet when this one fails, and the budget would
            # count both failures of one turn.
            await asyncio.sleep(0)
            return await call(arguments, started, scope)
        ticket = in_order.take()
        await in_order.wait(ticket)
        try:
            return await call(arguments, started, scope)
        finally:
            in_order.done(ticket)

    setattr(function, _RESTORE_RETRY_COUNT, restore)
    return AirflowTool(
        name=name,
        description=tool.tool_def.description or name,
        parameters=tool.tool_def.parameters_json_schema,
        function=function,
    )
