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
"""
Give Airflow tools to an agent loop run by the Anthropic SDK's tool runner.

.. note:: Experimental; see :mod:`airflow.providers.common.ai.tools`.
"""

from __future__ import annotations

import copy
import logging
from collections.abc import Callable, Coroutine, Iterator
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, TypeVar

try:
    from anthropic.lib.tools import (
        BetaAsyncFunctionTool,
        BetaAsyncToolRunner,
        BetaFunctionTool,
        BetaToolRunner,
        ToolError,
        beta_async_tool,
        beta_tool,
    )
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, collect_tools
from airflow.providers.common.ai.tools._from_toolset import tool_call_scope
from airflow.providers.common.ai.utils.coroutines import run_coroutine_sync
from airflow.providers.common.ai.utils.tool_definition import serialize_for_llm
from airflow.providers.common.ai.utils.tool_metrics import calling_framework

if TYPE_CHECKING:
    from anthropic.types.beta import BetaMessage
    from anthropic.types.beta.parsed_beta_message import ParsedBetaMessage

    from airflow.providers.common.ai.tools import ToolProvider

log = logging.getLogger(__name__)

__all__ = ["AirflowTools", "AsyncAirflowTools"]

# The ``output_format`` a runner parses its final message into.
_OutputT = TypeVar("_OutputT")

# The failures of the run in progress. ``run`` sets it, the tools append to it.
_failures: ContextVar[list[ToolCallError] | None] = ContextVar("common_ai_anthropic_failures", default=None)


class AirflowTools:
    """
    Airflow's tools for the tool runner of a synchronous ``Anthropic`` client.

    Pass toolsets that implement
    :class:`~airflow.providers.common.ai.tools.ToolProvider`, such as ``SQLToolset``
    and ``HookToolset``, or individual
    :class:`~airflow.providers.common.ai.tools.AirflowTool` objects. Give :attr:`tools`
    to ``client.beta.messages.tool_runner`` and drive the runner with :meth:`run`:

    .. code-block:: python

        from anthropic import Anthropic

        from airflow.providers.common.ai.tools.anthropic import AirflowTools
        from airflow.providers.common.ai.toolsets import SQLToolset

        client = Anthropic()
        tools = AirflowTools(SQLToolset("warehouse", allowed_tables=["orders"]))
        runner = client.beta.messages.tool_runner(
            model="claude-opus-5-5",
            max_tokens=16000,
            tools=tools.tools,
            messages=[...],
        )
        message = tools.run(runner)

    Each tool keeps the source tool's name, description and argument schema, and every
    result passes through Airflow's secret masker first. A failure the model can
    correct, such as a query naming a missing column, reaches it as a ``tool_result``
    with ``is_error: true``, counted against the tool's retry limit once per model
    turn. A tool that is still failing once the limit is used up, or a failure the
    model cannot fix, such as a hook raising, makes :meth:`run` raise
    :class:`~airflow.providers.common.ai.tools.ToolCallError` before the next model
    request, so the task fails and Airflow retries it. The runner on its own hands
    every exception back to the model, so drive it with :meth:`run` rather than
    ``until_done()``. A runner created with ``stream=True`` is not supported.

    For an ``AsyncAnthropic`` client, use :class:`AsyncAirflowTools`.

    :param sources: Toolsets and tools to add, in order.
    """

    def __init__(self, *sources: ToolProvider | AirflowTool) -> None:
        self.tools: list[BetaFunctionTool[Callable[..., str]]] = [
            _sync_tool(tool) for tool in collect_tools(sources)
        ]

    def run(self, runner: BetaToolRunner[_OutputT]) -> ParsedBetaMessage[_OutputT]:
        """
        Drive ``runner`` to its final message.

        :raises ToolCallError: when a tool failed in a way the model cannot fix.
        """
        with _run() as turn:
            for message in runner:
                with turn(message.id):
                    if _runs_tools(message):
                        runner.generate_tool_call_response()
        return runner.until_done()


class AsyncAirflowTools:
    """
    Airflow's tools for the tool runner of an ``AsyncAnthropic`` client.

    The asynchronous counterpart of :class:`AirflowTools`, with the same behaviour:

    .. code-block:: python

        tools = AsyncAirflowTools(SQLToolset("warehouse", allowed_tables=["orders"]))
        runner = AsyncAnthropic().beta.messages.tool_runner(
            model="claude-opus-5-5",
            max_tokens=16000,
            tools=tools.tools,
            messages=[...],
        )
        message = await tools.run(runner)

    :param sources: Toolsets and tools to add, in order.
    """

    def __init__(self, *sources: ToolProvider | AirflowTool) -> None:
        self.tools: list[BetaAsyncFunctionTool[Callable[..., Coroutine[Any, Any, str]]]] = [
            _async_tool(tool) for tool in collect_tools(sources)
        ]

    async def run(self, runner: BetaAsyncToolRunner[_OutputT]) -> ParsedBetaMessage[_OutputT]:
        """
        Drive ``runner`` to its final message.

        :raises ToolCallError: when a tool failed in a way the model cannot fix.
        """
        with _run() as turn:
            async for message in runner:
                with turn(message.id):
                    if _runs_tools(message):
                        await runner.generate_tool_call_response()
        return await runner.until_done()


def _runs_tools(message: BetaMessage) -> bool:
    """
    Whether the runner will run this turn's tool calls, which it does only for ``tool_use``.

    A turn cut off at ``max_tokens`` can hold a tool call with incomplete arguments, and a
    ``pause_turn`` or ``compaction`` turn is sent back to the server unchanged; the runner
    runs no tool calls for either, so neither may this.
    """
    return message.stop_reason == "tool_use"


@contextmanager
def _run() -> Iterator[Callable[[str], AbstractContextManager[None]]]:
    """
    Scope one run of the runner, and yield the context that runs one model turn's tool calls.

    The runner caches the tool results it builds and sends them with the next model
    request, so building them in the turn's context lets the tools count retries per turn
    and lets a failure end the run before that request.
    """
    run = object()
    failures: list[ToolCallError] = []
    token = _failures.set(failures)

    def raise_first_failure() -> None:
        if failures:
            raise failures[0]

    @contextmanager
    def turn(message_id: str) -> Iterator[None]:
        # Checked on the way in too: should the runner ever run tools outside a turn, their
        # failure ends the run at the next turn rather than once the runner stops.
        raise_first_failure()
        with tool_call_scope(run=run, turn=message_id):
            yield
        raise_first_failure()

    try:
        yield turn
        raise_first_failure()
    finally:
        _failures.reset(token)


async def _call(tool: AirflowTool, arguments: dict[str, Any]) -> str:
    failures = _failures.get()
    if failures:
        # The run ends once this turn's calls are done, so start no more of them.
        raise ToolError("Not run: an earlier tool call in this turn failed.")
    try:
        with calling_framework("anthropic"):
            result = await tool.call(arguments)
    except ToolCallError as e:
        if failures is None:
            log.warning(
                "The tool runner was not driven by AirflowTools.run, so it hands this failure to "
                "the model instead of failing the task: %s",
                e,
            )
            raise
        failures.append(e)
        # ``run`` raises the original once the turn is done. The runner logs any other
        # exception with its traceback, so this keeps the task log to one account of it.
        raise ToolError(str(e)) from None
    # Masks what a plain JSON dump would render with str(), such as a dataclass holding a secret.
    text = serialize_for_llm(result.content)
    if result.is_error:
        raise ToolError(text)
    return text


def _sync_tool(tool: AirflowTool) -> BetaFunctionTool[Callable[..., str]]:
    def call_airflow_tool(**arguments: Any) -> str:
        return run_coroutine_sync(_call(tool, arguments))

    # The source schema can be a toolset's module-level constant, so hand over a copy.
    return beta_tool(
        call_airflow_tool,
        name=tool.name,
        description=tool.description,
        input_schema=copy.deepcopy(tool.parameters),
    )


def _async_tool(tool: AirflowTool) -> BetaAsyncFunctionTool[Callable[..., Coroutine[Any, Any, str]]]:
    async def call_airflow_tool(**arguments: Any) -> str:
        return await _call(tool, arguments)

    return beta_async_tool(
        call_airflow_tool,
        name=tool.name,
        description=tool.description,
        input_schema=copy.deepcopy(tool.parameters),
    )
