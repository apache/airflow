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
Bridge pydantic-ai toolsets into LangChain tools.

This is the reverse of pydantic-ai's upstream ``pydantic_ai.ext.langchain``
bridge. Upstream turns LangChain tools *into* a pydantic-ai toolset
(:class:`~pydantic_ai.ext.langchain.LangChainToolset`) so they can be used with
common.ai's ``AgentOperator``. This module goes the other way: it turns a
pydantic-ai :class:`~pydantic_ai.toolsets.abstract.AbstractToolset` -- such as
common.ai's :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`,
:class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`, or
:class:`~airflow.providers.common.ai.toolsets.mcp.MCPToolset` -- into a list of
LangChain ``StructuredTool`` objects, so Airflow's curated tools can be handed
to a LangChain agent or chain.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from typing import TYPE_CHECKING, Any

from pydantic_ai import RunContext
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from langchain_core.tools import StructuredTool
    from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool


def _run_coro_sync(coro: Coroutine[Any, Any, Any]) -> Any:
    """
    Run an awaitable to completion from synchronous code.

    LangChain's ``StructuredTool.func`` is synchronous and is what an Airflow
    ``@task`` calls, but a pydantic-ai toolset's ``get_tools`` / ``call_tool``
    are coroutines. When no event loop is running we drive the coroutine with
    :func:`asyncio.run`; if one is already running in this thread (an async
    caller) we run it in a worker thread to avoid nesting loops.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def airflow_toolset_to_langchain_tools(
    toolset: AbstractToolset[Any],
    *,
    deps: Any = None,
) -> list[StructuredTool]:
    """
    Convert a pydantic-ai toolset into a list of LangChain ``StructuredTool`` objects.

    Each returned tool is backed by ``toolset.call_tool`` and carries the
    ``args_schema`` derived from the tool's JSON schema, so a LangChain agent or
    chain can call it the same way it calls any native LangChain tool.

    If a tool raises pydantic-ai's :exc:`~pydantic_ai.exceptions.ModelRetry`
    (the bundled SQL toolsets do this to ask the model to correct its input,
    e.g. an unknown column), the bridge returns the retry message as the tool's
    output so the model sees it and tries again. ``ModelRetry`` is a
    feed-the-model-and-retry signal, not a failure; returning it mirrors that and
    works regardless of how the agent handles tool errors. Raising instead would
    abort the run under ``create_agent``'s default tool-error handling.

    The retry message is bounded by the tool's ``max_retries``: a tool that keeps
    raising ``ModelRetry`` (for example an unrecoverable connection error) stops
    being fed back and propagates once the budget is exhausted, so the run fails
    instead of looping forever. The count resets after a successful call.

    The toolset's ``get_tools`` is invoked eagerly here to enumerate the tools.

    .. warning::
        The bridge does not hold a toolset session open across calls. ``get_tools``
        and every ``call_tool`` each run under their own event loop, and pydantic-ai
        opens and tears the connection down around each one. For ``MCPToolset`` this
        means the server is reconnected on every tool call. That is fine for
        stateless tools (and for HTTP/SSE servers, modulo per-call latency), but an
        ``MCPServerStdio`` server, or any server that keeps state between calls,
        will lose that state because each call starts a fresh process/session.

    .. note::
        A pydantic-ai toolset is normally driven inside an agent run, where a
        live :class:`~pydantic_ai.RunContext` carries the model, usage, and
        message history. Outside an agent run there is no such context, so this
        bridge builds a minimal one with an inert placeholder model. The curated
        common.ai toolsets (``SQLToolset``, ``HookToolset``, ``MCPToolset``)
        ignore the context, so this works for them. A custom toolset that reads
        live run state (``ctx.model``, ``ctx.messages``, ``ctx.usage``) will not
        behave correctly when bridged standalone.

    :param toolset: The pydantic-ai toolset to convert.
    :param deps: Optional dependency object exposed to the toolset as
        ``ctx.deps``. Defaults to ``None``.
    :return: A list of LangChain ``StructuredTool`` objects, one per tool in the
        toolset.
    """
    try:
        from langchain_core.tools import StructuredTool
    except ImportError as e:
        from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

        raise AirflowOptionalProviderFeatureException(e)

    # An inert placeholder context. The curated common.ai toolsets ignore it;
    # TestModel satisfies RunContext's required `model` field without reaching a
    # real LLM (the bridge never runs the model, only the tools).
    ctx: RunContext[Any] = RunContext(deps=deps, model=TestModel(), usage=RunUsage())

    toolset_tools = _run_coro_sync(toolset.get_tools(ctx))

    return [
        _build_structured_tool(toolset, name, toolset_tool, ctx, StructuredTool)
        for name, toolset_tool in toolset_tools.items()
    ]


def _build_structured_tool(
    toolset: AbstractToolset[Any],
    name: str,
    toolset_tool: ToolsetTool[Any],
    ctx: RunContext[Any],
    structured_tool_cls: type[StructuredTool],
) -> StructuredTool:
    """Build a single LangChain ``StructuredTool`` from one pydantic-ai tool."""
    tool_def = toolset_tool.tool_def

    def _validate(kwargs: dict[str, Any]) -> dict[str, Any]:
        # Mirrors what pydantic-ai's ToolManager does before dispatch, which the
        # bridge bypasses. A passthrough validator (the bundled toolsets) returns
        # the args unchanged; a typed one coerces them (e.g. "5" -> 5).
        return toolset_tool.args_validator.validate_python(kwargs)

    # ModelRetry is a "feed this back to the model and retry" signal, so the bridge
    # returns its message as the tool output instead of raising (see docstring).
    # Bound it the way native pydantic-ai does, via the tool's max_retries: a tool
    # that keeps raising ModelRetry (e.g. an unrecoverable connection error) must
    # eventually propagate so the run fails rather than looping forever. The count
    # resets on the first successful call.
    max_retries = toolset_tool.max_retries if toolset_tool.max_retries is not None else 1
    retries = {"count": 0}

    def _handle_retry(error: ModelRetry) -> str:
        retries["count"] += 1
        if retries["count"] > max_retries:
            # Reset before propagating so a reused tool starts the next run with a
            # fresh budget instead of staying permanently exhausted.
            retries["count"] = 0
            raise error
        return str(error)

    def _sync_call(**kwargs: Any) -> Any:
        try:
            result = _run_coro_sync(toolset.call_tool(name, _validate(kwargs), ctx, toolset_tool))
        except ModelRetry as e:
            return _handle_retry(e)
        retries["count"] = 0
        return result

    async def _async_call(**kwargs: Any) -> Any:
        try:
            result = await toolset.call_tool(name, _validate(kwargs), ctx, toolset_tool)
        except ModelRetry as e:
            return _handle_retry(e)
        retries["count"] = 0
        return result

    return structured_tool_cls.from_function(
        func=_sync_call,
        coroutine=_async_call,
        name=name,
        description=tool_def.description or name,
        args_schema=tool_def.parameters_json_schema,
    )
