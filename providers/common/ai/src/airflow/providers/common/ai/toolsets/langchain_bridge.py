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

from typing import TYPE_CHECKING, Any

# Imported at run time, not for type checking only: LangChain's agent runtime evaluates each
# tool function's type hints to find injected arguments.
from pydantic import JsonValue  # noqa: TC002

from airflow.providers.common.ai.tools._from_toolset import airflow_tools_from_toolset
from airflow.providers.common.ai.utils.coroutines import run_coroutine_sync

if TYPE_CHECKING:
    from langchain_core.tools import StructuredTool, ToolException
    from pydantic_ai.toolsets.abstract import AbstractToolset

    from airflow.providers.common.ai.tools import AirflowTool


def airflow_toolset_to_langchain_tools(
    toolset: AbstractToolset[Any],
    *,
    deps: Any = None,
) -> list[StructuredTool]:
    """
    Convert a pydantic-ai toolset into a list of LangChain ``StructuredTool`` objects.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Each returned tool carries the ``args_schema`` of the toolset's tool, so a
    LangChain agent or chain can call it the same way it calls any native LangChain
    tool. What it returns passes through Airflow's secret masker first.

    A failure the model can correct reaches it as an error result, a LangChain
    ``ToolMessage`` with ``status="error"``, so it can try again: an argument that fails
    the toolset's validation, or a pydantic-ai :exc:`~pydantic_ai.exceptions.ModelRetry`,
    which the bundled SQL toolsets raise to ask for a corrected query. These retries are
    bounded by
    the tool's ``max_retries``: once they are used up, and for any other exception the
    tool raises, the call raises
    :class:`~airflow.providers.common.ai.tools.ToolCallError`, so the run fails
    instead of looping. A ``ValidationError`` raised by the tool itself also
    propagates, since the call may already have had a side effect.

    The toolset's ``get_tools`` is invoked eagerly here to enumerate the tools.

    .. warning::
        The bridge does not keep a toolset open between calls, so an ``MCPToolset``
        reconnects to its server on every call, on the sync and async paths alike, and a
        stdio server loses any state it keeps between calls. A ``SandboxToolset`` has to be
        used inside its ``with`` block.

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
        from langchain_core.tools import StructuredTool, ToolException
    except ImportError as e:
        from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

        raise AirflowOptionalProviderFeatureException(e)

    return [
        _to_structured_tool(tool, StructuredTool, ToolException)
        for tool in airflow_tools_from_toolset(toolset, deps=deps)
    ]


def _to_structured_tool(
    tool: AirflowTool,
    structured_tool_cls: type[StructuredTool],
    tool_exception_cls: type[ToolException],
) -> StructuredTool:
    async def call(**kwargs: Any) -> JsonValue:
        result = await tool.call(kwargs)
        if result.is_error:
            # With handle_tool_error, LangChain hands this text to the model as an error result.
            raise tool_exception_cls(str(result.content))
        return result.content

    def call_sync(**kwargs: Any) -> JsonValue:
        return run_coroutine_sync(call(**kwargs))

    return structured_tool_cls.from_function(
        func=call_sync,
        coroutine=call,
        name=tool.name,
        description=tool.description,
        args_schema=tool.parameters,
        handle_tool_error=True,
    )
