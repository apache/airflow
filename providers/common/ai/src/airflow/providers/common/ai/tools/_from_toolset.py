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
import threading
import weakref
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError
from pydantic_ai import RunContext
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.tools import AirflowTool, ToolResult
from airflow.providers.common.ai.utils.coroutines import run_coroutine_sync

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool


# The bundled toolsets share one hook doing blocking I/O, which is why their tool definitions
# are marked sequential, and SQLToolset reads the hook's ``last_description`` after each query.
# Frameworks may run tool calls concurrently, and several agents may share one toolset, so
# calls into the same toolset instance are serialised on one lock per instance.
_toolset_locks: weakref.WeakKeyDictionary[AbstractToolset[Any], threading.Lock] = weakref.WeakKeyDictionary()
_toolset_locks_guard = threading.Lock()


def _lock_for(toolset: AbstractToolset[Any]) -> threading.Lock:
    with _toolset_locks_guard:
        return _toolset_locks.setdefault(toolset, threading.Lock())


def airflow_tools_from_toolset(toolset: AbstractToolset[Any]) -> list[AirflowTool]:
    """
    Return one :class:`AirflowTool` per tool in ``toolset``.

    Each tool validates its arguments with the toolset's own validator and
    dispatches to the toolset's ``call_tool``, so the toolset's behaviour
    (connection resolution, SQL validation, ``allowed_tables``, bounded results)
    is unchanged. ``ModelRetry`` and argument validation failures become error
    results the model can read and correct, as they would inside a pydantic-ai run.

    Only for toolsets whose ``call_tool`` ignores the run context, which is true of
    ``SQLToolset`` and ``HookToolset``. Outside a pydantic-ai run there is no live
    ``RunContext``, so an inert one with a placeholder model is passed.
    """
    ctx: RunContext[Any] = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    toolset_tools = run_coroutine_sync(toolset.get_tools(ctx))
    lock = _lock_for(toolset)
    return [_as_airflow_tool(toolset, name, tool, ctx, lock) for name, tool in toolset_tools.items()]


def _as_airflow_tool(
    toolset: AbstractToolset[Any],
    name: str,
    tool: ToolsetTool[Any],
    ctx: RunContext[Any],
    lock: threading.Lock,
) -> AirflowTool:
    def invoke(arguments: dict[str, Any]) -> ToolResult:
        try:
            validated = tool.args_validator.validate_python(arguments)
        except ValidationError as e:
            return ToolResult(content=str(e), is_error=True)
        try:
            with lock:
                result = run_coroutine_sync(toolset.call_tool(name, validated, ctx, tool))
        except ModelRetry as e:
            return ToolResult(content=str(e), is_error=True)
        return ToolResult(content=result)

    async def function(arguments: dict[str, Any]) -> ToolResult:
        # The hook call blocks, so keep it off the framework's event loop.
        return await asyncio.to_thread(invoke, arguments)

    return AirflowTool(
        name=name,
        description=tool.tool_def.description or name,
        parameters=tool.tool_def.parameters_json_schema,
        function=function,
    )
