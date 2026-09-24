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
Give Airflow tools to a `Strands Agents <https://strandsagents.com/>`__ agent.

.. note:: Experimental; see :mod:`airflow.providers.common.ai.tools`.
"""

from __future__ import annotations

import copy
import itertools
from typing import TYPE_CHECKING, Any

try:
    # Needed at runtime: @hook reads the event type from the method's annotation.
    from strands.hooks import AfterToolCallEvent, BeforeInvocationEvent  # noqa: TC002
    from strands.plugins import Plugin, hook
    from strands.tools.tools import PythonAgentTool
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, collect_tools
from airflow.providers.common.ai.tools._from_toolset import tool_call_scope

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic import JsonValue
    from strands.types.tools import (
        AgentTool,
        ToolResult as StrandsToolResult,
        ToolResultContent,
        ToolSpec,
        ToolUse,
    )

    from airflow.providers.common.ai.tools import ToolProvider

__all__ = ["AirflowTools"]

# Strands refuses two plugins with the same name on one agent, so each instance gets its own.
_instance_numbers = itertools.count(1)


class AirflowTools(Plugin):
    """
    A Strands plugin that gives an agent Airflow's tools.

    Pass toolsets that implement
    :class:`~airflow.providers.common.ai.tools.ToolProvider`, such as ``SQLToolset``
    and ``HookToolset``, or individual
    :class:`~airflow.providers.common.ai.tools.AirflowTool` objects. The agent, its
    model and its loop stay plain Strands.

    .. code-block:: python

        from strands import Agent

        from airflow.providers.common.ai.tools.strands import AirflowTools
        from airflow.providers.common.ai.toolsets import SQLToolset

        warehouse = SQLToolset("warehouse", allowed_tables=["orders"])
        agent = Agent(model=model, plugins=[AirflowTools(warehouse)])

    Pass it in ``plugins=``, not ``tools=``: Strands ignores a plugin given as a tool, and
    handing over only its ``tools`` leaves out the hook that ends the run on a failure.

    Each tool keeps the source tool's name, description and argument schema, and every
    result passes through Airflow's secret masker first. A failure the model can
    correct, such as a query naming a missing column, reaches it as a Strands result
    with ``status="error"``, counted against the tool's retry limit once per model turn
    and afresh on every run of the agent. A tool that is still failing once the limit is
    used up, or a failure the model cannot fix, such as a hook raising, fails the agent
    run, so the task fails and Airflow retries it. Strands on its own would hand that
    failure to the model and carry on. Tools a toolset marks sequential, such as the
    sandbox's, run one at a time in the order the model called them.

    :param sources: Toolsets and tools to add, in order.
    """

    def __init__(self, *sources: ToolProvider | AirflowTool) -> None:
        # Before Plugin.__init__, which reads every attribute, ``name`` and ``tools`` included.
        self._name = f"airflow-tools-{next(_instance_numbers)}"
        self._run = object()
        self._airflow_tools: list[AgentTool] = [
            _to_strands_tool(tool, lambda: self._run) for tool in collect_tools(sources)
        ]
        super().__init__()

    @property
    def name(self) -> str:
        return self._name

    @property
    def tools(self) -> list[AgentTool]:  # type: ignore[override]
        # Plugin types this as its decorated tools; the registry takes any AgentTool.
        return self._airflow_tools

    @hook
    def _start_a_run(self, event: BeforeInvocationEvent) -> None:
        # Each call on the agent is a run of its own, with a fresh retry budget per tool.
        self._run = object()

    @hook
    def _fail_the_run_on_tool_call_error(self, event: AfterToolCallEvent) -> None:
        if isinstance(event.exception, ToolCallError):
            raise event.exception


def _to_strands_tool(tool: AirflowTool, current_run: Callable[[], object]) -> PythonAgentTool:
    spec: ToolSpec = {
        "name": tool.name,
        "description": tool.description,
        # Strands fills in missing property types and descriptions in place when it
        # registers a tool, and the source schema can be a toolset's module-level
        # constant that the pydantic-ai path also uses, so hand it a copy.
        "inputSchema": {"json": copy.deepcopy(tool.parameters)},
    }

    async def call_airflow_tool(tool_use: ToolUse, **invocation_state: Any) -> StrandsToolResult:
        # Strands gives every model turn of the event loop its own cycle ID.
        with tool_call_scope(run=current_run(), turn=invocation_state.get("event_loop_cycle_id")):
            result = await tool.call(tool_use["input"])
        return {
            "toolUseId": tool_use["toolUseId"],
            "status": "error" if result.is_error else "success",
            "content": [_content_block(result.content)],
        }

    return PythonAgentTool(tool.name, spec, call_airflow_tool)


def _content_block(content: JsonValue) -> ToolResultContent:
    if isinstance(content, str):
        return {"text": content}
    return {"json": content}
