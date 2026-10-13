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
Give Airflow tools to a `Google ADK <https://google.github.io/adk-docs/>`__ agent.

.. note:: Experimental; see :mod:`airflow.providers.common.ai.tools`.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

try:
    from google.adk.tools.base_tool import BaseTool
    from google.adk.tools.base_toolset import BaseToolset
    from google.genai import types
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.tools import AirflowTool, collect_tools
from airflow.providers.common.ai.tools._from_toolset import tool_call_scope
from airflow.providers.common.ai.utils.tool_metrics import calling_framework

if TYPE_CHECKING:
    from google.adk.agents.readonly_context import ReadonlyContext
    from google.adk.tools.tool_context import ToolContext

    from airflow.providers.common.ai.tools import ToolProvider

__all__ = ["AirflowTools"]


class AirflowTools(BaseToolset):
    """
    An ADK toolset that gives an agent Airflow's tools.

    Pass toolsets that implement
    :class:`~airflow.providers.common.ai.tools.ToolProvider`, such as ``SQLToolset``
    and ``HookToolset``, or individual
    :class:`~airflow.providers.common.ai.tools.AirflowTool` objects. The agent, its
    model and its runner stay plain ADK.

    .. code-block:: python

        from google.adk.agents import LlmAgent

        from airflow.providers.common.ai.tools.adk import AirflowTools
        from airflow.providers.common.ai.toolsets import SQLToolset

        warehouse = SQLToolset("warehouse", allowed_tables=["orders"])
        agent = LlmAgent(name="analyst", model=model, tools=[AirflowTools(warehouse)])

    Each tool keeps the source tool's name, description and argument schema, and every
    result passes through Airflow's secret masker first. A result reaches the model as
    ``{"result": ...}``; a failure the model can correct, such as a query naming a
    missing column, as ``{"error": ...}``, ADK's own convention for a failed tool. Such
    failures count against the tool's retry limit afresh on every run of the agent. A tool
    still failing once the limit is used up, or any other failure, raises
    :class:`~airflow.providers.common.ai.tools.ToolCallError` out of the run, so the task
    fails and Airflow retries it. Tools a toolset marks sequential, such as the sandbox's,
    run one at a time in the order the model called them.

    :param sources: Toolsets and tools to add, in order.
    """

    def __init__(self, *sources: ToolProvider | AirflowTool) -> None:
        super().__init__()
        self._adk_tools: list[BaseTool] = [_AirflowAdkTool(tool) for tool in collect_tools(sources)]

    async def get_tools(self, readonly_context: ReadonlyContext | None = None) -> list[BaseTool]:
        return self._adk_tools


class _AirflowAdkTool(BaseTool):
    def __init__(self, tool: AirflowTool) -> None:
        super().__init__(name=tool.name, description=tool.description)
        self._tool = tool

    def _get_declaration(self) -> types.FunctionDeclaration:
        return types.FunctionDeclaration(
            name=self._tool.name,
            description=self._tool.description,
            # A copy, as for Strands: the schema can be a toolset's module-level constant.
            parameters_json_schema=copy.deepcopy(self._tool.parameters),
        )

    async def run_async(self, *, args: dict[str, Any], tool_context: ToolContext) -> dict[str, Any]:
        # ADK does not identify the model turn, so calls that run at the same time count once.
        with calling_framework("adk"), tool_call_scope(run=tool_context.invocation_id):
            result = await self._tool.call(args)
        return {"error": result.content} if result.is_error else {"result": result.content}

    def _detect_error_in_response(self, response: Any) -> str | None:
        # ADK calls this to mark a failed call in its own telemetry, as its built-in tools do.
        return "TOOL_ERROR" if isinstance(response, dict) and "error" in response else None
