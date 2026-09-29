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

.. warning:: Experimental; see :mod:`airflow.providers.common.ai.tools`.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

try:
    from strands.tools.tools import PythonAgentTool
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.tools import AirflowTool

if TYPE_CHECKING:
    from pydantic import JsonValue
    from strands.types.tools import ToolResult as StrandsToolResult, ToolResultContent, ToolSpec, ToolUse

    from airflow.providers.common.ai.tools import ToolProvider

__all__ = ["as_strands_tools"]


def as_strands_tools(*sources: ToolProvider | AirflowTool) -> list[PythonAgentTool]:
    """
    Convert Airflow tools into Strands tools.

    Pass toolsets that implement
    :class:`~airflow.providers.common.ai.tools.ToolProvider`, such as
    ``SQLToolset`` and ``HookToolset``, or individual
    :class:`~airflow.providers.common.ai.tools.AirflowTool` objects. The result
    goes straight into ``strands.Agent(tools=[...])`` next to any tools of your
    own; the agent, its model and its loop stay plain Strands.

    Each Strands tool keeps the source tool's name, description and argument
    schema. A failed call reaches the model as a Strands result with
    ``status="error"``, and every result passes through Airflow's secret masker
    first.

    .. code-block:: python

        from strands import Agent

        from airflow.providers.common.ai.tools.strands import as_strands_tools
        from airflow.providers.common.ai.toolsets import SQLToolset

        warehouse = SQLToolset("warehouse", allowed_tables=["orders"])
        agent = Agent(model=model, tools=as_strands_tools(warehouse))

    :param sources: Toolsets and tools to convert, in order.
    :return: One ``PythonAgentTool`` per Airflow tool.
    """
    tools: list[AirflowTool] = []
    for source in sources:
        if isinstance(source, AirflowTool):
            tools.append(source)
        else:
            tools.extend(source.airflow_tools())
    return [_to_strands_tool(tool) for tool in tools]


def _to_strands_tool(tool: AirflowTool) -> PythonAgentTool:
    spec: ToolSpec = {
        "name": tool.name,
        "description": tool.description,
        # Strands fills in missing property types and descriptions in place when it
        # registers a tool, and the source schema can be a toolset's module-level
        # constant that the pydantic-ai path also uses, so hand it a copy.
        "inputSchema": {"json": copy.deepcopy(tool.parameters)},
    }

    async def call_airflow_tool(tool_use: ToolUse, **invocation_state: Any) -> StrandsToolResult:
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
