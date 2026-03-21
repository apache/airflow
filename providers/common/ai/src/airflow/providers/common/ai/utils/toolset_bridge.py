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
"""Bridge pydantic-ai toolsets to ADK-compatible callable tools."""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import AbstractToolset

log = logging.getLogger(__name__)


def toolsets_to_adk_tools(toolsets: list[AbstractToolset[Any]]) -> list:
    """
    Convert pydantic-ai ``AbstractToolset`` instances to ADK-compatible tools.

    Extracts tool definitions from each toolset and creates async wrapper
    functions that ADK's ``FunctionTool`` can invoke.  Each wrapper preserves
    the tool name, docstring (with Google-style ``Args:`` section built from
    the JSON schema), and a dynamic ``__signature__`` so ADK can generate
    correct function declarations for the LLM.

    :param toolsets: List of pydantic-ai ``AbstractToolset`` instances
        (e.g. ``SQLToolset``, ``HookToolset``).
    :return: List of async callables suitable for ``google.adk.agents.Agent(tools=...)``.
    """
    adk_tools: list = []
    for toolset in toolsets:
        try:
            tools_dict = asyncio.run(toolset.get_tools(None))  # type: ignore[arg-type]
        except Exception:
            log.warning("Could not extract tools from toolset %r for ADK bridge — skipping.", toolset)
            continue

        for tool_name, ts_tool in tools_dict.items():
            wrapper = _create_adk_wrapper(toolset, tool_name, ts_tool)
            adk_tools.append(wrapper)

    return adk_tools


def _create_adk_wrapper(toolset: AbstractToolset[Any], tool_name: str, ts_tool: Any):
    """Create a single async callable that delegates to the toolset."""
    definition = ts_tool.definition
    schema = definition.parameters_json_schema or {}
    properties = schema.get("properties", {})
    required_params = set(schema.get("required", []))

    # Build a dynamic signature so ADK knows what parameters the tool accepts.
    params = []
    for param_name, _param_info in properties.items():
        default = inspect.Parameter.empty if param_name in required_params else None
        params.append(
            inspect.Parameter(
                param_name,
                inspect.Parameter.KEYWORD_ONLY,
                default=default,
            )
        )

    sig = inspect.Signature(params)

    # Build a Google-style docstring with Args section for ADK.
    docstring = _build_docstring(definition.description or tool_name, properties)

    async def wrapper(**kwargs):
        return await toolset.call_tool(tool_name, kwargs, None, ts_tool)  # type: ignore[arg-type]

    wrapper.__name__ = tool_name
    wrapper.__qualname__ = tool_name
    wrapper.__doc__ = docstring
    wrapper.__signature__ = sig  # type: ignore[attr-defined]

    return wrapper


def _build_docstring(description: str, properties: dict[str, Any]) -> str:
    """Build a Google-style docstring from a tool description and JSON schema properties."""
    if not properties:
        return description

    parts = [description, "", "Args:"]
    for param_name, param_info in properties.items():
        param_desc = param_info.get("description", "")
        parts.append(f"    {param_name}: {param_desc}")

    return "\n".join(parts)
