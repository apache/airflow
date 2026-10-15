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
Give Airflow tools to an `OpenAI Agents SDK <https://openai.github.io/openai-agents-python/>`__ agent.

.. note:: Experimental; see :ref:`howto/stability`.
"""

from __future__ import annotations

import copy
import json
from typing import TYPE_CHECKING, Any

try:
    from agents import FunctionTool, Tool
    from agents.exceptions import AgentsException
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, collect_tools
from airflow.providers.common.ai.tools._from_toolset import tool_call_scope
from airflow.providers.common.ai.utils.tool_definition import serialize_for_llm
from airflow.providers.common.ai.utils.tool_metrics import calling_framework

if TYPE_CHECKING:
    from agents.tool_context import ToolContext

    from airflow.providers.common.ai.tools import ToolProvider

__all__ = ["AirflowTools"]


class AirflowTools(list[Tool]):
    """
    Airflow's tools as OpenAI Agents SDK function tools, in a list to give an ``Agent``.

    Pass toolsets that implement
    :class:`~airflow.providers.common.ai.tools.ToolProvider`, such as ``SQLToolset``
    and ``HookToolset``, or individual
    :class:`~airflow.providers.common.ai.tools.AirflowTool` objects. The agent, its
    model and its runner stay plain OpenAI Agents SDK.

    .. code-block:: python

        from agents import Agent, Runner

        from airflow.providers.common.ai.tools.openai_agents import AirflowTools
        from airflow.providers.common.ai.toolsets import SQLToolset

        warehouse = SQLToolset("warehouse", allowed_tables=["orders"])
        agent = Agent(name="analyst", model=model, tools=AirflowTools(warehouse))
        answer = Runner.run_sync(agent, question).final_output

    It is a list of ``FunctionTool`` objects typed as ``Agent.tools`` is, so add your own
    tools with ``[*AirflowTools(warehouse), my_tool]``.

    Each tool keeps the source tool's name, description and argument schema, and every
    result passes through Airflow's secret masker first. The schema is passed as it is,
    with ``strict_json_schema=False``: the SDK's strict mode would make every argument
    required. A failure the model can correct, such as a query naming a missing column,
    reaches it as ``{"error": ...}``, counted against the tool's retry limit afresh on
    every run of the agent. Arguments that are not a JSON object also come back as
    ``{"error": ...}``, bounded by the run's ``max_turns`` rather than that limit. A tool
    still failing once the limit is used up, or any other failure, raises
    :class:`~airflow.providers.common.ai.tools.ToolCallError` out of ``Runner.run``, so
    the task fails and Airflow retries it. Tools a toolset marks sequential, such as the
    sandbox's, run one at a time in the order the model called them.

    An agent holding these tools that another agent calls through ``Agent.as_tool()`` must
    be given ``failure_error_function=None``: by default ``as_tool`` hands any exception
    from the inner run to the outer model as text, so the task would carry on.

    :param sources: Toolsets and tools to add, in order.
    """

    def __init__(self, *sources: ToolProvider | AirflowTool) -> None:
        super().__init__(_to_function_tool(tool) for tool in collect_tools(sources))


class _FailedToolCall(ToolCallError, AgentsException):
    """
    A :class:`ToolCallError` the runner lets through as it is.

    The runner wraps any other exception a tool raises in a ``UserError``; one of its own
    exception types it raises unchanged, so the task fails with the tool's error.
    """


class _SameRun:
    """
    The run a tool call belongs to, compared by identity.

    The runner gives every run a ``Usage`` of its own and hands that same object to each of
    the run's tool calls. ``Usage`` compares by value, so two runs that have used the same
    tokens would compare equal; this compares the object itself.
    """

    __slots__ = ("_usage",)

    def __init__(self, usage: object) -> None:
        self._usage = usage

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _SameRun) and other._usage is self._usage

    def __hash__(self) -> int:
        return id(self._usage)


def _to_function_tool(tool: AirflowTool) -> FunctionTool:
    async def call_airflow_tool(ctx: ToolContext[Any], arguments: str) -> str:
        # The SDK passes the model's arguments as it sent them, without parsing them first.
        try:
            parsed = json.loads(arguments) if arguments else {}
        except json.JSONDecodeError as e:
            return serialize_for_llm({"error": f"The arguments are not valid JSON: {e}"})
        if not isinstance(parsed, dict):
            return serialize_for_llm({"error": "The arguments must be a JSON object."})
        # The SDK does not identify the model turn, so calls that run at the same time count once.
        try:
            with calling_framework("openai_agents"), tool_call_scope(run=_SameRun(ctx.usage)):
                result = await tool.call(parsed)
        except ToolCallError as e:
            # Already masked, and logged to the task log with its traceback by AirflowTool.call.
            raise _FailedToolCall(str(e)) from None
        if result.is_error:
            return serialize_for_llm({"error": result.content})
        return serialize_for_llm(result.content)

    return FunctionTool(
        name=tool.name,
        description=tool.description,
        # The source schema can be a toolset's module-level constant, so hand over a copy.
        params_json_schema=copy.deepcopy(tool.parameters),
        on_invoke_tool=call_airflow_tool,
        strict_json_schema=False,
    )
