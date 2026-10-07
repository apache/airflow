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
Give Airflow tools to an agent loop run by the Claude Agent SDK.

.. note:: Experimental; see :mod:`airflow.providers.common.ai.tools`.
"""

from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING, Any

try:
    from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, create_sdk_mcp_server, tool
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, collect_tools
from airflow.providers.common.ai.tools._from_toolset import tool_call_scope
from airflow.providers.common.ai.utils.tool_definition import serialize_for_llm
from airflow.providers.common.ai.utils.tool_metrics import calling_framework

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Awaitable, Callable

    from claude_agent_sdk import McpSdkServerConfig, Message

    from airflow.providers.common.ai.tools import ToolProvider

log = logging.getLogger(__name__)

__all__ = ["AirflowTools"]


class _Run:
    """One run of :meth:`AirflowTools.run`: identifies its calls and collects their failures."""

    def __init__(self) -> None:
        self.failures: list[ToolCallError] = []


class AirflowTools:
    """
    Airflow's tools for an agent loop run by the Claude Agent SDK.

    Pass toolsets that implement
    :class:`~airflow.providers.common.ai.tools.ToolProvider`, such as ``SQLToolset``
    and ``HookToolset``, or individual
    :class:`~airflow.providers.common.ai.tools.AirflowTool` objects. They are served
    through an in-process MCP server; add it and :attr:`allowed_tools` to
    ``ClaudeAgentOptions``, or build them with :meth:`options`, and drive the SDK's
    ``query()`` with :meth:`run`:

    .. code-block:: python

        from claude_agent_sdk import query

        from airflow.providers.common.ai.tools.claude_agent_sdk import AirflowTools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        tools = AirflowTools(SQLToolset(db_conn_id="warehouse", allowed_tables=["orders"]))
        options = tools.options(model="claude-sonnet-5", max_turns=10)
        result = await tools.run(query(prompt="...", options=options))

    Each tool keeps the source tool's name, description and argument schema, and every
    result passes through Airflow's secret masker first. A failure the model can
    correct, such as a query naming a missing column, reaches it as an ``is_error``
    tool result. A failure the model cannot fix, such as a hook raising, makes
    :meth:`run` raise :class:`~airflow.providers.common.ai.tools.ToolCallError`, so the
    task fails and Airflow retries it. The SDK's own tool dispatch catches every
    exception a handler raises and turns it into an ``is_error`` result the model
    reads, so drive the query with :meth:`run` rather than iterating it yourself.

    An instance runs one query at a time: build a new ``AirflowTools`` per task, or
    call :meth:`run` again once the previous run has finished.

    :param sources: Toolsets and tools to serve, in order.
    :param server_name: The MCP server name the tools are served under. Also the
        prefix of each tool's name in :attr:`allowed_tools`
        (``mcp__<server_name>__<tool name>``).
    """

    def __init__(self, *sources: ToolProvider | AirflowTool, server_name: str = "airflow") -> None:
        self.server_name = server_name
        airflow_tools = collect_tools(sources)
        self.allowed_tools: list[str] = [f"mcp__{server_name}__{t.name}" for t in airflow_tools]
        self._run: _Run | None = None
        sdk_tools = [
            tool(t.name, t.description, copy.deepcopy(t.parameters))(self._build_handler(t))
            for t in airflow_tools
        ]
        self.server: McpSdkServerConfig = create_sdk_mcp_server(name=server_name, tools=sdk_tools)

    def options(self, **kwargs: Any) -> ClaudeAgentOptions:
        """
        Build ``ClaudeAgentOptions`` wired to this instance's tools, with safe defaults.

        ``ClaudeAgentOptions()`` on its own loads every settings source on the worker
        (``~/.claude``, project settings, a project ``.mcp.json``) and leaves the CLI's
        built-in tools on, including a worker-host ``Bash``. This method instead
        defaults ``tools`` to ``[]`` (no built-in tools), ``setting_sources`` to ``[]``
        (no settings files) and ``strict_mcp_config`` to ``True`` (only the MCP servers
        passed here), and adds :attr:`server` and :attr:`allowed_tools` to whatever
        ``mcp_servers`` and ``allowed_tools`` the caller passes. A keyword this method
        does not default, such as ``model`` or ``env``, is passed through unchanged.
        Everything it builds can also be assembled by hand; see :attr:`server` and
        :attr:`allowed_tools`.

        :raises ValueError: when ``mcp_servers`` already has a server named
            :attr:`server_name`.
        """
        mcp_servers = dict(kwargs.pop("mcp_servers", {}))
        if self.server_name in mcp_servers:
            raise ValueError(
                f"mcp_servers already has a server named {self.server_name!r}. Pass a different "
                "server_name to AirflowTools, or drop it from mcp_servers."
            )
        mcp_servers[self.server_name] = self.server
        allowed_tools = [*kwargs.pop("allowed_tools", []), *self.allowed_tools]
        kwargs.setdefault("tools", [])
        kwargs.setdefault("setting_sources", [])
        kwargs.setdefault("strict_mcp_config", True)
        return ClaudeAgentOptions(mcp_servers=mcp_servers, allowed_tools=allowed_tools, **kwargs)

    async def run(self, messages: AsyncIterable[Message]) -> ResultMessage:
        """
        Drive ``messages``, the stream ``claude_agent_sdk.query()`` (or a client) returns.

        :raises RuntimeError: when this instance is already running a query, or when
            ``messages`` ends without a :class:`~claude_agent_sdk.ResultMessage`.
        :raises ToolCallError: when a tool failed in a way the model cannot fix.
        """
        if self._run is not None:
            raise RuntimeError(
                f"{self!r} is already running a query. Build a separate AirflowTools instance "
                "for a concurrent run, or await the first run before starting another."
            )
        state = _Run()
        self._run = state
        try:
            result: ResultMessage | None = None
            async for message in messages:
                if isinstance(message, ResultMessage):
                    result = message
                if state.failures:
                    raise state.failures[0]
            if result is None:
                if state.failures:
                    raise state.failures[0]
                raise RuntimeError("The Claude Agent SDK stream ended without a ResultMessage.")
            return result
        finally:
            self._run = None

    def _build_handler(
        self, airflow_tool: AirflowTool
    ) -> Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]:
        async def handle_tool_call(args: dict[str, Any]) -> dict[str, Any]:
            state = self._run
            if state is not None and state.failures:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": "Not run: an earlier Airflow tool call in this run failed.",
                        }
                    ],
                    "is_error": True,
                }
            with calling_framework("claude_agent_sdk"), tool_call_scope(run=state):
                try:
                    result = await airflow_tool.call(args)
                except ToolCallError as e:
                    if state is None:
                        log.warning(
                            "Tool %s failed, but the Claude Agent SDK query was not driven by "
                            "AirflowTools.run, so the model reads this failure instead of the "
                            "task failing: %s",
                            airflow_tool.name,
                            e,
                        )
                    else:
                        state.failures.append(e)
                    return {"content": [{"type": "text", "text": str(e)}], "is_error": True}
            payload: dict[str, Any] = {
                "content": [{"type": "text", "text": serialize_for_llm(result.content)}]
            }
            if result.is_error:
                payload["is_error"] = True
            return payload

        return handle_tool_call
