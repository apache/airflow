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
"""MCP server toolset that resolves configuration from an Airflow connection."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import Self

try:
    from pydantic_ai.mcp import MCPServerSSE, MCPServerStdio, MCPServerStreamableHTTP
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from pydantic_ai._run_context import RunContext


class MCPToolset(AbstractToolset[Any]):
    """
    Toolset that connects to an MCP server configured via an Airflow connection.

    Reads MCP server transport type, URL, command, and credentials from the
    connection and creates the appropriate PydanticAI MCP server instance.
    All ``AbstractToolset`` methods delegate to the underlying MCP server.

    This is the recommended way to use MCP servers in Airflow — it stores
    server configuration in Airflow connections (and secret backends) rather
    than hardcoding URLs and credentials in DAG code.

    If you prefer full PydanticAI control, you can pass MCP server instances
    directly to ``AgentOperator(toolsets=[...])``, since
    :class:`~pydantic_ai.mcp.MCPServerStreamableHTTP`,
    :class:`~pydantic_ai.mcp.MCPServerSSE`, and
    :class:`~pydantic_ai.mcp.MCPServerStdio` all implement ``AbstractToolset``.

    :param mcp_conn_id: Airflow connection ID for the MCP server.
    :param tool_prefix: Optional prefix prepended to tool names
        (e.g. ``"weather"`` → ``"weather_get_forecast"``).
    """

    def __init__(
        self,
        mcp_conn_id: str,
        *,
        tool_prefix: str | None = None,
    ) -> None:
        self._mcp_conn_id = mcp_conn_id
        self._tool_prefix = tool_prefix
        self._server: MCPServerStreamableHTTP | MCPServerSSE | MCPServerStdio | None = None

    @property
    def id(self) -> str:
        return f"mcp-{self._mcp_conn_id}"

    def _get_server(self) -> MCPServerStreamableHTTP | MCPServerSSE | MCPServerStdio:
        if self._server is None:
            conn = BaseHook.get_connection(self._mcp_conn_id)
            extra = conn.extra_dejson
            transport = extra.get("transport", "http")
            headers = {"Authorization": f"Bearer {conn.password}"} if conn.password else None

            if transport == "http":
                if not conn.host:
                    raise ValueError(
                        f"Connection {self._mcp_conn_id!r} requires a host URL for HTTP transport."
                    )
                self._server = MCPServerStreamableHTTP(
                    conn.host, headers=headers, tool_prefix=self._tool_prefix
                )
            elif transport == "sse":
                if not conn.host:
                    raise ValueError(
                        f"Connection {self._mcp_conn_id!r} requires a host URL for SSE transport."
                    )
                self._server = MCPServerSSE(conn.host, headers=headers, tool_prefix=self._tool_prefix)
            elif transport == "stdio":
                command = extra.get("command")
                if not command:
                    raise ValueError(
                        f"Connection {self._mcp_conn_id!r} requires 'command' in extra for stdio transport."
                    )
                args = extra.get("args", [])
                if isinstance(args, str):
                    args = [args]
                timeout = extra.get("timeout", 10)
                self._server = MCPServerStdio(
                    command, args=args, timeout=timeout, tool_prefix=self._tool_prefix
                )
            else:
                raise ValueError(
                    f"Unknown transport {transport!r} in connection {self._mcp_conn_id!r}. "
                    "Supported: 'http', 'sse', 'stdio'."
                )
        return self._server

    async def __aenter__(self) -> Self:
        await self._get_server().__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> bool | None:
        if self._server is not None:
            return await self._server.__aexit__(*args)
        return None

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        return await self._get_server().get_tools(ctx)

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        return await self._get_server().call_tool(name, tool_args, ctx, tool)
