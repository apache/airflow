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
from __future__ import annotations

from typing import Any

from airflow.providers.common.compat.sdk import BaseHook


class MCPHook(BaseHook):
    """
    Hook for connecting to MCP (Model Context Protocol) servers.

    Manages connection configuration for MCP servers. Supports three
    transport types: HTTP (Streamable HTTP), SSE, and stdio.

    Connection fields:
        - **host**: Server URL for HTTP/SSE transports (e.g. ``http://localhost:3001/mcp``)
        - **password**: Auth token (optional)
        - **Extra.transport**: Transport type — ``http`` (default), ``sse``, or ``stdio``
        - **Extra.command**: Command to run for stdio transport (e.g. ``uvx``)
        - **Extra.args**: Command arguments for stdio transport (e.g. ``["mcp-run-python"]``)
        - **Extra.timeout**: Connection timeout in seconds for stdio (default: 10)

    :param mcp_conn_id: Airflow connection ID for the MCP server.
    :param tool_prefix: Optional prefix prepended to tool names
        (e.g. ``"weather"`` → ``"weather_get_forecast"``).
    """

    conn_name_attr = "mcp_conn_id"
    default_conn_name = "mcp_default"
    conn_type = "mcp"
    hook_name = "MCP Server"

    def __init__(
        self,
        mcp_conn_id: str = default_conn_name,
        tool_prefix: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mcp_conn_id = mcp_conn_id
        self.tool_prefix = tool_prefix
        self._server: Any = None

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "Auth Token"},
            "placeholders": {
                "host": "http://localhost:3001/mcp (for HTTP/SSE transport)",
            },
        }

    def get_conn(self) -> Any:
        """
        Return a configured PydanticAI MCP server instance.

        Creates the appropriate MCP server based on the transport type
        in the connection's extra field:

        - ``http`` (default): :class:`~pydantic_ai.mcp.MCPServerStreamableHTTP`
        - ``sse``: :class:`~pydantic_ai.mcp.MCPServerSSE`
        - ``stdio``: :class:`~pydantic_ai.mcp.MCPServerStdio`

        The result is cached for the lifetime of this hook instance.
        """
        if self._server is not None:
            return self._server

        try:
            from pydantic_ai.mcp import MCPServerSSE, MCPServerStdio, MCPServerStreamableHTTP
        except ImportError:
            raise ImportError(
                'MCP support requires the `mcp` package. Install it with: pip install "pydantic-ai-slim[mcp]"'
            )

        conn = self.get_connection(self.mcp_conn_id)
        extra = conn.extra_dejson
        transport = extra.get("transport", "http")
        headers = {"Authorization": f"Bearer {conn.password}"} if conn.password else None

        if transport == "http":
            if not conn.host:
                raise ValueError(f"Connection {self.mcp_conn_id!r} requires a host URL for HTTP transport.")
            self._server = MCPServerStreamableHTTP(conn.host, headers=headers, tool_prefix=self.tool_prefix)
        elif transport == "sse":
            if not conn.host:
                raise ValueError(f"Connection {self.mcp_conn_id!r} requires a host URL for SSE transport.")
            self._server = MCPServerSSE(conn.host, headers=headers, tool_prefix=self.tool_prefix)
        elif transport == "stdio":
            command = extra.get("command")
            if not command:
                raise ValueError(
                    f"Connection {self.mcp_conn_id!r} requires 'command' in extra for stdio transport."
                )
            args = extra.get("args", [])
            if isinstance(args, str):
                args = [args]
            timeout = extra.get("timeout", 10)
            self._server = MCPServerStdio(command, args=args, timeout=timeout, tool_prefix=self.tool_prefix)
        else:
            raise ValueError(
                f"Unknown transport {transport!r} in connection {self.mcp_conn_id!r}. "
                "Supported: 'http', 'sse', 'stdio'."
            )

        return self._server

    def test_connection(self) -> tuple[bool, str]:
        """
        Test connection by verifying configuration is valid.

        Validates that the connection has the required fields for the
        configured transport type. Does NOT connect to the MCP server —
        that requires an async context manager.
        """
        try:
            self.get_conn()
            return True, "MCP server configuration is valid."
        except Exception as e:
            return False, str(e)
