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

from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.sdk.execution_time.secrets_masker import mask_secret
else:
    try:
        from airflow.sdk.log import mask_secret
    except ImportError:
        try:
            from airflow.sdk.execution_time.secrets_masker import mask_secret
        except ImportError:
            from airflow.utils.log.secrets_masker import mask_secret


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

    For HTTP/SSE transports the ``Authorization`` header is, by default, a static
    ``Bearer`` token taken from the connection ``password``. Endpoints that require
    a freshly minted or short-lived token (e.g. a Snowflake managed MCP server
    authenticated with a key-pair JWT, OAuth/refresh tokens, Workload Identity
    Federation, or GitHub App installation tokens) can pass a ``token_provider``
    callable instead. It is invoked each time the server connection is established
    and its return value is used as the bearer token, so a fresh token is minted
    without storing a long-lived secret in the connection.

    :param mcp_conn_id: Airflow connection ID for the MCP server.
    :param tool_prefix: Optional prefix prepended to tool names
        (e.g. ``"weather"`` → ``"weather_get_forecast"``).
    :param token_provider: Optional zero-argument callable returning a bearer
        token string. When set, it overrides the connection ``password`` for the
        ``Authorization`` header on HTTP/SSE transports and is called each time the
        server connection is established. Ignored for the ``stdio`` transport.
    """

    conn_name_attr = "mcp_conn_id"
    default_conn_name = "mcp_default"
    conn_type = "mcp"
    hook_name = "MCP Server"

    def __init__(
        self,
        mcp_conn_id: str = default_conn_name,
        tool_prefix: str | None = None,
        *,
        token_provider: Callable[[], str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mcp_conn_id = mcp_conn_id
        self.tool_prefix = tool_prefix
        self.token_provider = token_provider
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

    def _auth_headers(self, conn: Any) -> dict[str, str] | None:
        """
        Build the ``Authorization`` header for HTTP/SSE transports.

        Prefers ``token_provider`` (minted per connection) over the static
        connection ``password``. Returns ``None`` when neither yields a token.
        """
        if self.token_provider is not None:
            token = self.token_provider()
            if not isinstance(token, str) or not token:
                raise ValueError(
                    f"token_provider for connection {self.mcp_conn_id!r} must return a non-empty "
                    f"string token, got {type(token).__name__}."
                )
            # The static connection password is masked when the connection is
            # fetched; mask the minted token too so it never leaks into task logs.
            mask_secret(token)
            return {"Authorization": f"Bearer {token}"}
        if conn.password:
            return {"Authorization": f"Bearer {conn.password}"}
        return None

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

        if transport == "http":
            if not conn.host:
                raise ValueError(f"Connection {self.mcp_conn_id!r} requires a host URL for HTTP transport.")
            self._server = MCPServerStreamableHTTP(
                conn.host, headers=self._auth_headers(conn), tool_prefix=self.tool_prefix
            )
        elif transport == "sse":
            if not conn.host:
                raise ValueError(f"Connection {self.mcp_conn_id!r} requires a host URL for SSE transport.")
            self._server = MCPServerSSE(
                conn.host, headers=self._auth_headers(conn), tool_prefix=self.tool_prefix
            )
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
