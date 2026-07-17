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
        - **Extra.env**: Environment variables for the stdio subprocess (e.g.
          ``{"API_KEY": "..."}``). Ignored for HTTP/SSE.
        - **Extra.timeout**: Connection init timeout in seconds for stdio (default: 10)

    For HTTP/SSE transports the ``Authorization`` header is, by default, a static
    ``Bearer`` token taken from the connection ``password``. Endpoints that require
    a freshly minted or short-lived token (e.g. a Snowflake managed MCP server
    authenticated with a key-pair JWT, OAuth/refresh tokens, Workload Identity
    Federation, or GitHub App installation tokens) can pass a ``token_provider``
    callable instead. It is invoked once, the first time this hook establishes a
    connection, and its return value is used as the bearer token, so a fresh token
    is minted without storing a long-lived secret in the connection.

    For the ``stdio`` transport, the subprocess environment is, by default, the
    static ``Extra.env`` mapping -- a fine place for a value that genuinely
    belongs to this connection. When the credential has no stable static form to
    store here at all -- it lives in a different connection (e.g. a Splunk
    credential a Splunk connection already manages), or is minted fresh per call
    (e.g. a Vault lease) -- pass an ``env_provider`` callable instead. Its return
    value is merged over ``Extra.env`` (``env_provider`` keys win on conflicts),
    invoked once, the first time this hook establishes a connection.

    :param mcp_conn_id: Airflow connection ID for the MCP server.
    :param tool_prefix: Optional prefix prepended to tool names
        (e.g. ``"weather"`` → ``"weather_get_forecast"``).
    :param token_provider: Optional zero-argument callable returning a bearer
        token string. When set, it overrides the connection ``password`` for the
        ``Authorization`` header on HTTP/SSE transports. Called once, the first
        time this hook establishes a connection (the result is then cached for
        the hook's lifetime). Ignored for the ``stdio`` transport.
    :param env_provider: Optional zero-argument callable returning a
        ``dict[str, str]`` of environment variables for the ``stdio`` subprocess.
        Merged over ``Extra.env`` (``env_provider`` wins on key conflicts). Called
        once, the first time this hook establishes a connection (the result is
        then cached for the hook's lifetime). Ignored for the ``http``/``sse``
        transports.
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
        env_provider: Callable[[], dict[str, str]] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mcp_conn_id = mcp_conn_id
        self.tool_prefix = tool_prefix
        self.token_provider = token_provider
        self.env_provider = env_provider
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

    def _validate_env_dict(self, env: dict[Any, Any], source: str) -> None:
        """Raise if ``env`` (from ``source``, for error messages) has non-string keys/values."""
        for key, value in env.items():
            if not isinstance(key, str) or not key:
                raise ValueError(
                    f"{source} for connection {self.mcp_conn_id!r} must have non-empty string keys, "
                    f"got {type(key).__name__}."
                )
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"{source} for connection {self.mcp_conn_id!r} must have non-empty string values, "
                    f"got {type(value).__name__} for key {key!r}."
                )

    def _stdio_env(self, extra: dict[str, Any]) -> dict[str, str] | None:
        """
        Build the subprocess environment for the ``stdio`` transport.

        Merges the static ``Extra.env`` mapping with ``env_provider`` (minted per
        connection), with ``env_provider`` keys taking precedence. Returns ``None``
        when neither yields anything, in which case the MCP stdio client falls back
        to a small allowlist of inherited variables (see the ``mcp`` package's
        ``get_default_environment()``), not the full parent process environment.
        """
        extra_env = extra.get("env")
        if extra_env is None:
            extra_env = {}
        if not isinstance(extra_env, dict):
            raise ValueError(
                f"'env' in extra for connection {self.mcp_conn_id!r} must be an object of "
                f"string keys/values, got {type(extra_env).__name__}."
            )
        self._validate_env_dict(extra_env, "'env' in extra")

        env: dict[str, str] = dict(extra_env)
        if self.env_provider is not None:
            provided = self.env_provider()
            if not isinstance(provided, dict):
                raise ValueError(
                    f"env_provider for connection {self.mcp_conn_id!r} must return a dict[str, str], "
                    f"got {type(provided).__name__}."
                )
            self._validate_env_dict(provided, "env_provider")
            # The static connection extra is masked when the connection is fetched;
            # mask minted values too so they never leak into task logs.
            for value in provided.values():
                mask_secret(value)
            env.update(provided)

        return env or None

    def get_conn(self) -> Any:
        """
        Return a configured PydanticAI MCP toolset instance.

        Builds a :class:`~pydantic_ai.mcp.MCPToolset` over the FastMCP transport
        matching the transport type in the connection's extra field:

        - ``http`` (default): ``fastmcp.client.transports.StreamableHttpTransport``
        - ``sse``: ``fastmcp.client.transports.SSETransport``
        - ``stdio``: ``fastmcp.client.transports.StdioTransport``

        When ``tool_prefix`` is set the toolset is wrapped via
        :meth:`~pydantic_ai.toolsets.abstract.AbstractToolset.prefixed`, so a
        prefix of ``"weather"`` yields tool names like ``weather_get_forecast``.

        The result is cached for the lifetime of this hook instance.
        """
        if self._server is not None:
            return self._server

        try:
            from fastmcp.client.transports import SSETransport, StdioTransport, StreamableHttpTransport
            from pydantic_ai.mcp import MCPToolset
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
            toolset = MCPToolset(StreamableHttpTransport(conn.host, headers=self._auth_headers(conn)))
        elif transport == "sse":
            if not conn.host:
                raise ValueError(f"Connection {self.mcp_conn_id!r} requires a host URL for SSE transport.")
            toolset = MCPToolset(SSETransport(conn.host, headers=self._auth_headers(conn)))
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
            toolset = MCPToolset(
                StdioTransport(command=command, args=args, env=self._stdio_env(extra)),
                init_timeout=timeout,
            )
        else:
            raise ValueError(
                f"Unknown transport {transport!r} in connection {self.mcp_conn_id!r}. "
                "Supported: 'http', 'sse', 'stdio'."
            )

        self._server = toolset.prefixed(self.tool_prefix) if self.tool_prefix else toolset
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
