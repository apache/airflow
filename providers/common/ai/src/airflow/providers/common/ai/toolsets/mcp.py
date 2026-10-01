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

from airflow.providers.common.ai.utils.toolset_base import AirflowToolset

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from pydantic_ai._run_context import RunContext
    from pydantic_ai.toolsets.abstract import ToolsetTool

    from airflow.providers.common.ai.tools import AirflowTool


class MCPToolset(AirflowToolset):
    """
    Toolset that connects to an MCP server configured via an Airflow connection.

    Reads MCP server transport type, URL, command, and credentials from the
    connection via :class:`~airflow.providers.common.ai.hooks.mcp.MCPHook` and
    builds the matching PydanticAI :class:`~pydantic_ai.mcp.MCPToolset`.
    All ``AbstractToolset`` methods delegate to the underlying MCP toolset.

    This is the recommended way to use MCP servers in Airflow — it stores
    server configuration in Airflow connections (and secret backends) rather
    than hard-coding URLs and credentials in DAG code.

    If you prefer full PydanticAI control, you can pass a
    :class:`~pydantic_ai.mcp.MCPToolset` (built over a FastMCP transport)
    directly to ``AgentOperator(toolsets=[...])``, since it implements
    ``AbstractToolset``.

    For MCP endpoints that need a freshly minted or short-lived token (e.g. a
    Snowflake managed MCP server authenticated with a key-pair JWT, or OAuth /
    Workload Identity / GitHub App tokens), pass a ``token_provider`` callable.
    It is invoked once, the first time this toolset establishes a connection, and
    its return value is used as the bearer token, so a fresh token is minted
    rather than storing a long-lived secret in the connection.

    For ``stdio`` servers whose subprocess needs a credential that lives in a
    different connection, or one minted fresh per call (e.g. a Splunk/Vault
    token), pass an ``env_provider`` callable instead. It is invoked once, the
    first time this toolset establishes a connection, and its return value is
    merged over the connection's static ``Extra.env`` (``env_provider`` wins on
    key conflicts).

    :param mcp_conn_id: Airflow connection ID for the MCP server. Templated when
        the toolset is passed to ``AgentOperator`` / ``@task.agent``.
    :param tool_prefix: Optional prefix prepended to tool names
        (e.g. ``"weather"`` → ``"weather_get_forecast"``).
    :param token_provider: Optional zero-argument callable returning a bearer
        token string, overriding the connection ``password`` for HTTP/SSE auth.
        Called once, the first time this toolset establishes a connection.
    :param env_provider: Optional zero-argument callable returning a
        ``dict[str, str]`` merged over the connection's ``Extra.env`` (winning on
        key conflicts) for the ``stdio`` subprocess environment. Called once, the
        first time this toolset establishes a connection.
    """

    # Rendered, on a copy, by AgentOperator. Deliberately not ``template_fields``, which
    # Airflow's templater would render in place wherever the toolset is nested.
    agent_template_fields: Sequence[str] = ("_mcp_conn_id",)

    def __init__(
        self,
        mcp_conn_id: str,
        *,
        tool_prefix: str | None = None,
        token_provider: Callable[[], str] | None = None,
        env_provider: Callable[[], dict[str, str]] | None = None,
    ) -> None:
        self._mcp_conn_id = mcp_conn_id
        self._tool_prefix = tool_prefix
        self._token_provider = token_provider
        self._env_provider = env_provider
        self._server: Any = None

    @property
    def id(self) -> str:
        return f"mcp-{self._mcp_conn_id}"

    def _get_server(self) -> Any:
        if self._server is None:
            from airflow.providers.common.ai.hooks.mcp import MCPHook

            hook = MCPHook(
                mcp_conn_id=self._mcp_conn_id,
                tool_prefix=self._tool_prefix,
                token_provider=self._token_provider,
                env_provider=self._env_provider,
            )
            self._server = hook.get_conn()
        return self._server

    async def _resolve_server(self) -> Any:
        # Resolving the connection talks to the supervisor, so it takes the blocking-call lock.
        return self._server if self._server is not None else await self.run_blocking(self._get_server)

    async def __aenter__(self) -> Self:
        await (await self._resolve_server()).__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> bool | None:
        if self._server is not None:
            return await self._server.__aexit__(*args)
        return None

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        return await (await self._resolve_server()).get_tools(ctx)

    async def execute_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        *,
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        return await (await self._resolve_server()).call_tool(name, tool_args, ctx, tool)

    def airflow_tools(self) -> list[AirflowTool]:
        """
        Not supported: use the agent framework's own MCP client instead.

        An MCP session belongs to the event loop that opened it, and the framework-neutral
        tools run outside the Pydantic AI run that manages it, so every call would reconnect
        to the server, and a stdio server would restart each time.
        """
        raise NotImplementedError(
            "MCPToolset works in Pydantic AI agents and through the LangChain bridge, not through "
            "the framework-neutral tools. Connect your framework's own MCP client to the server "
            "instead, such as Strands' MCPClient or ADK's McpToolset."
        )
