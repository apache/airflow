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
"""Toolset that gives a common.ai agent the tools of a Unity AI Gateway MCP Service."""

from __future__ import annotations

import asyncio
import contextlib
import re
import threading
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.databricks.exceptions import (
    DatabricksUnityMCPAccessDeniedError,
    DatabricksUnityMCPError,
    DatabricksUnityMCPServiceNotFoundError,
    DatabricksUnityMCPThrottledError,
    DatabricksUnityMCPTransportError,
)
from airflow.providers.databricks.hooks.databricks import DatabricksHook

try:
    import httpx2
    from fastmcp.client.transports import StreamableHttpTransport
    from pydantic_ai.mcp import MCPToolset as PydanticAIMCPToolset

    from airflow.providers.common.ai.toolsets.mcp import MCPToolset
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(
        "DatabricksUnityMCPToolset needs the 'common.ai' extra of the databricks provider: "
        "pip install 'apache-airflow-providers-databricks[common.ai]'"
    ) from e

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Generator, Sequence

    from pydantic_ai._run_context import RunContext
    from pydantic_ai.toolsets.abstract import ToolsetTool

    from airflow.sdk.execution_time.secrets_masker import mask_secret
else:
    try:
        from airflow.sdk.log import mask_secret
    except ImportError:
        try:
            from airflow.sdk.execution_time.secrets_masker import mask_secret
        except ImportError:
            from airflow.utils.log.secrets_masker import mask_secret

GATEWAY_MCP_SERVICES_PATH = "ai-gateway/mcp-services"

# Restricting each part to these characters keeps the name from adding a path, query or
# fragment to the URL, so the request can only reach the service path on the connection's host.
_SERVICE_NAME_PART = r"[A-Za-z0-9_-]+"
_SERVICE_NAME = re.compile(rf"{_SERVICE_NAME_PART}\.{_SERVICE_NAME_PART}\.{_SERVICE_NAME_PART}")


def validate_service_name(service_name: str) -> None:
    """
    Raise ``ValueError`` unless ``service_name`` is a three-level ``catalog.schema.service`` name.

    Each part may contain only ASCII letters, digits, underscores and hyphens.
    """
    if not isinstance(service_name, str) or not _SERVICE_NAME.fullmatch(service_name):
        raise ValueError(
            f"Invalid Unity AI Gateway MCP Service name {service_name!r}: expected "
            "'catalog.schema.service', each part made of ASCII letters, digits, '_' or '-'."
        )


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        # The HTTP-date form is not worth parsing: the caller only uses this as a hint.
        return None
    return seconds if seconds >= 0 else None


def _find_transport_error(exc: BaseException) -> httpx2.TransportError | None:
    """Return the network error behind ``exc``, looking through causes and exception groups."""
    seen: set[int] = set()
    pending: list[BaseException] = [exc]
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, httpx2.TransportError):
            return current
        # Duck-typed: the ExceptionGroup builtin is Python 3.11+, and anyio uses the backport on 3.10.
        if isinstance(grouped := getattr(current, "exceptions", None), (list, tuple)):
            pending.extend(e for e in grouped if isinstance(e, BaseException))
        pending.extend(e for e in (current.__cause__, current.__context__) if e is not None)
    return None


class _DatabricksTokenAuth(httpx2.Auth):
    """
    Authenticate each gateway request with a token from the Databricks connection.

    Asking the hook on every request, rather than once, lets OAuth tokens refresh during a long
    agent run and on reconnection; the hook caches tokens until they are about to expire.

    The MCP client reports every HTTP error from the gateway as the same generic error, so the
    status and ``Retry-After`` of the last error response, and any failure to get a token, are
    kept here for :class:`DatabricksUnityMCPToolset` to report what went wrong. With concurrent
    calls on one toolset, the error reported for a failed call can be another call's.
    """

    def __init__(self, hook: DatabricksHook) -> None:
        self._hook = hook
        self.error_status: int | None = None
        self.retry_after: float | None = None
        self.token_error: Exception | None = None
        self._token_lock = threading.Lock()

    def get_token(self) -> str:
        with self._token_lock:
            token = self._hook._get_token(raise_error=False)
        if not token:
            raise ValueError(
                f"Connection {self._hook.databricks_conn_id!r} has no token-based authentication "
                "configured. Unity AI Gateway accepts bearer tokens only: use a personal access "
                "token, service principal OAuth, Azure AD, or workload identity federation. "
                "Username and password authentication is not supported."
            )
        # A personal access token is masked when the connection is fetched; mask minted
        # OAuth tokens too, so they never reach task logs.
        mask_secret(token)
        return token

    def take_error(self) -> tuple[int | None, float | None, Exception | None]:
        """Return the last recorded error and forget it."""
        error = (self.error_status, self.retry_after, self.token_error)
        self.error_status = self.retry_after = self.token_error = None
        return error

    def _record(self, response: httpx2.Response) -> None:
        if response.status_code >= 400:
            self.error_status = response.status_code
            self.retry_after = _parse_retry_after(response.headers.get("Retry-After"))

    def sync_auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response, None]:
        try:
            token = self.get_token()
        except Exception as e:
            self.token_error = e
            raise
        request.headers["Authorization"] = f"Bearer {token}"
        response = yield request
        self._record(response)

    async def async_auth_flow(
        self, request: httpx2.Request
    ) -> AsyncGenerator[httpx2.Request, httpx2.Response]:
        try:
            # Not AirflowToolset.run_blocking: its lock is shared by every toolset, so a long SQL
            # query in another toolset would hold up each request here. The connection is already
            # resolved by _get_server, so fetching a token only calls the token endpoint, and the
            # hook is this toolset's own, so a lock of its own is enough.
            token = await asyncio.to_thread(self.get_token)
        except Exception as e:
            self.token_error = e
            raise
        request.headers["Authorization"] = f"Bearer {token}"
        response = yield request
        self._record(response)


class DatabricksUnityMCPToolset(MCPToolset):
    """
    Give an agent the tools of a Unity AI Gateway MCP Service, authenticated as the connection's identity.

    The service is named by its three-level Unity Catalog name, ``catalog.schema.service``, and
    reached at ``https://<workspace host>/ai-gateway/mcp-services/<catalog.schema.service>``. The
    workspace host and the credentials both come from the Databricks connection, so Dag code holds
    neither a gateway URL nor a token, and the token is only ever sent to that workspace.

    The gateway runs every tool call as the identity of the connection's credentials (a user's
    personal access token, a service principal, or an Azure AD / federated identity), which needs
    ``EXECUTE`` on the MCP Service and ``USE CATALOG`` and ``USE SCHEMA`` on its catalog and schema.
    It sees only the tools selected for the service, and the service's policies apply.

    Tokens are fetched from the connection for each request, so OAuth tokens refresh during a long
    agent run and on reconnection. Gateway errors are raised as
    :class:`~airflow.providers.databricks.exceptions.DatabricksUnityMCPAccessDeniedError`,
    :class:`~airflow.providers.databricks.exceptions.DatabricksUnityMCPServiceNotFoundError`,
    :class:`~airflow.providers.databricks.exceptions.DatabricksUnityMCPThrottledError` or
    :class:`~airflow.providers.databricks.exceptions.DatabricksUnityMCPTransportError`. Tool calls
    are never retried by this toolset, because a call interrupted after it was sent may already
    have run.

    .. code-block:: python

        from airflow.providers.common.ai.operators.agent import AgentOperator
        from airflow.providers.databricks.toolsets.unity_mcp import DatabricksUnityMCPToolset

        AgentOperator(
            task_id="ask_mcp_service",
            prompt="Which tools do you have?",
            llm_conn_id="pydanticai_default",
            toolsets=[DatabricksUnityMCPToolset("main.default.my_mcp", databricks_conn_id="databricks")],
        )

    :param service_name: Three-level name of the MCP Service, ``catalog.schema.service``. Templated
        when the toolset is passed to ``AgentOperator`` / ``@task.agent``.
    :param databricks_conn_id: Databricks connection whose host and credentials are used. Templated
        when the toolset is passed to ``AgentOperator`` / ``@task.agent``.
    :param tool_prefix: Optional prefix prepended to tool names.
    """

    agent_template_fields: Sequence[str] = ("_databricks_conn_id", "_service_name")

    def __init__(
        self,
        service_name: str,
        *,
        databricks_conn_id: str = DatabricksHook.default_conn_name,
        tool_prefix: str | None = None,
    ) -> None:
        super().__init__(databricks_conn_id, tool_prefix=tool_prefix)
        self._databricks_conn_id = databricks_conn_id
        self._service_name = service_name
        self._auth: _DatabricksTokenAuth | None = None
        # A templated name is checked once it has been rendered, in _get_server.
        if "{{" not in service_name:
            validate_service_name(service_name)

    @property
    def id(self) -> str:
        return f"databricks-unity-mcp-{self._databricks_conn_id}-{self._service_name}"

    def get_service_url(self, hook: DatabricksHook) -> str:
        """Return the gateway URL of the MCP Service on the connection's workspace."""
        validate_service_name(self._service_name)
        if not hook.host:
            raise ValueError(f"Connection {self._databricks_conn_id!r} has no workspace host.")
        return hook._endpoint_url(f"{GATEWAY_MCP_SERVICES_PATH}/{self._service_name}")

    def _get_server(self) -> Any:
        if self._server is None:
            hook = DatabricksHook(self._databricks_conn_id, caller=type(self).__name__)
            url = self.get_service_url(hook)
            auth = _DatabricksTokenAuth(hook)
            # Fail here, with a clear message, rather than inside the MCP client, which reports
            # any failure as a generic connection error.
            auth.get_token()
            transport = StreamableHttpTransport(url, headers=hook.user_agent_header, auth=auth)
            toolset = PydanticAIMCPToolset(transport)
            self._auth = auth
            self._server = toolset.prefixed(self._tool_prefix) if self._tool_prefix else toolset
        return self._server

    def _translate_error(self, error: Exception, *, during_tool_call: bool) -> Exception | None:
        """Return the provider exception for a gateway failure, or ``None`` to re-raise ``error`` as is."""
        status, retry_after, token_error = self._auth.take_error() if self._auth else (None, None, None)
        service = self._service_name
        if token_error is not None:
            return DatabricksUnityMCPError(
                f"Could not get a token from connection {self._databricks_conn_id!r} to call MCP Service "
                f"{service!r}: {token_error}"
            )
        if status in (401, 403):
            return DatabricksUnityMCPAccessDeniedError(
                f"Unity AI Gateway denied access to MCP Service {service!r} (HTTP {status}). The "
                f"identity of connection {self._databricks_conn_id!r} needs EXECUTE on the service and "
                "USE CATALOG and USE SCHEMA on its catalog and schema, and its credentials must be valid.",
                http_status_code=status,
            )
        if status == 404:
            return DatabricksUnityMCPServiceNotFoundError(
                f"MCP Service {service!r} was not found on the workspace of connection "
                f"{self._databricks_conn_id!r}, or is not visible to its identity (HTTP 404).",
                http_status_code=status,
            )
        if status == 429:
            hint = f" Retry after {retry_after:g} seconds." if retry_after is not None else ""
            return DatabricksUnityMCPThrottledError(
                f"Unity AI Gateway rate-limited calls to MCP Service {service!r} (HTTP 429).{hint}",
                http_status_code=status,
                retry_after=retry_after,
            )
        ambiguous = (
            " The tool call may or may not have run, so it was not retried." if during_tool_call else ""
        )
        if status is not None:
            return DatabricksUnityMCPError(
                f"Unity AI Gateway returned HTTP {status} for MCP Service {service!r}.{ambiguous}",
                http_status_code=status,
            )
        if (transport_error := _find_transport_error(error)) is not None:
            return DatabricksUnityMCPTransportError(
                f"Could not reach Unity AI Gateway for MCP Service {service!r}: {transport_error}.{ambiguous}"
            )
        return None

    @contextlib.asynccontextmanager
    async def _gateway_errors(self, *, during_tool_call: bool = False) -> AsyncIterator[None]:
        try:
            yield
        except Exception as e:
            translated = self._translate_error(e, during_tool_call=during_tool_call)
            if translated is None:
                raise
            raise translated from e

    async def __aenter__(self) -> DatabricksUnityMCPToolset:
        async with self._gateway_errors():
            await super().__aenter__()
        return self

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        async with self._gateway_errors():
            return await super().get_tools(ctx)

    async def execute_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        *,
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        async with self._gateway_errors(during_tool_call=True):
            return await super().execute_tool(name, tool_args, ctx=ctx, tool=tool)
