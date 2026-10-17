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
"""Toolset that gives a common.ai agent the tools of a Unity Gateway MCP Service."""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
import re
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.databricks.exceptions import (
    DatabricksUnityMCPAccessDeniedError,
    DatabricksUnityMCPError,
    DatabricksUnityMCPThrottledError,
    DatabricksUnityMCPTransportError,
)
from airflow.providers.databricks.hooks.databricks import DatabricksHook

try:
    # httpx2 and fastmcp come with pydantic-ai's mcp extra, which the common.ai extra installs.
    import httpx2
    from fastmcp.client.transports import StreamableHttpTransport
    from fastmcp.exceptions import McpError
    from pydantic_ai.mcp import MCPToolset as PydanticAIMCPToolset

    from airflow.providers.common.ai.utils.toolset_base import AirflowToolset
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(
        "DatabricksUnityMCPToolset needs the 'common.ai' extra of the databricks provider: "
        "pip install 'apache-airflow-providers-databricks[common.ai]'"
    ) from e

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Iterator, Sequence

    from pydantic_ai._run_context import RunContext
    from pydantic_ai.toolsets.abstract import ToolsetTool

    from airflow.providers.common.ai.tools import AirflowTool
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
_SERVICE_NAME_PART = r"[A-Za-z0-9_]+"
_SERVICE_NAME = re.compile(rf"{_SERVICE_NAME_PART}\.{_SERVICE_NAME_PART}\.{_SERVICE_NAME_PART}")

# The JSON-RPC error code Unity Gateway answers with, at HTTP 403, when the caller may not invoke
# the service, including when no service has that name.
_NOT_AUTHORIZED_CODE = -32007

# Errors the MCP client makes up when the gateway gave no JSON-RPC answer, such as an HTTP error
# with a plain body or a response that never arrived (see mcp.client.streamable_http). Every other
# MCP error is the server's own answer. Matched on code and message, the only things they carry.
_CLIENT_ERRORS = {
    (-32603, "Server returned an error response"),
    (-32601, "Not Found"),
    (-32600, "Session terminated"),
    (-32000, "Connection closed"),
}
# What fastmcp raises, in place of the TimeoutError it loses, when the MCP handshake outlasts
# init_timeout (see fastmcp.client.client).
_HANDSHAKE_TIMEOUT_MESSAGE = "Failed to initialize server session"
_CLIENT_ERROR_PREFIXES = (
    "Unexpected content type:",
    "Failed to parse JSON response:",
    "Failed to parse SSE message:",
    "SSE stream ended without a response",
    "server answered a request with 202 Accepted",
    "Redirect to ",
)


def validate_service_name(service_name: str) -> None:
    """
    Raise ``ValueError`` unless ``service_name`` is a three-level ``catalog.schema.service`` name.

    Each part may contain only ASCII letters, digits and underscores.
    """
    if not isinstance(service_name, str) or not _SERVICE_NAME.fullmatch(service_name):
        raise ValueError(
            f"Invalid Unity Gateway MCP Service name {service_name!r}: expected "
            "'catalog.schema.service', each part made of ASCII letters, digits or '_'."
        )


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return False


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        # The HTTP-date form is not worth parsing: the caller only uses this as a hint.
        return None
    return seconds if seconds >= 0 else None


def _walk_exceptions(exc: BaseException) -> Iterator[BaseException]:
    """Yield ``exc``, its causes and the exceptions inside any exception groups, each once."""
    seen: set[int] = set()
    pending: list[BaseException] = [exc]
    while pending:
        current = pending.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        # Duck-typed: the ExceptionGroup builtin is Python 3.11+, and anyio uses the backport on 3.10.
        if isinstance(grouped := getattr(current, "exceptions", None), (list, tuple)):
            pending.extend(e for e in grouped if isinstance(e, BaseException))
        pending.extend(e for e in (current.__cause__, current.__context__) if e is not None)


def _find(exc: BaseException, types: type[BaseException] | tuple[type[BaseException], ...]) -> Any:
    """Return the first exception of ``types`` in ``exc``, its causes and any exception groups."""
    return next((e for e in _walk_exceptions(exc) if isinstance(e, types)), None)


def _is_handshake_timeout(error: BaseException) -> bool:
    return any(
        isinstance(e, TimeoutError)
        or (isinstance(e, RuntimeError) and str(e).endswith(_HANDSHAKE_TIMEOUT_MESSAGE))
        for e in _walk_exceptions(error)
    )


def _is_client_error(error: McpError) -> bool:
    code, message = error.error.code, error.error.message
    return (code, message) in _CLIENT_ERRORS or message.startswith(_CLIENT_ERROR_PREFIXES)


class _TokenUnavailableError(Exception):
    """Raised, for this module only, when no token could be fetched for a gateway request."""


@dataclass(eq=False)
class _Operation:
    """
    One toolset operation (connecting, listing tools or calling a tool) and the gateway errors seen while it ran.

    The MCP client does not say which HTTP response made a call fail, so the errors are only
    attributed to the operation when no other operation of the toolset ran at the same time.
    """

    overlapped: bool = False
    errors: list[tuple[int, float | None]] = field(default_factory=list)

    @property
    def http_error(self) -> tuple[int, float | None] | None:
        """Return the status and ``Retry-After`` of the last error response it got, if it is known."""
        return self.errors[-1] if self.errors and not self.overlapped else None


class _DatabricksTokenAuth(httpx2.Auth):
    """
    Authenticate each gateway request with a token from the Databricks connection.

    Asking the hook on every request, rather than once, lets OAuth tokens refresh during a long
    agent run and on reconnection; the hook caches tokens until they are about to expire.
    """

    def __init__(self, hook: DatabricksHook, operations: set[_Operation]) -> None:
        self._hook = hook
        self._operations = operations
        self._token_lock = threading.Lock()

    def get_token(self) -> str:
        with self._token_lock:
            token = self._hook._get_token(raise_error=False)
        if not token:
            raise ValueError(
                f"Connection {self._hook.databricks_conn_id!r} has no token-based authentication "
                "configured. This toolset sends a bearer token: use a personal access token, "
                "service principal OAuth, Azure AD, or workload identity federation. Username and "
                "password authentication is not supported."
            )
        # A personal access token is masked when the connection is fetched; mask minted
        # OAuth tokens too, so they never reach task logs.
        mask_secret(token)
        return token

    async def async_auth_flow(
        self, request: httpx2.Request
    ) -> AsyncGenerator[httpx2.Request, httpx2.Response]:
        try:
            # Not AirflowToolset.run_blocking: its lock is shared by every toolset, so a long SQL
            # query in another toolset would hold up each request here. The connection is already
            # resolved when the toolset connects, so fetching a token only calls the token
            # endpoint, and the hook is this toolset's own, so a lock of its own is enough.
            token = await asyncio.to_thread(self.get_token)
        except Exception as e:
            raise _TokenUnavailableError(str(e)) from e
        request.headers["Authorization"] = f"Bearer {token}"
        response = yield request
        if response.status_code >= 400 and _is_jsonrpc_request(request):
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            for operation in self._operations:
                operation.errors.append((response.status_code, retry_after))


def _is_jsonrpc_request(request: httpx2.Request) -> bool:
    """
    Whether ``request`` sends a JSON-RPC request, which expects an answer.

    Not a notification or a GET for server events: the MCP client tolerates errors on those, so
    they say nothing about why an operation failed.
    """
    if request.method != "POST":
        return False
    try:
        return "id" in json.loads(request.content)
    except (ValueError, TypeError):
        return False


class DatabricksUnityMCPToolset(AirflowToolset):
    """
    Give an agent the tools of a Unity Gateway MCP Service, authenticated as the connection's identity.

    The service is named by its three-level Unity Catalog name, ``catalog.schema.service``, and
    reached at ``https://<workspace host>/ai-gateway/mcp-services/<catalog.schema.service>``. The
    workspace host and the credentials both come from the Databricks connection, so Dag code holds
    neither a gateway URL nor a token, and the token is only ever sent to that workspace, over
    HTTPS. The connection's ``proxies`` extra applies to gateway requests.

    The gateway runs every tool call as the identity of the connection's credentials (a user's
    personal access token, a service principal, or an Azure AD / federated identity). That identity
    needs ``EXECUTE`` on the MCP Service, ``USE CATALOG`` and ``USE SCHEMA`` on its catalog and
    schema, and an assignment to the workspace. It sees only the tools selected for the service, and
    the service's policies apply.

    Tokens are fetched from the connection for each request, so OAuth tokens refresh during a long
    agent run and on reconnection. An error the server returns for a tool call reaches the model,
    which can correct its arguments and try again. When the gateway gives no answer of its own, a
    tool call may or may not have run, so it is never retried; it fails the task with
    :class:`~airflow.providers.databricks.exceptions.DatabricksUnityMCPAccessDeniedError`,
    :class:`~airflow.providers.databricks.exceptions.DatabricksUnityMCPThrottledError`,
    :class:`~airflow.providers.databricks.exceptions.DatabricksUnityMCPTransportError`, or
    :class:`~airflow.providers.databricks.exceptions.DatabricksUnityMCPError`, as do failures
    while connecting and listing tools.

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
    :param init_timeout: Seconds to wait for the MCP handshake when connecting. Unity Gateway forwards
        a response only once the MCP server has finished sending it, so a server that keeps its
        response stream open needs more than pydantic-ai's default of 5 seconds.
    """

    # Rendered, on a copy, by AgentOperator.
    agent_template_fields: Sequence[str] = ("_databricks_conn_id", "_service_name")

    def __init__(
        self,
        service_name: str,
        *,
        databricks_conn_id: str = DatabricksHook.default_conn_name,
        tool_prefix: str | None = None,
        init_timeout: float = 30,
    ) -> None:
        if not init_timeout > 0:
            raise ValueError(f"init_timeout must be a positive number of seconds, got {init_timeout!r}.")
        self._databricks_conn_id = databricks_conn_id
        self._service_name = service_name
        self._tool_prefix = tool_prefix
        self._init_timeout = init_timeout
        self._server: Any = None
        self._operations: set[_Operation] = set()
        # A templated name is checked once it has been rendered, when the toolset connects.
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
        url = hook._endpoint_url(f"{GATEWAY_MCP_SERVICES_PATH}/{self._service_name}")
        parts = urlsplit(url)
        if parts.scheme != "https" and not _is_loopback(parts.hostname or ""):
            raise ValueError(
                f"Connection {self._databricks_conn_id!r} uses the {parts.scheme!r} scheme. Unity Gateway "
                "requests carry a bearer token, so they are only sent over HTTPS."
            )
        return url

    def _get_server(self) -> Any:
        hook = DatabricksHook(self._databricks_conn_id, caller=type(self).__name__)
        url = self.get_service_url(hook)
        auth = _DatabricksTokenAuth(hook, self._operations)
        # Fail here, with a clear message, rather than inside the MCP client, which reports
        # any failure as a generic connection error.
        auth.get_token()
        proxy = (hook.proxies or {}).get(urlsplit(url).scheme)
        transport = StreamableHttpTransport(
            url,
            headers=hook.user_agent_header,
            auth=auth,
            httpx_client_factory=_build_proxied_client_factory(proxy) if proxy else None,
        )
        toolset = PydanticAIMCPToolset(transport, init_timeout=self._init_timeout)
        return toolset.prefixed(self._tool_prefix) if self._tool_prefix else toolset

    async def _resolve_server(self) -> Any:
        if self._server is None:
            # Resolving the connection talks to the supervisor, so it takes the blocking-call lock.
            self._server = await self.run_blocking(self._get_server)
        return self._server

    def _translate_error(
        self, error: Exception, operation: _Operation, *, during_tool_call: bool
    ) -> Exception | None:
        """Return the provider exception for a gateway failure, or ``None`` to re-raise ``error`` as is."""
        service = self._service_name
        if (token_error := _find(error, _TokenUnavailableError)) is not None:
            return DatabricksUnityMCPError(
                f"Could not get a token from connection {self._databricks_conn_id!r} to call MCP Service "
                f"{service!r}: {token_error}"
            )
        mcp_error = _find(error, McpError)
        status, retry_after = operation.http_error or (None, None)
        if (mcp_error is not None and mcp_error.error.code == _NOT_AUTHORIZED_CODE) or status in (401, 403):
            return DatabricksUnityMCPAccessDeniedError(
                f"Unity Gateway denied access to MCP Service {service!r}"
                f"{f' (HTTP {status})' if status else ''}. Either no service has that name, or the "
                f"identity of connection {self._databricks_conn_id!r} lacks EXECUTE on the service or "
                "USE CATALOG and USE SCHEMA on its catalog and schema, or its credentials are not valid.",
                http_status_code=status,
            )
        if status == 429:
            hint = f" Retry after {retry_after:g} seconds." if retry_after is not None else ""
            return DatabricksUnityMCPThrottledError(
                f"Unity Gateway rate-limited calls to MCP Service {service!r} (HTTP 429).{hint}",
                http_status_code=status,
                retry_after=retry_after,
            )
        ambiguous = (
            " The tool call may or may not have run, so it was not retried." if during_tool_call else ""
        )
        if mcp_error is not None:
            if not _is_client_error(mcp_error):
                # The server's own answer: for a tool call, it reaches the model as a retry.
                if during_tool_call:
                    return None
                return DatabricksUnityMCPError(
                    f"MCP Service {service!r} returned an error: {mcp_error.error.message} "
                    f"(code {mcp_error.error.code}).",
                    http_status_code=status,
                )
            what = f"HTTP {status}" if status else f"an error without an answer ({mcp_error.error.message})"
            return DatabricksUnityMCPError(
                f"Unity Gateway returned {what} for MCP Service {service!r}.{ambiguous}",
                http_status_code=status,
            )
        if (transport_error := _find(error, httpx2.TransportError)) is not None:
            return DatabricksUnityMCPTransportError(
                f"Could not reach Unity Gateway for MCP Service {service!r}: {transport_error}.{ambiguous}"
            )
        if _is_handshake_timeout(error):
            if during_tool_call:
                return DatabricksUnityMCPTransportError(
                    f"Timed out waiting for Unity Gateway to answer MCP Service {service!r}.{ambiguous}"
                )
            return DatabricksUnityMCPTransportError(
                f"Unity Gateway did not complete the MCP handshake for MCP Service {service!r} within "
                f"{self._init_timeout:g} seconds. Raise init_timeout if the service is slow to answer."
            )
        return None

    @contextlib.asynccontextmanager
    async def _gateway_operation(self, *, during_tool_call: bool = False) -> AsyncIterator[None]:
        operation = _Operation(overlapped=bool(self._operations))
        for other in self._operations:
            other.overlapped = True
        self._operations.add(operation)
        try:
            yield
        except Exception as e:
            translated = self._translate_error(e, operation, during_tool_call=during_tool_call)
            if translated is None:
                raise
            raise translated from e
        finally:
            self._operations.discard(operation)

    async def __aenter__(self) -> DatabricksUnityMCPToolset:
        async with self._gateway_operation():
            await (await self._resolve_server()).__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> bool | None:
        if self._server is not None:
            return await self._server.__aexit__(*args)
        return None

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        async with self._gateway_operation():
            return await (await self._resolve_server()).get_tools(ctx)

    async def execute_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        *,
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        async with self._gateway_operation(during_tool_call=True):
            return await (await self._resolve_server()).call_tool(name, tool_args, ctx, tool)

    def airflow_tools(self) -> list[AirflowTool]:
        """
        Not supported: use the agent framework's own MCP client instead.

        An MCP session belongs to the event loop that opened it, and the framework-neutral
        tools run outside the Pydantic AI run that manages it, so every call would reconnect.
        """
        raise NotImplementedError(
            "DatabricksUnityMCPToolset works in Pydantic AI agents and through the LangChain bridge, "
            "not through the framework-neutral tools."
        )


def _build_proxied_client_factory(proxy: str) -> Any:
    """Return an MCP HTTP client factory whose clients send requests through ``proxy``."""

    def factory(
        headers: dict[str, str] | None = None,
        timeout: httpx2.Timeout | None = None,
        auth: httpx2.Auth | None = None,
        **kwargs: Any,
    ) -> httpx2.AsyncClient:
        # The MCP client's defaults: a response stream may stay open, so reads wait longer.
        timeout = timeout or httpx2.Timeout(30.0, read=300.0)
        return httpx2.AsyncClient(headers=headers, timeout=timeout, auth=auth, proxy=proxy, **kwargs)

    return factory
