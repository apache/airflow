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
import logging
import re
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, mask_secret
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
    from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterator, Sequence

    from pydantic_ai._run_context import RunContext
    from pydantic_ai.toolsets.abstract import ToolsetTool

    from airflow.providers.common.ai.tools import AirflowTool

log = logging.getLogger(__name__)

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
# What the MCP client raises when a request outlasts its read timeout (see
# mcp.shared.jsonrpc_dispatcher). A server may use the same code, so the message is matched too.
_REQUEST_TIMEOUT_CODE = -32001
_REQUEST_TIMEOUT_MESSAGE = re.compile(r"Request '[^']*' timed out")
_CLIENT_ERROR_PREFIXES = (
    "Unexpected content type:",
    "Failed to parse JSON response:",
    "Failed to parse SSE message:",
    "SSE stream ended",
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


def _describe(error: BaseException) -> str:
    return str(error) or type(error).__name__


def _find(exc: BaseException, types: type[BaseException] | tuple[type[BaseException], ...]) -> Any:
    """Return the first exception of ``types`` in ``exc``, its causes and any exception groups."""
    return next((e for e in _walk_exceptions(exc) if isinstance(e, types)), None)


def _is_handshake_timeout(error: BaseException) -> bool:
    return any(
        isinstance(e, TimeoutError)
        or (isinstance(e, RuntimeError) and str(e).endswith(_HANDSHAKE_TIMEOUT_MESSAGE))
        for e in _walk_exceptions(error)
    )


def _is_request_timeout(error: McpError | None) -> bool:
    return (
        error is not None
        and error.error.code == _REQUEST_TIMEOUT_CODE
        and _REQUEST_TIMEOUT_MESSAGE.fullmatch(error.error.message) is not None
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

    The MCP client does not say which HTTP response made a call fail, so the error statuses are
    only attributed to the operation when no other operation of the toolset ran at the same time.
    A request that got no response at all ends the MCP session, which fails every operation
    running then, so those failures are kept whatever the overlap.
    """

    overlapped: bool = False
    errors: list[tuple[int, float | None]] = field(default_factory=list)
    failures: list[Exception] = field(default_factory=list)

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

    def __init__(self, hook: DatabricksHook) -> None:
        self._hook = hook
        self._token_lock = threading.Lock()
        self._masked_token: str | None = None

    def fetch_token(self) -> str:
        with self._token_lock:
            token = self._hook._get_token(raise_error=False)
        if not token:
            raise ValueError(
                f"Connection {self._hook.databricks_conn_id!r} has no token-based authentication "
                "configured. This toolset sends a bearer token: use a personal access token, "
                "service principal OAuth, Azure AD, or workload identity federation. Username and "
                "password authentication is not supported."
            )
        return token

    def fetch_and_mask_token(self) -> None:
        """Fetch a token and mask it; only call it through ``AirflowToolset.run_blocking``."""
        self._mask(self.fetch_token())

    def _mask(self, token: str) -> None:
        # A personal access token is masked when the connection is fetched; mask minted
        # OAuth tokens too, so they never reach task logs.
        mask_secret(token)
        self._masked_token = token

    async def async_auth_flow(
        self, request: httpx2.Request
    ) -> AsyncGenerator[httpx2.Request, httpx2.Response]:
        try:
            # Not AirflowToolset.run_blocking: its lock is shared by every toolset, so a long SQL
            # query in another toolset would hold up each request here. The connection is
            # resolved when the toolset connects, so this only calls the token endpoint, and the
            # hook is this toolset's own, so a lock of its own is enough.
            token = await asyncio.to_thread(self.fetch_token)
        except Exception as e:
            raise _TokenUnavailableError(str(e)) from e
        if token != self._masked_token:
            # mask_secret messages the supervisor, which before Airflow 3.2 does not guard its
            # socket against concurrent calls; run_blocking's lock serializes them.
            await AirflowToolset.run_blocking(self._mask, token)
        request.headers["Authorization"] = f"Bearer {token}"
        yield request


class _RecordingStream(httpx2.AsyncByteStream):
    """A response body that records a transport error raised while it is read."""

    def __init__(self, stream: httpx2.AsyncByteStream, record: Callable[[Exception], None]) -> None:
        self._stream = stream
        self._record = record

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self._stream:
                yield chunk
        except httpx2.TransportError as e:
            self._record(e)
            raise

    async def aclose(self) -> None:
        await self._stream.aclose()


class _GatewayClient(httpx2.AsyncClient):
    """
    Record, on the toolset's running operations, why each JSON-RPC request to the gateway failed.

    Once the MCP session is open, requests are sent from the MCP client's own tasks, and a call
    waiting for an answer only learns that the connection closed. These records say why.
    """

    def __init__(self, *args: Any, operations: set[_Operation], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._operations = operations

    async def send(self, request: httpx2.Request, **kwargs: Any) -> httpx2.Response:
        if not _is_jsonrpc_request(request):
            return await super().send(request, **kwargs)

        def record(failure: Exception) -> None:
            for operation in self._operations:
                operation.failures.append(failure)

        try:
            response = await super().send(request, **kwargs)
        except (httpx2.TransportError, _TokenUnavailableError) as e:
            record(e)
            raise
        if response.status_code >= 400:
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            for operation in self._operations:
                operation.errors.append((response.status_code, retry_after))
        if isinstance(response.stream, httpx2.AsyncByteStream):
            response.stream = _RecordingStream(response.stream, record)
        return response


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
        auth = _DatabricksTokenAuth(hook)
        # Fail here, with a clear message, rather than inside the MCP client, which reports
        # any failure as a generic connection error.
        try:
            auth.fetch_and_mask_token()
        except ValueError:
            raise
        except Exception as e:
            raise _TokenUnavailableError(str(e)) from e
        proxy = (hook.proxies or {}).get(urlsplit(url).scheme)
        transport = StreamableHttpTransport(
            url,
            headers=hook.user_agent_header,
            auth=auth,
            httpx_client_factory=_build_client_factory(self._operations, proxy),
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
        mcp_error = _find(error, McpError)
        # When a request fails once the session is open, the call only sees an error the MCP
        # client makes up; what the request ran into is in the operation's failures.
        saw_no_answer = mcp_error is not None and _is_client_error(mcp_error)
        failure = operation.failures[0] if saw_no_answer and operation.failures else None
        token_error = _find(error, _TokenUnavailableError)
        if token_error is None and isinstance(failure, _TokenUnavailableError):
            token_error = failure
        if token_error is not None:
            return DatabricksUnityMCPError(
                f"Could not get a token from connection {self._databricks_conn_id!r} to call MCP Service "
                f"{service!r}: {token_error}"
            )
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
        # The MCP client reports its own timeouts as MCP errors, so they are told apart from the
        # server's answers before those are.
        if _is_request_timeout(mcp_error):
            return DatabricksUnityMCPTransportError(
                f"Timed out waiting for Unity Gateway to answer MCP Service {service!r}.{ambiguous}"
            )
        transport_error = _find(error, httpx2.TransportError)
        if transport_error is None and isinstance(failure, httpx2.TransportError):
            transport_error = failure
        if transport_error is not None:
            return DatabricksUnityMCPTransportError(
                f"Could not reach Unity Gateway for MCP Service {service!r}: "
                f"{_describe(transport_error)}.{ambiguous}"
            )
        if during_tool_call and _is_handshake_timeout(error):
            return DatabricksUnityMCPTransportError(
                f"Timed out waiting for Unity Gateway to answer MCP Service {service!r}.{ambiguous}"
            )
        if _is_handshake_timeout(error):
            return DatabricksUnityMCPTransportError(
                f"Unity Gateway did not complete the MCP handshake for MCP Service {service!r} within "
                f"{self._init_timeout:g} seconds. Raise init_timeout if the service is slow to answer."
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
        if self._server is None:
            return None
        if args[0] is None:
            return await self._server.__aexit__(*args)
        try:
            return await self._server.__aexit__(*args)
        except Exception:
            # Closing the session re-raises the failure of a request that already failed, such
            # as the read timeout of a tool call, which would hide the error explaining it.
            log.warning(
                "Error while closing the session of MCP Service %r", self._service_name, exc_info=True
            )
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


def _build_client_factory(operations: set[_Operation], proxy: str | None) -> Any:
    """Return an MCP HTTP client factory for gateway requests, sent through ``proxy`` if one is set."""

    def factory(
        headers: dict[str, str] | None = None,
        timeout: httpx2.Timeout | None = None,
        auth: httpx2.Auth | None = None,
        **kwargs: Any,
    ) -> httpx2.AsyncClient:
        # The MCP client's defaults: a response stream may stay open, so reads wait longer.
        timeout = timeout or httpx2.Timeout(30.0, read=300.0)
        if proxy:
            kwargs["proxy"] = proxy
        return _GatewayClient(headers=headers, timeout=timeout, auth=auth, operations=operations, **kwargs)

    return factory
