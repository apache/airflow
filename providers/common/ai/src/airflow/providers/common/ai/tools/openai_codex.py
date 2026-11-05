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
Give Airflow tools to an agent loop run by the OpenAI Codex SDK.

.. note:: Experimental; see :ref:`howto/stability`.
"""

from __future__ import annotations

try:
    import uvicorn
    from mcp import types as mcp_types
    from mcp.server.lowlevel.server import Server
    from openai_codex import ApprovalMode, CodexConfig, Sandbox, TurnResult
    from openai_codex.generated.v2_all import (
        AgentMessageThreadItem,
        ItemCompletedNotification,
        MessagePhase,
        ThreadTokenUsageUpdatedNotification,
        TurnCompletedNotification,
        TurnStatus,
    )
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

import asyncio
import contextlib
import copy
import hmac
import json
import logging
import os
import re
import secrets
import shutil
import socket
import tempfile
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, collect_tools
from airflow.providers.common.ai.tools._from_toolset import tool_call_scope
from airflow.providers.common.ai.utils.tool_definition import serialize_for_llm
from airflow.providers.common.ai.utils.tool_metrics import calling_framework

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterator, MutableMapping

    from mcp.server.context import ServerRequestContext
    from openai_codex import AsyncTurnHandle
    from openai_codex.generated.v2_all import ThreadItem, ThreadTokenUsage

    from airflow.providers.common.ai.tools import ToolProvider

log = logging.getLogger(__name__)

__all__ = ["AirflowTools"]


_MAX_ERROR_DETAIL_CHARS = 500
# Every constant below was observed with openai-codex 0.163.0, not a documented guarantee.
#
# The ceiling on a tool result this adapter lets through. It is our own choice: unlike the Claude
# CLI, Codex has no fixed clamp that we know of. A result over it is cut here, with a notice the
# model can act on, instead of by Codex, whose truncation of an MCP result is unverified.
_MAX_INLINE_RESULT_CHARS = 100_000
# Set to the character ceiling so that Codex is unlikely to cut a result before
# ``_truncate_result_text`` does. A token can be shorter than a character for some text (CJK,
# emoji), so this is not a guarantee.
_TOOL_OUTPUT_TOKEN_LIMIT = _MAX_INLINE_RESULT_CHARS
# Codex's default budget is 5_000 bytes of input schema per MCP server; over it, Codex compresses
# the schema and drops parameter descriptions. The largest schema of the toolsets this provider
# ships is ~1 KB, so this leaves room; ``_build_config_overrides`` raises it for a larger one.
_MIN_TOOL_INPUT_SCHEMA_BYTES = 65_536
# Codex stops waiting for a tool after this long (its default is 60 s, too short for a SQL query).
# What happens to the hook when Codex gives up is not verified.
_DEFAULT_TOOL_TIMEOUT_SEC = 600.0
# How long ``serve()`` waits for the HTTP server to start, and for open connections to drain on exit.
_SERVER_START_TIMEOUT_SEC = 10.0
# How long ``run()`` waits for Codex to confirm an interrupt before it raises the tool failure anyway.
_INTERRUPT_TIMEOUT_SEC = 10.0
_SERVER_SHUTDOWN_TIMEOUT_SEC = 5
# The variable that carries the bearer token to the Codex child process. Only its name is in
# ``codex --config`` (so in argv, visible to ``ps``); the value goes through the environment.
_MCP_TOKEN_ENV_VAR = "AIRFLOW_CODEX_MCP_TOKEN"
# ``server_name`` becomes an unquoted TOML key in ``mcp_servers.<server_name>.*``.
_SERVER_NAME_RE = re.compile(r"[A-Za-z0-9_-]+")


def _truncate_result_text(text: str) -> str:
    if len(text) <= _MAX_INLINE_RESULT_CHARS:
        return text
    notice = (
        f"\n\n[Truncated: this result had {len(text)} characters and only the beginning is shown. "
        "Narrow the request, for example by adding filters, selecting fewer columns or lowering "
        "the LIMIT, to get a smaller result.]"
    )
    return text[: _MAX_INLINE_RESULT_CHARS - len(notice)] + notice


def _bound_detail(text: str) -> str:
    omitted = len(text) - _MAX_ERROR_DETAIL_CHARS
    return text if omitted <= 0 else f"{text[:_MAX_ERROR_DETAIL_CHARS]}...(+{omitted} chars)"


def _to_toml_string(value: str) -> str:
    # A JSON string is a valid TOML basic string.
    return json.dumps(value)


def _find_final_response(items: list[ThreadItem]) -> str | None:
    """Pick the final answer the way the SDK's ``thread.run()`` does, which keeps that helper private."""
    last_unphased: str | None = None
    for item in reversed(items):
        message = getattr(item, "root", item)
        if not isinstance(message, AgentMessageThreadItem):
            continue
        if message.phase == MessagePhase.final_answer:
            return message.text
        if message.phase is None and last_unphased is None:
            last_unphased = message.text
    return last_unphased


async def _close_stream(events: AsyncIterable[Any]) -> None:
    # ``AsyncIterable`` does not guarantee ``aclose``; the stream ``AsyncTurnHandle.stream()`` returns does.
    aclose = getattr(events, "aclose", None)
    if aclose is not None:
        await aclose()


class _Run:
    """
    One run of :meth:`AirflowTools.run`.

    Collects its calls' failures and records whether it has ended.
    """

    def __init__(self) -> None:
        self.failures: list[ToolCallError] = []
        self.ended = False


@dataclass(frozen=True)
class _Serving:
    """What a ``serve()`` block hands to :meth:`AirflowTools.build_thread_options`."""

    cwd: str


class _BearerTokenMiddleware:
    """Let a request through only when it carries the run's bearer token; deny every other scope by default."""

    def __init__(self, app: Any, token: str) -> None:
        self._app = app
        self._expected = f"Bearer {token}".encode()

    async def __call__(self, scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        scope_type = scope["type"]
        if scope_type == "lifespan":
            await self._app(scope, receive, send)
            return
        supplied = next((value for name, value in scope.get("headers", []) if name == b"authorization"), b"")
        if hmac.compare_digest(supplied, self._expected):
            await self._app(scope, receive, send)
        elif scope_type == "http":
            await send(
                {
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [(b"content-type", b"text/plain")],
                }
            )
            await send({"type": "http.response.body", "body": b"Unauthorized"})
        elif scope_type == "websocket":
            await send({"type": "websocket.close", "code": 1008})


class _InProcessServer(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        # uvicorn would replace the worker's SIGTERM and SIGINT handlers for as long as it serves,
        # so the task could no longer be stopped by them. The server is stopped by ``serve()`` alone.
        yield


class AirflowTools:
    """
    Airflow's tools for an agent loop run by the OpenAI Codex SDK.

    Pass toolsets that implement
    :class:`~airflow.providers.common.ai.tools.ToolProvider`, such as ``SQLToolset``
    and ``HookToolset``, or individual
    :class:`~airflow.providers.common.ai.tools.AirflowTool` objects. Codex has no
    in-process tools, so :meth:`serve` starts an MCP server on ``127.0.0.1`` in the
    running event loop and points a ``codex app-server`` child process at it. Start a
    thread with :meth:`build_thread_options` and drive its turn with :meth:`run`:

    .. code-block:: python

        from openai_codex import AsyncCodex

        from airflow.providers.common.ai.tools.openai_codex import AirflowTools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        tools = AirflowTools(SQLToolset(db_conn_id="warehouse", allowed_tables=["orders"]))
        async with tools.serve() as config, AsyncCodex(config) as codex:
            await codex.login_api_key(api_key)
            thread = await codex.thread_start(**tools.build_thread_options())
            result = await tools.run(await thread.turn("..."))

    Each tool keeps the source tool's name, description and argument schema, and every
    result passes through Airflow's secret masker first. A failure the model can
    correct, such as a query naming a missing column, reaches it as an ``isError``
    tool result. A failure the model cannot fix, such as a hook raising, makes
    :meth:`run` raise :class:`~airflow.providers.common.ai.tools.ToolCallError`, so the
    task fails and Airflow retries it.

    Only the async client, ``AsyncCodex``, is supported. The MCP server runs in the same
    event loop as the tools, so a synchronous ``Codex`` call that blocks that loop's
    thread keeps the server from answering, and the call waits until Codex gives up on
    the tool.

    An instance runs one turn at a time and serves one :meth:`serve` block at a time:
    build a new ``AirflowTools`` per task. A tool call that arrives after a run has ended
    is refused until the next :meth:`run` starts. A call that lands after the next run has
    started cannot be told apart and is served by that run.

    :param sources: Toolsets and tools to serve, in order.
    :param server_name: The MCP server name the tools are served under. Letters, digits,
        ``_`` and ``-`` only, as it becomes part of a Codex config key.
    :param tool_timeout: Seconds Codex waits for one tool call. What happens to a hook still
        running when Codex gives up is not verified: the call may be cancelled, or a hook that
        runs in a thread may run on, and its result is discarded either way.
    :raises ValueError: when ``server_name`` has other characters.
    """

    def __init__(
        self,
        *sources: ToolProvider | AirflowTool,
        server_name: str = "airflow",
        tool_timeout: float = _DEFAULT_TOOL_TIMEOUT_SEC,
    ) -> None:
        if not _SERVER_NAME_RE.fullmatch(server_name):
            raise ValueError(
                f"server_name {server_name!r} must contain only letters, digits, '_' and '-', "
                "because it is used as a Codex config key."
            )
        self.server_name = server_name
        self.tool_timeout = tool_timeout
        self._tools = collect_tools(sources)
        # ``_run`` is the run in progress (guards re-entry); ``_last_run`` is the one that most
        # recently ended (refuses late calls). It is only read while ``_run`` is None.
        self._run: _Run | None = None
        self._last_run: _Run | None = None
        self._serving: _Serving | None = None
        self._handlers = {t.name: self._build_handler(t) for t in self._tools}

    @contextlib.asynccontextmanager
    async def serve(self) -> AsyncIterator[CodexConfig]:
        """
        Serve the tools over MCP for the duration of the block and yield the ``CodexConfig`` to use.

        Creates an empty ``CODEX_HOME``, an empty working directory and an empty ``HOME`` in a
        temporary directory, starts the MCP server on a free ``127.0.0.1`` port with a
        random bearer token, and yields a ``CodexConfig`` that points ``codex app-server`` at it
        and applies the safe defaults described in
        :ref:`the OpenAI Codex guide <howto/frameworks:openai_codex>`. On exit it stops the
        server and deletes the temporary directory, so leave the block after the ``AsyncCodex``
        that uses it.

        The yielded ``CodexConfig`` holds the bearer token in ``env``, so its ``repr`` does too:
        do not log it.

        Do not use the synchronous ``Codex`` client inside the block; see the class docstring.

        :raises RuntimeError: when this instance is already serving.
        """
        if self._serving is not None:
            raise RuntimeError(
                f"{self!r} is already serving. Build a separate AirflowTools instance "
                "for a concurrent run, or leave the first serve() block before starting another."
            )
        token = secrets.token_urlsafe(32)
        base = tempfile.mkdtemp(prefix="airflow-codex-")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server: _InProcessServer | None = None
        task: asyncio.Task[None] | None = None
        try:
            codex_home, cwd, home = (f"{base}/{name}" for name in ("codex_home", "cwd", "home"))
            for path in (codex_home, cwd, home):
                os.mkdir(path)
            # Before the first await, so that a second concurrent serve() is refused.
            self._serving = _Serving(cwd=cwd)
            # Not "localhost": it may resolve to ::1 while the socket is IPv4.
            sock.bind(("127.0.0.1", 0))
            sock.listen(16)
            port = sock.getsockname()[1]
            app = _BearerTokenMiddleware(
                self._build_mcp_server().streamable_http_app(host="127.0.0.1"), token
            )
            server = _InProcessServer(
                uvicorn.Config(
                    app,
                    log_level="warning",
                    access_log=False,
                    lifespan="on",
                    timeout_graceful_shutdown=_SERVER_SHUTDOWN_TIMEOUT_SEC,
                )
            )
            task = asyncio.create_task(server.serve(sockets=[sock]))
            await self._wait_for_server(server, task)
            yield CodexConfig(
                config_overrides=self._build_config_overrides(port),
                cwd=cwd,
                # CodexConfig.env is merged over the worker's environment, so each variable that
                # points Codex at worker state has to be overridden here.
                env={
                    # Without it Codex reads ~/.codex (config.toml, AGENTS.md, skills, mcp_servers)
                    # and writes its state there.
                    "CODEX_HOME": codex_home,
                    # A CODEX_SQLITE_HOME in the worker's environment would send the state DB outside
                    # the temporary directory (observed). Whether the ``sqlite_home`` config key alone
                    # is enough is unverified, so this variable is what we rely on.
                    "CODEX_SQLITE_HOME": codex_home,
                    "HOME": home,
                    _MCP_TOKEN_ENV_VAR: token,
                },
            )
        finally:
            self._serving = None
            if server is not None and task is not None:
                server.should_exit = True
                for outcome in await asyncio.gather(task, return_exceptions=True):
                    if isinstance(outcome, Exception):
                        log.warning("The MCP server for the Codex tools ended with an error: %r", outcome)
            sock.close()
            shutil.rmtree(base, ignore_errors=True)

    def build_thread_options(self, **kwargs: Any) -> dict[str, Any]:
        """
        Build the keyword arguments for ``AsyncCodex.thread_start``, with safe defaults.

        Defaults ``approval_mode`` to ``ApprovalMode.deny_all`` and ``sandbox`` to
        ``Sandbox.read_only``, ``cwd`` to the empty directory :meth:`serve` created (so that no
        project ``.codex/config.toml`` or ``AGENTS.md`` is found), and ``ephemeral`` to ``True``.
        ``AsyncCodex`` always accepts approval requests for command execution and file changes,
        so the only way to refuse them is to make Codex never ask. A keyword you pass wins.
        The model and anything else are passed through unchanged.

        Call it inside the :meth:`serve` block.

        :raises RuntimeError: when called outside the :meth:`serve` block.
        :raises ValueError: when ``config`` already has an ``mcp_servers`` entry named
            :attr:`server_name`.
        """
        if self._serving is None:
            raise RuntimeError("build_thread_options() has to be called inside the serve() block.")
        mcp_servers = (kwargs.get("config") or {}).get("mcp_servers") or {}
        if self.server_name in mcp_servers:
            raise ValueError(
                f"config already has an MCP server named {self.server_name!r}. Pass a different "
                "server_name to AirflowTools, or drop it from config."
            )
        kwargs.setdefault("approval_mode", ApprovalMode.deny_all)
        kwargs.setdefault("sandbox", Sandbox.read_only)
        kwargs.setdefault("cwd", self._serving.cwd)
        # Observed to be accepted by 0.163.0; that it keeps the thread off disk is assumed.
        kwargs.setdefault("ephemeral", True)
        return kwargs

    async def run(self, turn: AsyncTurnHandle) -> TurnResult:
        """
        Drive ``turn``, the handle ``AsyncThread.turn()`` returns, until it completes.

        On a tool failure the model cannot fix, the turn is interrupted before the failure
        is raised: closing the stream would not stop Codex, which would keep calling tools.
        A tool call that arrives after this returns or raises is answered with an ``isError``
        "Not run" result.

        :raises RuntimeError: when this instance is already running a turn, when the stream ends
            without a ``turn/completed`` notification, or when the turn did not complete
            (it failed, or was interrupted by something other than a tool failure).
        :raises ToolCallError: when a tool failed in a way the model cannot fix.
        """
        if self._run is not None:
            raise RuntimeError(
                f"{self!r} is already running a turn. Build a separate AirflowTools instance "
                "for a concurrent run, or await the first run before starting another."
            )
        events = turn.stream()
        state = _Run()
        self._run = state
        raised = True
        try:
            items: list[ThreadItem] = []
            usage: ThreadTokenUsage | None = None
            completed: TurnCompletedNotification | None = None
            async for event in events:
                payload = event.payload
                if isinstance(payload, ItemCompletedNotification) and payload.turn_id == turn.id:
                    items.append(payload.item)
                elif isinstance(payload, ThreadTokenUsageUpdatedNotification) and payload.turn_id == turn.id:
                    usage = payload.token_usage
                elif isinstance(payload, TurnCompletedNotification) and payload.turn.id == turn.id:
                    completed = payload
                if state.failures:
                    await self._interrupt_turn(turn)
                    raise state.failures[0]
            if completed is None:
                if state.failures:
                    raise state.failures[0]
                raise RuntimeError("The Codex turn stream ended without a turn/completed notification.")
            finished = completed.turn
            if finished.status != TurnStatus.completed:
                detail = f": {_bound_detail(finished.error.message)}" if finished.error else ""
                raise RuntimeError(f"The Codex turn ended with status {finished.status.value}{detail}")
            raised = False
            return TurnResult(
                id=finished.id,
                status=finished.status,
                error=finished.error,
                started_at=finished.started_at,
                completed_at=finished.completed_at,
                duration_ms=finished.duration_ms,
                final_response=_find_final_response(items),
                items=items,
                usage=usage,
            )
        finally:
            try:
                await _close_stream(events)
            except Exception as e:
                if not raised:
                    raise
                log.warning("Closing the Codex turn stream failed: %s", e)
            finally:
                state.ended = True
                self._last_run = state
                self._run = None

    @staticmethod
    async def _interrupt_turn(turn: AsyncTurnHandle) -> None:
        try:
            # The SDK runs the interrupt in a worker thread (``to_thread``). On timeout we stop
            # waiting, but that thread is only released when ``AsyncCodex`` closes; keep the timeout,
            # or an unresponsive app-server hangs the task instead of failing it.
            await asyncio.wait_for(turn.interrupt(), _INTERRUPT_TIMEOUT_SEC)
        except Exception as e:
            log.warning("Interrupting the Codex turn after a tool failure failed: %s", e)

    @staticmethod
    async def _wait_for_server(server: uvicorn.Server, task: asyncio.Task[None]) -> None:
        deadline = time.monotonic() + _SERVER_START_TIMEOUT_SEC
        while not server.started:
            if task.done():
                raise RuntimeError("The MCP server for the Codex tools stopped while starting.") from (
                    None if task.cancelled() else task.exception()
                )
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"The MCP server for the Codex tools did not start within {_SERVER_START_TIMEOUT_SEC} seconds."
                )
            await asyncio.sleep(0.01)

    def _build_config_overrides(self, port: int) -> tuple[str, ...]:
        """
        Build the ``codex --config key=value`` overrides.

        Codex silently ignores a key it does not know, so every key is covered by a test that
        reads it back from a real ``codex app-server``. Values are TOML.
        """
        prefix = f"mcp_servers.{self.server_name}"
        largest_schema = max((len(json.dumps(t.parameters)) for t in self._tools), default=0)
        return (
            # Login credentials stay in memory instead of an auth file in CODEX_HOME.
            'cli_auth_credentials_store="ephemeral"',
            # The model must not get a shell on the worker. Whether these flags remove the tool
            # from the model's tool list is unverified without a model.
            "features.shell_tool=false",
            "features.unified_exec=false",
            # The model must not read an image file from the worker, outside the secret masker.
            "features.view_image=false",
            # Capabilities with no use for Airflow tools.
            "features.multi_agent=false",
            "features.apps=false",
            "features.plugins=false",
            "features.image_generation=false",
            # Data leaves the worker only through the Airflow tools, never through a web search.
            'web_search="disabled"',
            # If a command still ran, it would get none of the worker's environment (AIRFLOW_CONN_*).
            'shell_environment_policy.inherit="none"',
            # Keep session history, analytics, feedback, update checks and metrics out of the picture.
            'history.persistence="none"',
            "analytics.enabled=false",
            "feedback.enabled=false",
            "check_for_update_on_startup=false",
            'otel.metrics_exporter="none"',
            # See _TOOL_OUTPUT_TOKEN_LIMIT. Whether this limit applies to MCP results is unverified.
            f"tool_output_token_limit={_TOOL_OUTPUT_TOKEN_LIMIT}",
            f"{prefix}.url={_to_toml_string(f'http://127.0.0.1:{port}/mcp')}",
            # The token reaches the child through its environment, not argv.
            f"{prefix}.bearer_token_env_var={_to_toml_string(_MCP_TOKEN_ENV_VAR)}",
            # Fail the thread start when the server is unreachable instead of running with no tools.
            f"{prefix}.required=true",
            f"{prefix}.tool_timeout_sec={self.tool_timeout}",
            # The docs promise that hook calls run one at a time. Codex does not echo ``false``, so
            # whether it equals the default is unconfirmed; ``true`` is echoed.
            f"{prefix}.supports_parallel_tool_calls=false",
            f"{prefix}.tool_input_schema_max_bytes={max(_MIN_TOOL_INPUT_SCHEMA_BYTES, largest_schema)}",
        )

    @staticmethod
    def _build_tool_schema(airflow_tool: AirflowTool) -> dict[str, Any]:
        schema = copy.deepcopy(airflow_tool.parameters)
        schema.setdefault("properties", {})
        return schema

    def _build_mcp_server(self) -> Server[Any]:
        definitions = [
            mcp_types.Tool(name=t.name, description=t.description, input_schema=self._build_tool_schema(t))
            for t in self._tools
        ]

        async def list_tools(
            ctx: ServerRequestContext[Any], params: mcp_types.PaginatedRequestParams | None
        ) -> mcp_types.ListToolsResult:
            return mcp_types.ListToolsResult(tools=definitions)

        async def call_tool(
            ctx: ServerRequestContext[Any], params: mcp_types.CallToolRequestParams
        ) -> mcp_types.CallToolResult:
            handler = self._handlers.get(params.name)
            if handler is None:
                return self._build_error_result(f"Unknown tool {params.name!r}.")
            return await handler(params.arguments or {})

        return Server(self.server_name, on_list_tools=list_tools, on_call_tool=call_tool)

    @staticmethod
    def _build_error_result(text: str) -> mcp_types.CallToolResult:
        return mcp_types.CallToolResult(
            content=[mcp_types.TextContent(type="text", text=text)], is_error=True
        )

    def _build_handler(
        self, airflow_tool: AirflowTool
    ) -> Callable[[dict[str, Any]], Awaitable[mcp_types.CallToolResult]]:
        async def handle_tool_call(args: dict[str, Any]) -> mcp_types.CallToolResult:
            state = self._run if self._run is not None else self._last_run
            if state is not None and (state.failures or state.ended):
                reason = (
                    "an earlier Airflow tool call in this run failed"
                    if state.failures
                    else "the Airflow tool run has ended"
                )
                return self._build_error_result(f"Not run: {reason}.")
            # No turn: a Codex turn spans the whole agent loop, many model responses, so using it
            # would count a tool's failures across the loop as one. Without a turn, calls that run
            # at the same time count as one.
            with calling_framework("openai_codex"), tool_call_scope(run=state, turn=None):
                try:
                    result = await airflow_tool.call(args)
                except ToolCallError as e:
                    if state is None:
                        log.warning(
                            "Tool %s failed, but the Codex turn was not driven by "
                            "AirflowTools.run, so the model reads this failure instead of the "
                            "task failing: %s",
                            airflow_tool.name,
                            e,
                        )
                    else:
                        state.failures.append(e)
                    return self._build_error_result(str(e))
            return mcp_types.CallToolResult(
                content=[
                    mcp_types.TextContent(
                        type="text", text=_truncate_result_text(serialize_for_llm(result.content))
                    )
                ],
                is_error=bool(result.is_error),
            )

        return handle_tool_call
