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

.. note:: Experimental; see :ref:`howto/stability`.
"""

from __future__ import annotations

try:
    from claude_agent_sdk import (
        AssistantMessage,
        ClaudeAgentOptions,
        McpSdkServerConfig,
        Message,
        ResultMessage,
        ToolAnnotations,
        create_sdk_mcp_server,
        tool,
    )
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

import copy
import logging
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, collect_tools
from airflow.providers.common.ai.tools._from_toolset import tool_call_scope
from airflow.providers.common.ai.utils.tool_definition import serialize_for_llm
from airflow.providers.common.ai.utils.tool_metrics import calling_framework

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Awaitable, Callable

    from airflow.providers.common.ai.tools import ToolProvider

log = logging.getLogger(__name__)

__all__ = ["AirflowTools"]


_MAX_ERROR_DETAIL_CHARS = 500
_MAX_ERROR_ENTRIES = 5
# The bundled CLI clamps ``maxResultSizeChars`` to 500_000 (observed on CLI 2.1.286; other
# versions may differ), so a larger value is a no-op. With ``tools=[]`` the model has no Read
# tool, so a result over the ceiling that the CLI spills to a ``<persisted-output>`` file is lost.
_MAX_INLINE_RESULT_CHARS = 500_000
# The CLI writes each tool result to stdout twice, JSON-escaped, and the SDK fails the stream on any
# line over ``max_buffer_size`` (1 MiB by default). Escaping can multiply a result by up to ~6, so the
# buffer must sit far above ``_MAX_INLINE_RESULT_CHARS``. Sizes observed with the CLI bundled in
# claude-agent-sdk 0.2.165, not a documented guarantee.
_MAX_BUFFER_BYTES = 16 * 1024 * 1024


def _count_utf16_units(text: str) -> int:
    return len(text.encode("utf-16-le", errors="surrogatepass")) // 2


def _truncate_result_text(text: str) -> str:
    # The CLI measures the ceiling in UTF-16 code units, so a code point outside the BMP (most
    # emoji) counts twice; ``len(text)`` would let such a result through and the CLI would spill it.
    if _count_utf16_units(text) <= _MAX_INLINE_RESULT_CHARS:
        return text
    notice = (
        f"\n\n[Truncated: this result had {len(text)} characters and only the beginning is shown. "
        "Narrow the request, for example by adding filters, selecting fewer columns or lowering "
        "the LIMIT, to get a smaller result.]"
    )
    # The notice counts toward the ceiling: text kept up to the ceiling plus the notice would
    # still be spilled by the CLI.
    keep_units = _MAX_INLINE_RESULT_CHARS - _count_utf16_units(notice)
    units = text.encode("utf-16-le", errors="surrogatepass")[: keep_units * 2]
    # A cut that lands between the two halves of a surrogate pair would leave a lone high surrogate.
    if units and 0xD8 <= units[-1] <= 0xDB:
        units = units[:-2]
    return units.decode("utf-16-le", errors="surrogatepass") + notice


def _bound_detail(text: str) -> str:
    omitted = len(text) - _MAX_ERROR_DETAIL_CHARS
    return text if omitted <= 0 else f"{text[:_MAX_ERROR_DETAIL_CHARS]}...(+{omitted} chars)"


def _describe_error_result(result: ResultMessage) -> str:
    errors = result.errors or []
    omitted_errors = len(errors) - _MAX_ERROR_ENTRIES
    fields: dict[str, Any] = {
        "subtype": result.subtype,
        "result": result.result,
        "errors": errors[:_MAX_ERROR_ENTRIES],
        "stop_reason": result.stop_reason,
        "api_error_status": result.api_error_status,
        "terminal_reason": result.terminal_reason,
    }
    details = []
    for name, value in fields.items():
        if not value:
            continue
        text = _bound_detail(repr(value)) if name in ("result", "errors") else repr(value)
        if name == "errors" and omitted_errors > 0:
            text = f"{text} (+{omitted_errors} more)"
        details.append(f"{name}={text}")
    return f"The Claude Agent SDK run ended with an error result ({', '.join(details)})."


async def _close_stream(messages: AsyncIterable[Message]) -> None:
    # ``AsyncIterable`` does not guarantee ``aclose``; ``query()`` and ``receive_response()`` do.
    aclose = getattr(messages, "aclose", None)
    if aclose is not None:
        await aclose()


class _Run:
    """
    One run of :meth:`AirflowTools.run`.

    Identifies its calls, collects their failures, and records whether it has ended.
    """

    def __init__(self) -> None:
        self.failures: list[ToolCallError] = []
        self.turn: str | None = None
        self.ended = False


class AirflowTools:
    """
    Airflow's tools for an agent loop run by the Claude Agent SDK.

    Pass toolsets that implement
    :class:`~airflow.providers.common.ai.tools.ToolProvider`, such as ``SQLToolset``
    and ``HookToolset``, or individual
    :class:`~airflow.providers.common.ai.tools.AirflowTool` objects. They are served
    through an in-process MCP server; add it and :attr:`allowed_tools` to
    ``ClaudeAgentOptions``, or build them with :meth:`build_options`, and drive the SDK's
    ``query()`` with :meth:`run`:

    .. code-block:: python

        from claude_agent_sdk import query

        from airflow.providers.common.ai.tools.claude_agent_sdk import AirflowTools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        tools = AirflowTools(SQLToolset(db_conn_id="warehouse", allowed_tables=["orders"]))
        options = tools.build_options(model="claude-sonnet-5", max_turns=10)
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
    call :meth:`run` again once the previous run has finished. A tool call that arrives
    after a run has ended is refused until the next :meth:`run` starts. A call that lands
    after the next run has started cannot be told apart and is served by that run.

    :param sources: Toolsets and tools to serve, in order.
    :param server_name: The MCP server name the tools are served under. Also the
        prefix of each tool's name in :attr:`allowed_tools`
        (``mcp__<server_name>__<tool name>``).
    """

    def __init__(self, *sources: ToolProvider | AirflowTool, server_name: str = "airflow") -> None:
        self.server_name = server_name
        airflow_tools = collect_tools(sources)
        self.allowed_tools: list[str] = [f"mcp__{server_name}__{t.name}" for t in airflow_tools]
        # ``_run`` is the run in progress (guards re-entry); ``_last_run`` is the one that most
        # recently ended (refuses late calls). It is only read while ``_run`` is None.
        self._run: _Run | None = None
        self._last_run: _Run | None = None
        annotations = ToolAnnotations(maxResultSizeChars=_MAX_INLINE_RESULT_CHARS)
        sdk_tools = [
            tool(t.name, t.description, self._build_tool_schema(t), annotations=annotations)(
                self._build_handler(t)
            )
            for t in airflow_tools
        ]
        self.server: McpSdkServerConfig = create_sdk_mcp_server(name=server_name, tools=sdk_tools)

    def build_options(self, **kwargs: Any) -> ClaudeAgentOptions:
        """
        Build ``ClaudeAgentOptions`` wired to this instance's tools, with safe defaults.

        ``ClaudeAgentOptions()`` on its own loads every settings source on the worker
        (``~/.claude``, project settings, a project ``.mcp.json``) and leaves the CLI's
        built-in tools on, including a worker-host ``Bash``. This method instead
        defaults ``tools`` to ``[]`` (no built-in tools), ``setting_sources`` to ``[]``
        (no settings files) and ``strict_mcp_config`` to ``True`` (only the MCP servers
        passed here), and ``verbatim_prompts`` to ``True``. It adds :attr:`server` and
        :attr:`allowed_tools` to whatever ``mcp_servers`` and ``allowed_tools`` the caller
        passes. A keyword this method does not default, such as ``model`` or ``env``, is
        passed through unchanged.

        ``verbatim_prompts`` stops the CLI from expanding ``@path`` file mentions in a prompt;
        otherwise anyone who controls the prompt could send a file the worker user can read to
        the model, without passing the secret masker. It also turns off slash command dispatch
        and the CLI's turn-start attachment pass.

        When ``can_use_tool`` is passed, the Airflow tools are not added to ``allowed_tools``,
        because an allowed tool never reaches the callback, and ``permission_mode`` defaults to
        ``"default"``. The callback then decides on every Airflow tool call; the names are still
        in :attr:`allowed_tools` if you want to allow some by hand.

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
        allowed_tools = list(kwargs.pop("allowed_tools", []))
        if kwargs.get("can_use_tool") is None:
            allowed_tools.extend(self.allowed_tools)
        else:
            kwargs.setdefault("permission_mode", "default")
        kwargs.setdefault("tools", [])
        kwargs.setdefault("setting_sources", [])
        kwargs.setdefault("strict_mcp_config", True)
        kwargs.setdefault("verbatim_prompts", True)
        kwargs.setdefault("max_buffer_size", _MAX_BUFFER_BYTES)
        return ClaudeAgentOptions(mcp_servers=mcp_servers, allowed_tools=allowed_tools, **kwargs)

    async def run(self, messages: AsyncIterable[Message]) -> ResultMessage:
        """
        Drive ``messages``, the stream ``query()`` or ``ClaudeSDKClient.receive_response()`` returns.

        ``ClaudeSDKClient.receive_messages()`` is not supported: it never ends.

        ``messages`` is closed when this returns or raises. For ``query()`` that ends the CLI
        session, but only once the SDK finishes tearing down, so a tool call can still arrive
        after this returns or raises; it is answered with an ``is_error`` "Not run" result.
        A ``ClaudeSDKClient`` stays connected until you disconnect it, for example by leaving
        its ``async with`` block.

        :raises RuntimeError: when this instance is already running a query, when
            ``messages`` ends without a :class:`~claude_agent_sdk.ResultMessage`, or, for
            ``ClaudeSDKClient.receive_response()``, when the result message has ``is_error``
            set (for example ``max_turns`` reached or an API failure).
            For a streamed prompt, any result message with ``is_error`` set raises, even when a
            later turn succeeds.
        :raises ResultError: for ``query()`` when the result message has ``is_error`` set.
            The CLI exits non-zero right after such a result, so the SDK raises
            ``claude_agent_sdk.ResultError`` (a ``ProcessError``) from the stream before this
            method checks the result.
        :raises ToolCallError: when a tool failed in a way the model cannot fix.
        """
        if self._run is not None:
            raise RuntimeError(
                f"{self!r} is already running a query. Build a separate AirflowTools instance "
                "for a concurrent run, or await the first run before starting another."
            )
        state = _Run()
        self._run = state
        raised = True
        try:
            result: ResultMessage | None = None
            # Each turn yields its own ResultMessage; keep the first error so a later success
            # cannot mask it.
            error_result: ResultMessage | None = None
            async for message in messages:
                if isinstance(message, AssistantMessage):
                    # The CLI sends each turn's message before running that turn's tools, which is
                    # what ``handle_tool_call`` relies on. Observed with the bundled CLI, not documented.
                    state.turn = message.message_id
                if isinstance(message, ResultMessage):
                    result = message
                    if message.is_error and error_result is None:
                        error_result = message
                if state.failures:
                    raise state.failures[0]
            if result is None:
                if state.failures:
                    raise state.failures[0]
                raise RuntimeError("The Claude Agent SDK stream ended without a ResultMessage.")
            if error_result is not None:
                raise RuntimeError(_describe_error_result(error_result))
            raised = False
            return result
        finally:
            try:
                await _close_stream(messages)
            except Exception as e:
                if not raised:
                    raise
                log.warning("Closing the Claude Agent SDK stream failed: %s", e)
            finally:
                state.ended = True
                self._last_run = state
                self._run = None

    @staticmethod
    def _build_tool_schema(airflow_tool: AirflowTool) -> dict[str, Any]:
        schema = copy.deepcopy(airflow_tool.parameters)
        schema.setdefault("properties", {})
        return schema

    def _build_handler(
        self, airflow_tool: AirflowTool
    ) -> Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]:
        async def handle_tool_call(args: dict[str, Any]) -> dict[str, Any]:
            state = self._run if self._run is not None else self._last_run
            if state is not None and (state.failures or state.ended):
                reason = (
                    "an earlier Airflow tool call in this run failed"
                    if state.failures
                    else "the Airflow tool run has ended"
                )
                return {
                    "content": [{"type": "text", "text": f"Not run: {reason}."}],
                    "is_error": True,
                }
            with (
                calling_framework("claude_agent_sdk"),
                tool_call_scope(run=state, turn=state.turn if state is not None else None),
            ):
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
                "content": [
                    {"type": "text", "text": _truncate_result_text(serialize_for_llm(result.content))}
                ]
            }
            if result.is_error:
                payload["is_error"] = True
            return payload

        return handle_tool_call
