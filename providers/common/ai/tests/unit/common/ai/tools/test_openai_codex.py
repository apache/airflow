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

import asyncio
import dataclasses
import importlib
import json
import os
import signal
import sys
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

pytest.importorskip("openai_codex")
pytest.importorskip("mcp")

import httpx2
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from openai_codex import (
    ApprovalMode,
    AsyncCodex,
    AsyncTurnHandle,
    CodexConfig,
    InternalRpcError,
    Sandbox,
    TurnResult,
)
from openai_codex.client import CodexClient
from openai_codex.generated.v2_all import (
    AgentMessageThreadItem,
    AskForApprovalValue,
    ConfigReadResponse,
    ItemCompletedNotification,
    ListMcpServerStatusResponse,
    MessagePhase,
    ThreadItem,
    ThreadStartResponse,
    ThreadTokenUsage,
    ThreadTokenUsageUpdatedNotification,
    TokenUsageBreakdown,
    Turn,
    TurnCompletedNotification,
    TurnError,
    TurnStatus,
)
from openai_codex.models import Notification
from pydantic_ai.exceptions import ModelRetry

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult
from airflow.providers.common.ai.tools.openai_codex import (
    _MAX_INLINE_RESULT_CHARS,
    AirflowTools,
    _BearerTokenMiddleware,
)
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.ai.utils.tool_metrics import current_framework
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

from unit.common.ai.tools.test__from_toolset import _scripted
from unit.common.ai.toolsets.test_sql import _make_mock_db_hook

MODULE = "airflow.providers.common.ai.tools.openai_codex"
TURN_ID = "turn-1"
THREAD_ID = "thread-1"


def _tool(outcome: ToolResult | Exception, ran: list[dict[str, Any]] | None = None) -> AirflowTool:
    """A ``lookup`` tool that returns or raises ``outcome``, recording its calls in ``ran``."""

    async def function(arguments: dict[str, Any]) -> ToolResult:
        if ran is not None:
            ran.append(arguments)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return AirflowTool(
        name="lookup",
        description="Look something up.",
        parameters={"type": "object", "properties": {"key": {"type": "string"}}, "required": ["key"]},
        function=function,
    )


def _text(result: Any) -> str:
    return result.content[0].text


def _agent_message(text: str, phase: MessagePhase | None = MessagePhase.final_answer) -> ThreadItem:
    return ThreadItem(root=AgentMessageThreadItem(id="item", text=text, phase=phase, type="agentMessage"))


def _item_completed(item: ThreadItem, turn_id: str = TURN_ID) -> Notification:
    return Notification(
        method="item/completed",
        payload=ItemCompletedNotification(completed_at_ms=1, item=item, thread_id=THREAD_ID, turn_id=turn_id),
    )


def _token_usage() -> Notification:
    breakdown = TokenUsageBreakdown(
        cached_input_tokens=0, input_tokens=3, output_tokens=4, reasoning_output_tokens=0, total_tokens=7
    )
    return Notification(
        method="thread/tokenUsage/updated",
        payload=ThreadTokenUsageUpdatedNotification(
            thread_id=THREAD_ID,
            turn_id=TURN_ID,
            token_usage=ThreadTokenUsage(last=breakdown, total=breakdown),
        ),
    )


def _turn_completed(
    status: TurnStatus = TurnStatus.completed, error_message: str | None = None, turn_id: str = TURN_ID
) -> Notification:
    return Notification(
        method="turn/completed",
        payload=TurnCompletedNotification(
            thread_id=THREAD_ID,
            turn=Turn(
                id=turn_id,
                items=[],
                status=status,
                error=TurnError(message=error_message) if error_message is not None else None,
                started_at=10,
                completed_at=12,
                duration_ms=2000,
            ),
        ),
    )


def _build_turn(stream: AsyncIterator[Notification]) -> Any:
    """An ``AsyncTurnHandle`` whose ``stream()`` is ``stream`` and whose ``interrupt`` is an ``AsyncMock``."""
    turn = mock.create_autospec(AsyncTurnHandle, instance=True)
    turn.id = TURN_ID
    turn.stream.side_effect = lambda: stream
    return turn


async def _fake_codex(
    handlers: dict[str, Any],
    steps: list[tuple[str, dict[str, Any]]],
    *,
    final: Notification | None = None,
    closed: list[bool] | None = None,
) -> AsyncIterator[Notification]:
    """
    Stand in for ``codex app-server`` driving the tools ``AirflowTools`` served.

    Calls each step's tool the way the model would, one after the other, with an
    agent message after each, then ends the turn with ``final`` (``completed`` by default).
    """
    try:
        for tool_name, args in steps:
            await handlers[tool_name](args)
            yield _item_completed(_agent_message("step", MessagePhase.commentary))
        yield _turn_completed() if final is None else final
    finally:
        if closed is not None:
            closed.append(True)


async def _serve_and_leave(tools: AirflowTools) -> None:
    async with tools.serve():
        pass


def _real_path(*parts: str) -> str:
    return os.path.realpath(os.path.join(*parts))


def _exists(path: str) -> bool:
    return Path(path).exists()


def _list_dir(path: str | Path) -> list[Path]:
    return list(Path(path).iterdir())


def _parse_overrides(config: CodexConfig) -> dict[str, str]:
    return dict(override.partition("=")[::2] for override in config.config_overrides)


def _server_url(config: CodexConfig, server_name: str = "airflow") -> str:
    return json.loads(_parse_overrides(config)[f"mcp_servers.{server_name}.url"])


def _build_headers(config: CodexConfig) -> dict[str, str]:
    assert config.env is not None
    return {"Authorization": f"Bearer {config.env['AIRFLOW_CODEX_MCP_TOKEN']}"}


async def _call_over_http(
    config: CodexConfig, name: str, args: dict[str, Any], headers: dict[str, str] | None = None
) -> Any:
    """Call a tool the way Codex does: over the HTTP connection to the served MCP server."""
    http = httpx2.AsyncClient(headers=_build_headers(config) if headers is None else headers)
    async with (
        http,
        streamable_http_client(_server_url(config), http_client=http) as (read, write, *_),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        return await session.call_tool(name, args)


class TestTools:
    @pytest.mark.asyncio
    async def test_each_toolset_tool_becomes_an_mcp_tool_with_its_schema(self):
        ts = SQLToolset("pg_default")
        tools = AirflowTools(ts)

        async with tools.serve() as config:
            http = httpx2.AsyncClient(headers=_build_headers(config))
            async with (
                http,
                streamable_http_client(_server_url(config), http_client=http) as (read, write, *_),
                ClientSession(read, write) as session,
            ):
                await session.initialize()
                listed = (await session.list_tools()).tools

        expected = {t.name: t for t in ts.airflow_tools()}
        assert {t.name for t in listed} == expected.keys()
        for listed_tool in listed:
            assert listed_tool.description == expected[listed_tool.name].description
            assert listed_tool.input_schema["properties"] == expected[listed_tool.name].parameters.get(
                "properties", {}
            )

    @pytest.mark.asyncio
    async def test_a_schema_without_properties_takes_no_arguments(self):
        no_arguments = AirflowTool(
            name="ping",
            description="Ping.",
            parameters={"type": "object"},
            function=mock.AsyncMock(return_value=ToolResult("pong")),
        )
        tools = AirflowTools(no_arguments)

        async with tools.serve() as config:
            result = await _call_over_http(config, "ping", {})

        assert _text(result) == "pong"

    @pytest.mark.asyncio
    async def test_an_mcp_tool_does_not_share_the_source_schema(self):
        source = _tool(ToolResult("ok"))
        tools = AirflowTools(source)

        async with tools.serve() as config:
            source.parameters["properties"]["key"]["type"] = "integer"
            http = httpx2.AsyncClient(headers=_build_headers(config))
            async with (
                http,
                streamable_http_client(_server_url(config), http_client=http) as (read, write, *_),
                ClientSession(read, write) as session,
            ):
                await session.initialize()
                listed = (await session.list_tools()).tools[0]

        assert listed.input_schema["properties"]["key"]["type"] == "string"


class TestHandler:
    @pytest.mark.asyncio
    async def test_the_model_reads_a_toolset_result(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Ada")], last_description=[("id",), ("name",)])
        tools = AirflowTools(ts)

        result = await tools._handlers["query"]({"sql": "SELECT id, name FROM users"})

        assert result.is_error is False
        assert json.loads(_text(result))["rows"] == [[1, "Ada"]]

    @pytest.mark.asyncio
    async def test_a_correctable_failure_reaches_the_model_as_an_error(self):
        tools = AirflowTools(_tool(ToolResult("Unknown column 'nme'", is_error=True)))

        result = await tools._handlers["lookup"]({"key": "a"})

        assert result.is_error is True
        assert _text(result) == "Unknown column 'nme'"

    @pytest.mark.asyncio
    async def test_a_structured_result_reaches_the_model_as_json(self):
        tools = AirflowTools(_tool(ToolResult({"rows": [[1, None]]})))

        result = await tools._handlers["lookup"]({"key": "a"})

        assert json.loads(_text(result)) == {"rows": [[1, None]]}

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.log", autospec=True)
    async def test_not_driven_by_run_hands_the_failure_to_the_model_with_a_warning(self, mock_log):
        tools = AirflowTools(_tool(PermissionError("role cannot read orders")))

        result = await tools._handlers["lookup"]({"key": "a"})

        assert result.is_error is True
        assert "PermissionError: role cannot read orders" in _text(result)
        mock_log.warning.assert_called_once()
        assert "not driven by AirflowTools.run" in mock_log.warning.call_args.args[0]

    @pytest.mark.asyncio
    async def test_the_tool_runs_as_openai_codex(self):
        seen: list[str | None] = []

        async def function(arguments: dict[str, Any]) -> ToolResult:
            seen.append(current_framework())
            return ToolResult("ok")

        tools = AirflowTools(
            AirflowTool(name="probe", description="Probe.", parameters={"type": "object"}, function=function)
        )

        await tools._handlers["probe"]({})

        assert seen == ["openai_codex"]

    @pytest.mark.asyncio
    async def test_the_toolsets_calls_are_counted_as_openai_codex(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        tools = AirflowTools(ts)

        with mock.patch(
            "airflow.providers.common.ai.utils.tool_metrics.Stats", mock.MagicMock(spec=["incr"])
        ) as stats:
            await tools._handlers["list_tables"]({})

        assert stats.incr.call_args.kwargs["tags"]["framework"] == "openai_codex"

    @pytest.mark.enable_redact
    @pytest.mark.asyncio
    async def test_a_secret_in_a_plain_string_result_is_masked(self, registered_secret):
        tools = AirflowTools(_tool(ToolResult(f"key={registered_secret}")))

        result = await tools._handlers["lookup"]({"key": "a"})

        assert registered_secret not in _text(result)
        assert _text(result) == "key=***"

    @pytest.mark.enable_redact
    @pytest.mark.asyncio
    async def test_a_secret_in_a_result_over_http_is_masked(self, registered_secret):
        tools = AirflowTools(_tool(ToolResult(f"key={registered_secret}")))

        async with tools.serve() as config:
            result = await _call_over_http(config, "lookup", {"key": "a"})

        assert _text(result) == "key=***"

    @pytest.mark.asyncio
    async def test_a_result_over_the_ceiling_is_cut_to_fit_with_a_notice(self):
        tools = AirflowTools(_tool(ToolResult("x" * (_MAX_INLINE_RESULT_CHARS + 1))))

        result = await tools._handlers["lookup"]({"key": "a"})

        text = _text(result)
        assert len(text) == _MAX_INLINE_RESULT_CHARS
        assert text.endswith("to get a smaller result.]")
        assert f"this result had {_MAX_INLINE_RESULT_CHARS + 1} characters" in text

    @pytest.mark.asyncio
    async def test_a_result_at_the_ceiling_is_returned_as_is(self):
        tools = AirflowTools(_tool(ToolResult("x" * _MAX_INLINE_RESULT_CHARS)))

        result = await tools._handlers["lookup"]({"key": "a"})

        assert _text(result) == "x" * _MAX_INLINE_RESULT_CHARS


class TestRun:
    @pytest.mark.asyncio
    async def test_run_returns_the_turn_result(self):
        tools = AirflowTools(_tool(ToolResult("ok")))
        stream = _async_iter(
            [
                _item_completed(_agent_message("thinking", MessagePhase.commentary)),
                _item_completed(_agent_message("42")),
                _token_usage(),
                _item_completed(_agent_message("ignored"), turn_id="another-turn"),
                _turn_completed(),
            ]
        )

        result = await tools.run(_build_turn(stream))

        assert isinstance(result, TurnResult)
        assert result.id == TURN_ID
        assert result.status == TurnStatus.completed
        assert result.final_response == "42"
        assert len(result.items) == 2
        assert result.usage is not None
        assert result.usage.total.total_tokens == 7
        assert (result.started_at, result.completed_at, result.duration_ms) == (10, 12, 2000)

    @pytest.mark.asyncio
    async def test_the_final_response_falls_back_to_the_last_message_without_a_phase(self):
        tools = AirflowTools(_tool(ToolResult("ok")))
        stream = _async_iter(
            [
                _item_completed(_agent_message("first", None)),
                _item_completed(_agent_message("last", None)),
                _turn_completed(),
            ]
        )

        result = await tools.run(_build_turn(stream))

        assert result.final_response == "last"

    @pytest.mark.asyncio
    async def test_a_turn_that_made_no_final_response_has_none(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        result = await tools.run(_build_turn(_async_iter([_turn_completed()])))

        assert result.final_response is None

    @pytest.mark.asyncio
    async def test_a_failed_turn_raises_with_the_codex_error(self):
        tools = AirflowTools(_tool(ToolResult("ok")))
        stream = _async_iter([_turn_completed(TurnStatus.failed, "usage limit reached")])

        with pytest.raises(RuntimeError, match="status failed: usage limit reached"):
            await tools.run(_build_turn(stream))

    @pytest.mark.asyncio
    async def test_a_failed_turn_bounds_the_error_message(self):
        tools = AirflowTools(_tool(ToolResult("ok")))
        stream = _async_iter([_turn_completed(TurnStatus.failed, "e" * 600)])

        with pytest.raises(RuntimeError) as raised:
            await tools.run(_build_turn(stream))

        assert str(raised.value).endswith("e" * 500 + "...(+100 chars)")

    @pytest.mark.asyncio
    async def test_a_turn_interrupted_from_outside_raises(self):
        tools = AirflowTools(_tool(ToolResult("ok")))
        stream = _async_iter([_turn_completed(TurnStatus.interrupted)])

        with pytest.raises(RuntimeError, match="status interrupted"):
            await tools.run(_build_turn(stream))

    @pytest.mark.asyncio
    async def test_a_stream_without_turn_completed_raises(self):
        tools = AirflowTools(_tool(ToolResult("ok")))
        stream = _async_iter([_item_completed(_agent_message("42"))])

        with pytest.raises(RuntimeError, match="without a turn/completed notification"):
            await tools.run(_build_turn(stream))

    @pytest.mark.asyncio
    async def test_a_tool_failure_interrupts_the_turn_and_raises(self):
        ran: list[dict[str, Any]] = []
        tools = AirflowTools(_tool(PermissionError("role cannot read orders"), ran))
        turn = _build_turn(_fake_codex(tools._handlers, [("lookup", {"key": "a"}), ("lookup", {"key": "b"})]))

        with pytest.raises(ToolCallError, match="PermissionError"):
            await tools.run(turn)

        turn.interrupt.assert_awaited_once_with()
        assert ran == [{"key": "a"}]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.log", autospec=True)
    async def test_a_failing_interrupt_does_not_hide_the_tools_failure(self, mock_log):
        tools = AirflowTools(_tool(PermissionError("role cannot read orders")))
        turn = _build_turn(_fake_codex(tools._handlers, [("lookup", {"key": "a"})]))
        turn.interrupt.side_effect = OSError("app-server is gone")

        with pytest.raises(ToolCallError, match="PermissionError"):
            await tools.run(turn)

        mock_log.warning.assert_called_once()
        assert "app-server is gone" in str(mock_log.warning.call_args.args)

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}._INTERRUPT_TIMEOUT_SEC", 0.05)
    @mock.patch(f"{MODULE}.log", autospec=True)
    async def test_an_interrupt_that_never_returns_does_not_hang_the_failure(self, mock_log):
        tools = AirflowTools(_tool(PermissionError("role cannot read orders")))
        turn = _build_turn(_fake_codex(tools._handlers, [("lookup", {"key": "a"})]))
        never = asyncio.Event()
        turn.interrupt.side_effect = never.wait

        with pytest.raises(ToolCallError, match="PermissionError"):
            await asyncio.wait_for(tools.run(turn), 5)

        mock_log.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_failure_before_the_stream_ends_without_turn_completed_still_raises_it(self):
        tools = AirflowTools(_tool(PermissionError("role cannot read orders")))

        async def _call_then_end() -> AsyncIterator[Notification]:
            await tools._handlers["lookup"]({"key": "a"})
            return
            yield

        turn = _build_turn(_call_then_end())

        with pytest.raises(ToolCallError, match="PermissionError"):
            await tools.run(turn)

        turn.interrupt.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_tool_failure_takes_priority_over_a_failed_turn(self):
        tools = AirflowTools(_tool(PermissionError("role cannot read orders")))
        turn = _build_turn(
            _fake_codex(
                tools._handlers,
                [("lookup", {"key": "a"})],
                final=_turn_completed(TurnStatus.failed, "interrupted"),
            )
        )

        with pytest.raises(ToolCallError):
            await tools.run(turn)

    @pytest.mark.asyncio
    async def test_calls_after_a_failure_do_not_run(self):
        ran: list[dict[str, Any]] = []
        tools = AirflowTools(_tool(PermissionError("role cannot read orders"), ran))

        async def _three_calls() -> AsyncIterator[Notification]:
            for _ in range(3):
                await tools._handlers["lookup"]({"key": "a"})
            yield _turn_completed()

        with pytest.raises(ToolCallError, match="PermissionError"):
            await tools.run(_build_turn(_three_calls()))

        assert len(ran) == 1

    @pytest.mark.asyncio
    async def test_a_tool_that_keeps_failing_fails_the_run_once_its_retries_are_spent(self):
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)
        tools = AirflowTools(step)

        with pytest.raises(ToolCallError, match="kept failing after 1 correction"):
            await tools.run(_build_turn(_fake_codex(tools._handlers, [("step", {}), ("step", {})])))

    @pytest.mark.asyncio
    async def test_each_run_of_a_reused_instance_starts_with_a_fresh_retry_budget(self):
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)
        tools = AirflowTools(step)

        for _ in range(2):
            result = await tools.run(_build_turn(_fake_codex(tools._handlers, [("step", {})])))
            assert result.status == TurnStatus.completed

    @pytest.mark.asyncio
    async def test_simultaneous_failures_count_as_one(self):
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)
        tools = AirflowTools(step)

        async def _two_calls_at_once() -> AsyncIterator[Notification]:
            await asyncio.gather(tools._handlers["step"]({}), tools._handlers["step"]({}))
            yield _turn_completed()

        result = await tools.run(_build_turn(_two_calls_at_once()))

        assert result.status == TurnStatus.completed

    @pytest.mark.asyncio
    async def test_the_stream_is_closed_when_the_run_ends(self):
        closed: list[bool] = []
        tools = AirflowTools(_tool(ToolResult("ok")))

        await tools.run(_build_turn(_fake_codex(tools._handlers, [("lookup", {"key": "a"})], closed=closed)))

        assert closed == [True]

    @pytest.mark.asyncio
    async def test_the_stream_is_closed_when_the_run_raises(self):
        closed: list[bool] = []
        tools = AirflowTools(_tool(PermissionError("role cannot read orders")))
        turn = _build_turn(_fake_codex(tools._handlers, [("lookup", {"key": "a"})], closed=closed))

        with pytest.raises(ToolCallError):
            await tools.run(turn)

        assert closed == [True]

    @pytest.mark.asyncio
    async def test_a_call_after_a_successful_run_has_ended_does_not_run(self):
        ran: list[dict[str, Any]] = []
        tools = AirflowTools(_tool(ToolResult("ok"), ran))
        await tools.run(_build_turn(_fake_codex(tools._handlers, [("lookup", {"key": "a"})])))
        assert len(ran) == 1

        result = await tools._handlers["lookup"]({"key": "b"})

        assert result.is_error is True
        assert _text(result) == "Not run: the Airflow tool run has ended."
        assert len(ran) == 1

    @pytest.mark.asyncio
    async def test_a_call_after_a_failed_run_has_ended_does_not_run(self):
        ran: list[dict[str, Any]] = []
        tools = AirflowTools(_tool(PermissionError("role cannot read orders"), ran))
        with pytest.raises(ToolCallError):
            await tools.run(_build_turn(_fake_codex(tools._handlers, [("lookup", {"key": "a"})])))

        result = await tools._handlers["lookup"]({"key": "b"})

        assert result.is_error is True
        assert _text(result) == "Not run: an earlier Airflow tool call in this run failed."
        assert len(ran) == 1

    @pytest.mark.asyncio
    async def test_a_new_run_is_not_blocked_by_the_previous_ended_run(self):
        ran: list[dict[str, Any]] = []
        tools = AirflowTools(_tool(ToolResult("ok"), ran))
        await tools.run(_build_turn(_fake_codex(tools._handlers, [("lookup", {"key": "a"})])))

        await tools.run(_build_turn(_fake_codex(tools._handlers, [("lookup", {"key": "b"})])))

        assert ran == [{"key": "a"}, {"key": "b"}]

    @pytest.mark.asyncio
    async def test_concurrent_runs_on_one_instance_are_rejected(self):
        tools = AirflowTools(_tool(ToolResult("ok")))
        gate = asyncio.Event()

        async def _blocked() -> AsyncIterator[Notification]:
            await gate.wait()
            yield _turn_completed()

        first = asyncio.ensure_future(tools.run(_build_turn(_blocked())))
        await asyncio.sleep(0)

        with pytest.raises(RuntimeError, match="already running"):
            await tools.run(_build_turn(_async_iter([_turn_completed()])))

        gate.set()
        await first

    @pytest.mark.asyncio
    async def test_a_tool_failure_over_http_fails_the_run(self):
        tools = AirflowTools(_tool(PermissionError("role cannot read orders")))

        async with tools.serve() as config:

            async def _call_over_the_wire() -> AsyncIterator[Notification]:
                result = await _call_over_http(config, "lookup", {"key": "a"})
                assert result.is_error is True
                yield _turn_completed()

            with pytest.raises(ToolCallError, match="PermissionError"):
                await tools.run(_build_turn(_call_over_the_wire()))


async def _async_iter(notifications: list[Notification]) -> AsyncIterator[Notification]:
    for notification in notifications:
        yield notification


class TestBearerTokenMiddleware:
    @staticmethod
    async def _call(scope_type: str, headers: list[tuple[bytes, bytes]]) -> tuple[list[dict], list[str]]:
        sent: list[dict[str, Any]] = []
        forwarded: list[str] = []

        async def app(scope, receive, send):
            forwarded.append(scope["type"])

        async def send(message):
            sent.append(message)

        middleware = _BearerTokenMiddleware(app, "secret-token")
        await middleware({"type": scope_type, "headers": headers}, None, send)
        return sent, forwarded

    @pytest.mark.asyncio
    async def test_lifespan_passes_through_without_a_token(self):
        sent, forwarded = await self._call("lifespan", [])

        assert forwarded == ["lifespan"]

    @pytest.mark.asyncio
    async def test_a_websocket_without_the_token_is_closed_and_not_forwarded(self):
        sent, forwarded = await self._call("websocket", [])

        assert forwarded == []
        assert sent == [{"type": "websocket.close", "code": 1008}]

    @pytest.mark.asyncio
    async def test_an_unknown_scope_type_without_the_token_is_not_forwarded(self):
        sent, forwarded = await self._call("something-new", [])

        assert forwarded == []

    @pytest.mark.asyncio
    async def test_a_websocket_with_the_token_is_forwarded(self):
        sent, forwarded = await self._call("websocket", [(b"authorization", b"Bearer secret-token")])

        assert forwarded == ["websocket"]


class TestRealMcpServer:
    @pytest.mark.asyncio
    async def test_a_tool_result_marked_as_an_error_reaches_the_model_as_isError(self):
        tools = AirflowTools(_tool(ToolResult("Unknown column 'nme'", is_error=True)))

        async with tools.serve() as config:
            result = await _call_over_http(config, "lookup", {"key": "a"})

        assert result.is_error is True
        assert _text(result) == "Unknown column 'nme'"

    @pytest.mark.asyncio
    async def test_a_successful_tool_result_is_not_marked_isError(self):
        tools = AirflowTools(_tool(ToolResult("found")))

        async with tools.serve() as config:
            result = await _call_over_http(config, "lookup", {"key": "a"})

        assert result.is_error is False
        assert _text(result) == "found"

    @pytest.mark.asyncio
    async def test_an_unknown_tool_is_an_error_result(self):
        tools = AirflowTools(_tool(ToolResult("found")))

        async with tools.serve() as config:
            result = await _call_over_http(config, "missing", {})

        assert result.is_error is True

    @pytest.mark.asyncio
    async def test_a_request_without_the_token_is_refused(self):
        ran: list[dict[str, Any]] = []
        tools = AirflowTools(_tool(ToolResult("found"), ran))

        async with tools.serve() as config:
            async with httpx2.AsyncClient() as http:
                response = await http.post(_server_url(config), json={})

        assert response.status_code == 401
        assert ran == []

    @pytest.mark.asyncio
    async def test_a_request_with_a_wrong_token_is_refused(self):
        ran: list[dict[str, Any]] = []
        tools = AirflowTools(_tool(ToolResult("found"), ran))

        async with tools.serve() as config:
            async with httpx2.AsyncClient(headers={"Authorization": "Bearer nope"}) as http:
                response = await http.post(_server_url(config), json={})

        assert response.status_code == 401
        assert ran == []

    @pytest.mark.asyncio
    async def test_each_serve_block_gets_its_own_token(self):
        tools = AirflowTools(_tool(ToolResult("found")))

        async with tools.serve() as first:
            first_url, first_token = _server_url(first), first.env["AIRFLOW_CODEX_MCP_TOKEN"]
        async with tools.serve() as second:
            assert _server_url(second).startswith("http://127.0.0.1:")
            assert second.env["AIRFLOW_CODEX_MCP_TOKEN"] != first_token
            assert len(first_token) >= 32
        assert first_url.startswith("http://127.0.0.1:")

    @pytest.mark.asyncio
    async def test_leaving_the_block_stops_the_server_and_deletes_the_temporary_directory(self):
        tools = AirflowTools(_tool(ToolResult("found")))

        async with tools.serve() as config:
            temporary = Path(config.env["CODEX_HOME"]).parent
            assert temporary.is_dir()

        assert not temporary.exists()
        with pytest.raises(httpx2.ConnectError):
            async with httpx2.AsyncClient() as http:
                await http.post(_server_url(config), json={})

    @pytest.mark.asyncio
    async def test_an_exception_in_the_block_still_cleans_up_and_allows_serving_again(self):
        tools = AirflowTools(_tool(ToolResult("found")))

        temporaries: list[Path] = []

        async def _fail_inside_the_block() -> None:
            async with tools.serve() as config:
                temporaries.append(Path(config.env["CODEX_HOME"]).parent)
                raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await _fail_inside_the_block()

        assert not temporaries[0].exists()
        async with tools.serve():
            pass

    @pytest.mark.asyncio
    async def test_serving_twice_at_once_is_rejected(self):
        tools = AirflowTools(_tool(ToolResult("found")))

        async with tools.serve():
            with pytest.raises(RuntimeError, match="already serving"):
                await _serve_and_leave(tools)

    @pytest.mark.asyncio
    async def test_two_concurrent_serve_blocks_are_not_both_admitted(self):
        tools = AirflowTools(_tool(ToolResult("found")))

        outcomes = await asyncio.gather(
            _serve_and_leave(tools), _serve_and_leave(tools), return_exceptions=True
        )

        assert sum(isinstance(o, RuntimeError) and "already serving" in str(o) for o in outcomes) == 1
        assert sum(o is None for o in outcomes) == 1

    @pytest.mark.asyncio
    async def test_a_server_that_fails_to_start_leaves_nothing_behind(self):
        tools = AirflowTools(_tool(ToolResult("found")))
        created: list[str] = []
        make_directory = tempfile.mkdtemp

        def _record_directory(*args: Any, **kwargs: Any) -> str:
            created.append(make_directory(*args, **kwargs))
            return created[-1]

        with (
            mock.patch(f"{MODULE}.tempfile.mkdtemp", side_effect=_record_directory),
            mock.patch(f"{MODULE}.uvicorn.Server.serve", side_effect=OSError("bind failed")),
        ):
            with pytest.raises(RuntimeError, match="stopped while starting"):
                await _serve_and_leave(tools)

        assert len(created) == 1
        assert not _exists(created[0])
        async with tools.serve():
            pass

    @pytest.mark.asyncio
    async def test_serving_leaves_the_workers_signal_handlers_alone(self):
        tools = AirflowTools(_tool(ToolResult("found")))
        before = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}

        async with tools.serve():
            during = {sig: signal.getsignal(sig) for sig in before}

        assert during == before


class TestOptions:
    @pytest.mark.asyncio
    async def test_build_thread_options_defaults_to_deny_all_read_only_ephemeral_in_the_empty_directory(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            options = tools.build_thread_options(model="gpt-test")

        assert options == {
            "model": "gpt-test",
            "approval_mode": ApprovalMode.deny_all,
            "sandbox": Sandbox.read_only,
            "cwd": config.cwd,
            "ephemeral": True,
        }

    @pytest.mark.asyncio
    async def test_a_value_the_caller_passes_wins_over_the_default(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve():
            options = tools.build_thread_options(
                approval_mode=ApprovalMode.auto_review, sandbox=Sandbox.workspace_write, ephemeral=False
            )

        assert options["approval_mode"] == ApprovalMode.auto_review
        assert options["sandbox"] == Sandbox.workspace_write
        assert options["ephemeral"] is False

    def test_building_options_outside_the_serve_block_raises(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        with pytest.raises(RuntimeError, match="inside the serve\\(\\) block"):
            tools.build_thread_options()

    @pytest.mark.asyncio
    async def test_a_colliding_server_name_raises(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve():
            with pytest.raises(ValueError, match="already has an MCP server named 'airflow'"):
                tools.build_thread_options(config={"mcp_servers": {"airflow": {}}})

    @pytest.mark.parametrize("server_name", ["has space", "dotted.name", 'quo"te', ""])
    def test_a_server_name_that_is_not_a_bare_config_key_raises(self, server_name):
        with pytest.raises(ValueError, match="letters, digits"):
            AirflowTools(_tool(ToolResult("ok")), server_name=server_name)

    @pytest.mark.asyncio
    async def test_the_child_environment_points_codex_at_the_temporary_directories(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            env = config.env
            codex_home, home, cwd = Path(env["CODEX_HOME"]), Path(env["HOME"]), Path(config.cwd)
            assert [_list_dir(path) for path in (codex_home, home, cwd)] == [[], [], []]
            assert len({codex_home, home, cwd}) == 3
            assert codex_home.parent == home.parent == cwd.parent
            assert env["CODEX_SQLITE_HOME"] == env["CODEX_HOME"]
            assert set(env) == {"CODEX_HOME", "CODEX_SQLITE_HOME", "HOME", "AIRFLOW_CODEX_MCP_TOKEN"}

    @pytest.mark.asyncio
    async def test_the_token_goes_through_the_environment_and_not_argv(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            token = config.env["AIRFLOW_CODEX_MCP_TOKEN"]
            overrides = _parse_overrides(config)

        assert not any(token in override for override in config.config_overrides)
        assert overrides["mcp_servers.airflow.bearer_token_env_var"] == '"AIRFLOW_CODEX_MCP_TOKEN"'

    @pytest.mark.asyncio
    async def test_the_config_overrides_apply_each_safe_default(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            overrides = _parse_overrides(config)
            url = _server_url(config)

        assert url == f"http://127.0.0.1:{url.split(':')[2].split('/')[0]}/mcp"
        assert {key: overrides[key] for key in EXPECTED_OVERRIDES} == EXPECTED_OVERRIDES
        assert overrides["mcp_servers.airflow.supports_parallel_tool_calls"] == "false"

    @pytest.mark.asyncio
    async def test_the_tool_timeout_is_set_and_can_be_changed(self):
        default = AirflowTools(_tool(ToolResult("ok")))
        custom = AirflowTools(_tool(ToolResult("ok")), tool_timeout=30)

        async with default.serve() as default_config, custom.serve() as custom_config:
            assert _parse_overrides(default_config)["mcp_servers.airflow.tool_timeout_sec"] == "600.0"
            assert _parse_overrides(custom_config)["mcp_servers.airflow.tool_timeout_sec"] == "30"

    @pytest.mark.asyncio
    async def test_a_custom_server_name_is_used_in_every_key(self):
        tools = AirflowTools(_tool(ToolResult("ok")), server_name="warehouse-tools")

        async with tools.serve() as config:
            keys = [key for key in _parse_overrides(config) if key.startswith("mcp_servers.")]

        assert keys
        assert all(key.startswith("mcp_servers.warehouse-tools.") for key in keys)

    @pytest.mark.asyncio
    async def test_the_schema_byte_budget_covers_the_largest_tool_schema(self):
        small = AirflowTools(_tool(ToolResult("ok")))
        large_schema = {
            "type": "object",
            "properties": {f"field_{i}": {"type": "string", "description": "d" * 50} for i in range(2000)},
        }
        large = AirflowTools(
            AirflowTool(
                name="large",
                description="Large.",
                parameters=large_schema,
                function=mock.AsyncMock(return_value=ToolResult("ok")),
            )
        )

        async with small.serve() as small_config, large.serve() as large_config:
            small_budget = int(
                _parse_overrides(small_config)["mcp_servers.airflow.tool_input_schema_max_bytes"]
            )
            large_budget = int(
                _parse_overrides(large_config)["mcp_servers.airflow.tool_input_schema_max_bytes"]
            )

        assert small_budget == 65_536
        assert large_budget >= len(json.dumps(large_schema))
        assert large_budget > 65_536


EXPECTED_OVERRIDES = {
    "cli_auth_credentials_store": '"ephemeral"',
    "features.shell_tool": "false",
    "features.unified_exec": "false",
    "features.view_image": "false",
    "features.multi_agent": "false",
    "features.apps": "false",
    "features.plugins": "false",
    "features.image_generation": "false",
    "web_search": '"disabled"',
    "shell_environment_policy.inherit": '"none"',
    "history.persistence": '"none"',
    "analytics.enabled": "false",
    "feedback.enabled": "false",
    "check_for_update_on_startup": "false",
    "otel.metrics_exporter": '"none"',
    "tool_output_token_limit": str(_MAX_INLINE_RESULT_CHARS),
    "mcp_servers.airflow.bearer_token_env_var": '"AIRFLOW_CODEX_MCP_TOKEN"',
    "mcp_servers.airflow.required": "true",
    "mcp_servers.airflow.tool_timeout_sec": "600.0",
    "mcp_servers.airflow.tool_input_schema_max_bytes": "65536",
}
# What ``config/read`` should report for the keys above. ``supports_parallel_tool_calls=false`` is
# not in it: codex 0.163.0 leaves a ``false`` out of the echo, so only ``true`` can be read back.
EXPECTED_ECHOES: dict[str, Any] = {
    "cli_auth_credentials_store": "ephemeral",
    "features.shell_tool": False,
    "features.unified_exec": False,
    "features.view_image": False,
    "features.multi_agent": False,
    "features.apps": False,
    "features.plugins": False,
    "features.image_generation": False,
    "web_search": "disabled",
    "shell_environment_policy.inherit": "none",
    "history.persistence": "none",
    "analytics.enabled": False,
    "feedback.enabled": False,
    "check_for_update_on_startup": False,
    "otel.metrics_exporter": "none",
    "tool_output_token_limit": _MAX_INLINE_RESULT_CHARS,
    "mcp_servers.airflow.bearer_token_env_var": "AIRFLOW_CODEX_MCP_TOKEN",
    "mcp_servers.airflow.required": True,
    "mcp_servers.airflow.tool_timeout_sec": 600.0,
    "mcp_servers.airflow.tool_input_schema_max_bytes": 65536,
}


def _read_path(data: dict[str, Any], dotted: str) -> Any:
    current: Any = data
    for part in dotted.split("."):
        if not isinstance(current, dict) or part not in current:
            return "<absent>"
        current = current[part]
    return current


def _query_app_server(config: CodexConfig) -> tuple[dict[str, Any], ListMcpServerStatusResponse]:
    """Start the real ``codex app-server`` (no login, no model) and read back its config and MCP status."""
    with CodexClient(config) as client:
        client.initialize()
        read = client.request(
            "config/read", {"includeLayers": True, "cwd": config.cwd}, response_model=ConfigReadResponse
        )
        status = client.request("mcpServerStatus/list", {}, response_model=ListMcpServerStatusResponse)
    return read.model_dump(by_alias=True, exclude_none=True, mode="json"), status


async def _start_thread(config: CodexConfig, **thread_options: Any) -> ThreadStartResponse:
    """Start a thread on a real ``codex app-server`` and return the raw ``thread/start`` response."""
    async with AsyncCodex(config) as codex:
        started: list[ThreadStartResponse] = []
        # Wraps a private SDK method on purpose: it is the only place the raw response is visible,
        # and an SDK rename makes this fail loudly.
        start = codex._client.thread_start

        async def _record_start(params: Any) -> ThreadStartResponse:
            started.append(await start(params))
            return started[-1]

        codex._client.thread_start = _record_start  # type: ignore[method-assign,assignment]
        await codex.thread_start(**thread_options)
    return started[0]


class TestRealAppServer:
    """
    Read the safe defaults back from a real ``codex app-server``.

    Codex silently ignores a ``--config`` key it does not know, so a key that is misspelled or
    that a newer Codex drops passes every other test; only reading it back catches that. These
    tests start the binary in ``openai-codex-cli-bin``, with no login and no model call. Every
    process runs with ``HOME``, ``CODEX_HOME`` and its working directory in empty temporary
    directories and never touches the real ``~/.codex``.
    """

    @pytest.fixture(autouse=True)
    def worker_environment(self, tmp_path, monkeypatch) -> dict[str, Path]:
        try:
            from codex_cli_bin import bundled_codex_path

            bundled_codex_path()
        except (ImportError, FileNotFoundError) as e:
            pytest.skip(f"The codex binary of openai-codex-cli-bin is not available: {e}")
        # What a worker could carry in its environment: a populated ~/.codex, a CODEX_HOME of its
        # own, and a CODEX_SQLITE_HOME elsewhere. None of it may reach, or be written by, Codex.
        dirs = {name: tmp_path / name for name in ("worker_home", "worker_codex_home", "worker_sqlite")}
        for path in dirs.values():
            path.mkdir()
        leaked_config = 'model = "marker-model-xyz"\n[mcp_servers.leak]\nurl = "http://127.0.0.1:1/leak"\n'
        (dirs["worker_codex_home"] / "config.toml").write_text(leaked_config)
        (dirs["worker_home"] / ".codex").mkdir()
        (dirs["worker_home"] / ".codex" / "config.toml").write_text(leaked_config)
        monkeypatch.setenv("HOME", str(dirs["worker_home"]))
        monkeypatch.setenv("CODEX_HOME", str(dirs["worker_codex_home"]))
        monkeypatch.setenv("CODEX_SQLITE_HOME", str(dirs["worker_sqlite"]))
        return dirs

    @pytest.mark.asyncio
    async def test_every_safe_default_is_read_back_from_codex(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            echoed, _ = await asyncio.to_thread(_query_app_server, config)

        settings = echoed["config"]
        expected = {**EXPECTED_ECHOES, "mcp_servers.airflow.url": _server_url(config)}
        assert {key: _read_path(settings, key) for key in expected} == expected

    @pytest.mark.asyncio
    async def test_supports_parallel_tool_calls_is_a_key_codex_knows(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            parallel = dataclasses.replace(
                config,
                config_overrides=tuple(
                    "mcp_servers.airflow.supports_parallel_tool_calls=true"
                    if override.startswith("mcp_servers.airflow.supports_parallel_tool_calls=")
                    else override
                    for override in config.config_overrides
                ),
            )
            echoed, _ = await asyncio.to_thread(_query_app_server, parallel)

        # Only ``true`` is echoed, so this shows the key name is accepted; whether ``false`` equals
        # Codex's default is unconfirmed.
        assert _read_path(echoed["config"], "mcp_servers.airflow.supports_parallel_tool_calls") is True

    @pytest.mark.asyncio
    async def test_codex_reads_the_temporary_codex_home_and_not_the_workers(self, worker_environment):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            echoed, _ = await asyncio.to_thread(_query_app_server, config)
            codex_home = config.env["CODEX_HOME"]

        user_files = [
            _real_path(layer["name"]["file"]) for layer in echoed["layers"] if layer["name"]["type"] == "user"
        ]
        assert user_files == [_real_path(codex_home, "config.toml")]
        # The system layer (/etc/codex/config.toml) is still read from the worker; any other
        # kind of layer, such as a project or a managed one, would be new.
        assert {layer["name"]["type"] for layer in echoed["layers"]} <= {"sessionFlags", "user", "system"}
        assert _read_path(echoed["config"], "model") == "<absent>"
        assert set(echoed["config"]["mcp_servers"]) == {"airflow"}

    @pytest.mark.asyncio
    async def test_no_project_layer_is_loaded_from_the_workers_working_directory(self, tmp_path, monkeypatch):
        project = tmp_path / "project"
        (project / ".git").mkdir(parents=True)
        (project / ".codex").mkdir()
        (project / ".codex" / "config.toml").write_text('model = "project-marker-model"\n')
        monkeypatch.chdir(project)
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            # Control: the probe does see a project layer when Codex is pointed at the project.
            in_project, _ = await asyncio.to_thread(
                _query_app_server, dataclasses.replace(config, cwd=str(project))
            )
            # Codex resolves the project layer from the working directory the thread is started in.
            # Without a ``cwd`` it falls back to the child process's, which ``serve()`` sets.
            response = await _start_thread(config)
            echoed, _ = await asyncio.to_thread(
                _query_app_server, dataclasses.replace(config, cwd=str(response.cwd.root))
            )

        assert "project" in [layer["name"]["type"] for layer in in_project["layers"]]
        temporary_cwd = _real_path(os.path.dirname(config.env["CODEX_HOME"]), "cwd")
        assert _real_path(str(response.cwd.root)) == temporary_cwd
        assert "project" not in [layer["name"]["type"] for layer in echoed["layers"]]

    @pytest.mark.asyncio
    async def test_codex_state_stays_in_the_temporary_directory(self, worker_environment):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            await asyncio.to_thread(_query_app_server, config)
            written_to_temporary = _list_dir(config.env["CODEX_HOME"])

        assert written_to_temporary
        assert _list_dir(worker_environment["worker_sqlite"]) == []
        assert [p.name for p in _list_dir(worker_environment["worker_home"] / ".codex")] == ["config.toml"]

    @pytest.mark.asyncio
    async def test_a_worker_codex_sqlite_home_is_used_without_the_override(self, worker_environment):
        """Negative control: without ``CODEX_SQLITE_HOME`` in the child environment, the worker's wins."""
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            env = {k: v for k, v in config.env.items() if k != "CODEX_SQLITE_HOME"}
            await asyncio.to_thread(_query_app_server, dataclasses.replace(config, env=env))

        assert _list_dir(worker_environment["worker_sqlite"])

    @pytest.mark.asyncio
    async def test_codex_lists_the_airflow_tools_over_the_authenticated_connection(self):
        ts = SQLToolset("pg_default")
        tools = AirflowTools(ts)

        async with tools.serve() as config:
            _, status = await asyncio.to_thread(_query_app_server, config)

        [server] = status.data
        assert server.name == "airflow"
        assert server.tools_error is None
        assert set(server.tools) == {t.name for t in ts.airflow_tools()}

    @pytest.mark.asyncio
    async def test_codex_cannot_list_the_tools_with_a_wrong_token(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            wrong = dataclasses.replace(config, env={**config.env, "AIRFLOW_CODEX_MCP_TOKEN": "wrong"})
            _, status = await asyncio.to_thread(_query_app_server, wrong)

        [server] = status.data
        assert server.tools == {}
        assert "401" in (server.tools_error or "")

    @pytest.mark.asyncio
    async def test_a_thread_starts_with_the_defaults_when_the_server_is_reachable(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            response = await _start_thread(config, **tools.build_thread_options())

        assert response.approval_policy.root == AskForApprovalValue.never
        assert response.sandbox.root.type == "readOnly"
        assert response.thread.ephemeral is True
        assert _real_path(str(response.cwd.root)) == _real_path(config.cwd)

    @pytest.mark.asyncio
    async def test_a_thread_does_not_start_when_the_required_server_is_unreachable(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            url = _server_url(config)
            port = url.split(":")[2].split("/")[0]
            wrong_port = dataclasses.replace(
                config,
                config_overrides=tuple(o.replace(f":{port}/", ":1/") for o in config.config_overrides),
            )
            async with AsyncCodex(wrong_port) as codex:
                with pytest.raises(InternalRpcError, match="required MCP servers failed to initialize"):
                    await codex.thread_start(**tools.build_thread_options())

    @pytest.mark.asyncio
    async def test_an_unreachable_server_is_reported_in_the_status_and_not_raised(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        async with tools.serve() as config:
            port = _server_url(config).split(":")[2].split("/")[0]
            wrong_port = dataclasses.replace(
                config,
                config_overrides=tuple(o.replace(f":{port}/", ":1/") for o in config.config_overrides),
            )
            _, status = await asyncio.to_thread(_query_app_server, wrong_port)

        [server] = status.data
        assert server.tools == {}
        assert server.tools_error


def test_missing_sdk_raises_optional_feature_exception() -> None:
    with mock.patch.dict(sys.modules, {"openai_codex": None}):
        sys.modules.pop(MODULE, None)
        try:
            with pytest.raises(AirflowOptionalProviderFeatureException):
                importlib.import_module(MODULE)
        finally:
            sys.modules.pop(MODULE, None)


def test_missing_mcp_raises_optional_feature_exception() -> None:
    with mock.patch.dict(sys.modules, {"mcp": None}):
        sys.modules.pop(MODULE, None)
        try:
            with pytest.raises(AirflowOptionalProviderFeatureException):
                importlib.import_module(MODULE)
        finally:
            sys.modules.pop(MODULE, None)
