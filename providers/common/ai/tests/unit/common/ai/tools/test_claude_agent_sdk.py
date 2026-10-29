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
import copy
import importlib
import json
import sys
from collections.abc import AsyncIterator
from typing import Any
from unittest import mock

import pytest

pytest.importorskip("claude_agent_sdk")

from claude_agent_sdk import AssistantMessage, Message, ResultMessage, TextBlock, UserMessage
from pydantic_ai.exceptions import ModelRetry

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult, collect_tools
from airflow.providers.common.ai.tools.claude_agent_sdk import AirflowTools
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

from unit.common.ai.tools.test__from_toolset import _scripted
from unit.common.ai.toolsets.test_sql import _make_mock_db_hook

MODULE = "airflow.providers.common.ai.tools.claude_agent_sdk"


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


async def _pong(arguments: dict[str, Any]) -> ToolResult:
    return ToolResult("pong")


def _build_tools(*sources: Any, server_name: str = "airflow") -> tuple[AirflowTools, dict[str, Any]]:
    """
    Build ``AirflowTools``, with ``create_sdk_mcp_server`` mocked to capture its SDK tools.

    The ``@tool`` decorator itself is not mocked: it is only a plain wrapper
    (``SdkMcpTool(handler=handler, ...)``), so the handlers it returns run the same code
    they would through the real MCP server. Returns the instance and its handlers by name.
    """
    captured: dict[str, Any] = {}

    def _capture_server(*, name: str, version: str = "1.0.0", tools: Any = None) -> Any:
        captured["sdk_tools"] = tools
        return mock.sentinel.server

    with mock.patch(f"{MODULE}.create_sdk_mcp_server", autospec=True) as mock_create_server:
        mock_create_server.side_effect = _capture_server
        tools = AirflowTools(*sources, server_name=server_name)
    handlers = {sdk_tool.name: sdk_tool.handler for sdk_tool in captured["sdk_tools"]}
    return tools, handlers


def _assistant_message(message_id: str | None = None) -> AssistantMessage:
    return AssistantMessage(
        content=[TextBlock(text="")], model="claude-test", stop_reason="tool_use", message_id=message_id
    )


def _user_message() -> UserMessage:
    return UserMessage(content="")


def _result_message(**overrides: Any) -> ResultMessage:
    fields: dict[str, Any] = {
        "subtype": "success",
        "duration_ms": 1,
        "duration_api_ms": 1,
        "is_error": False,
        "num_turns": 1,
        "session_id": "session-1",
    }
    fields.update(overrides)
    return ResultMessage(**fields)


async def _fake_cli(
    handlers: dict[str, Any],
    turns: list[list[tuple[str, dict[str, Any]]]],
    *,
    final: ResultMessage | None = None,
    message_ids: list[str | None] | None = None,
) -> AsyncIterator[Message]:
    """
    Stand in for the CLI driving the tools ``AirflowTools`` served through one or more turns.

    For each turn, yields an ``AssistantMessage`` with a ``tool_use`` stop reason, then
    awaits every one of that turn's calls -- as the CLI waits for a turn's tool results
    before sending the next request -- before yielding the ``UserMessage`` carrying them.
    Ends with a ``ResultMessage``. ``message_ids`` gives each turn's ``AssistantMessage.message_id``.
    """
    for index, turn in enumerate(turns):
        yield _assistant_message(message_ids[index] if message_ids else None)
        for tool_name, args in turn:
            await handlers[tool_name](args)
        yield _user_message()
    yield final if final is not None else _result_message()


async def _call_real_server(tools: AirflowTools, name: str, args: dict[str, Any]) -> dict[str, Any]:
    """Call a tool through the real in-process MCP server, with the JSON-RPC the SDK feeds its bridge."""
    return await _request_real_server(tools, "tools/call", {"name": name, "arguments": args})


async def _request_real_server(tools: AirflowTools, method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Send one JSON-RPC request to the real in-process MCP server and return its ``result``."""
    moved = (
        "claude_agent_sdk's private path _internal.sdk_mcp_bridge.SdkMcpBridge has changed; "
        "update _request_real_server in this test module."
    )
    try:
        bridge_module = importlib.import_module("claude_agent_sdk._internal.sdk_mcp_bridge")
    except ImportError:
        pytest.fail(moved)
    bridge_cls = getattr(bridge_module, "SdkMcpBridge", None)
    if bridge_cls is None:
        pytest.fail(moved)
    bridge = bridge_cls("airflow", tools.server["instance"])
    try:
        await bridge.handle(
            {
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1"},
                },
            }
        )
        await bridge.handle({"jsonrpc": "2.0", "method": "notifications/initialized"})
        reply = await bridge.handle(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }
        )
        return reply["result"]
    finally:
        await bridge.aclose()


class TestTools:
    def test_each_toolset_tool_becomes_an_mcp_tool_with_its_schema(self):
        tools, handlers = _build_tools(_tool(ToolResult("ok")), SQLToolset("pg_default"))

        assert tools.allowed_tools == [
            "mcp__airflow__lookup",
            "mcp__airflow__list_tables",
            "mcp__airflow__get_schema",
            "mcp__airflow__query",
            "mcp__airflow__check_query",
        ]
        assert set(handlers) == {"lookup", "list_tables", "get_schema", "query", "check_query"}

    def test_allowed_tools_use_a_custom_server_name(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")), server_name="warehouse")

        assert tools.allowed_tools == ["mcp__warehouse__lookup"]

    def test_an_mcp_tool_does_not_share_the_source_schema(self):
        source = _tool(ToolResult("ok"))
        before = copy.deepcopy(source.parameters)
        captured: dict[str, Any] = {}

        def _capture_server(*, name: str, version: str = "1.0.0", tools: Any = None) -> Any:
            captured["sdk_tools"] = tools
            return mock.sentinel.server

        with mock.patch(f"{MODULE}.create_sdk_mcp_server", autospec=True) as mock_create_server:
            mock_create_server.side_effect = _capture_server
            AirflowTools(source)

        captured["sdk_tools"][0].input_schema["properties"]["key"]["type"] = "integer"
        assert source.parameters == before

    def test_every_mcp_tool_keeps_a_large_result_inline(self):
        captured: dict[str, Any] = {}
        sources = [_tool(ToolResult("ok")), SQLToolset("pg_default")]

        def _capture_server(*, name: str, version: str = "1.0.0", tools: Any = None) -> Any:
            captured["sdk_tools"] = tools
            return mock.sentinel.server

        with mock.patch(f"{MODULE}.create_sdk_mcp_server", autospec=True) as mock_create_server:
            mock_create_server.side_effect = _capture_server
            AirflowTools(*sources)

        assert [t.name for t in captured["sdk_tools"]] == [t.name for t in collect_tools(sources)]
        for sdk_tool in captured["sdk_tools"]:
            assert sdk_tool.annotations.maxResultSizeChars == 500_000

    @pytest.mark.asyncio
    async def test_a_schema_without_properties_takes_no_arguments(self):
        source = AirflowTool(
            name="ping",
            description="Ping.",
            parameters={"type": "object"},
            function=mock.create_autospec(_pong, return_value=ToolResult("pong")),
        )
        tools = AirflowTools(source)

        listed = await _request_real_server(tools, "tools/list", {})

        assert listed["tools"][0]["inputSchema"].get("required", []) == []
        assert "type" not in listed["tools"][0]["inputSchema"]["properties"]
        assert source.parameters == {"type": "object"}

    @pytest.mark.asyncio
    async def test_the_cli_lists_each_tool_with_its_description_schema_and_result_limit(self):
        schema = {
            "type": "object",
            "properties": {"key": {"type": "string", "description": "The key."}},
            "required": ["key"],
        }
        source = AirflowTool(
            name="lookup",
            description="Look a value up.",
            parameters=schema,
            function=mock.create_autospec(_pong, return_value=ToolResult("pong")),
        )
        tools = AirflowTools(source)

        listed = await _request_real_server(tools, "tools/list", {})

        (listed_tool,) = listed["tools"]
        assert listed_tool["name"] == "lookup"
        assert listed_tool["description"] == "Look a value up."
        assert listed_tool["inputSchema"] == schema
        assert listed_tool["_meta"] == {"anthropic/maxResultSizeChars": 500_000}


class TestHandler:
    @pytest.mark.asyncio
    async def test_the_model_reads_a_toolset_result(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Ada")], last_description=[("id",), ("name",)])
        tools, handlers = _build_tools(ts)

        payload = await handlers["query"]({"sql": "SELECT id, name FROM users"})

        assert "is_error" not in payload
        assert json.loads(payload["content"][0]["text"])["rows"] == [[1, "Ada"]]

    @pytest.mark.asyncio
    async def test_a_correctable_failure_reaches_the_model_as_an_error(self):
        tools, handlers = _build_tools(_tool(ToolResult("Unknown column 'nme'", is_error=True)))

        payload = await handlers["lookup"]({"key": "a"})

        assert payload["is_error"] is True
        assert payload["content"] == [{"type": "text", "text": "Unknown column 'nme'"}]

    @pytest.mark.asyncio
    async def test_a_structured_result_reaches_the_model_as_json(self):
        tools, handlers = _build_tools(_tool(ToolResult({"rows": [[1, None]]})))

        payload = await handlers["lookup"]({"key": "a"})

        assert json.loads(payload["content"][0]["text"]) == {"rows": [[1, None]]}

    @pytest.mark.asyncio
    async def test_not_driven_by_run_hands_the_failure_to_the_model_with_a_warning(self):
        tools, handlers = _build_tools(_tool(PermissionError("role cannot read orders")))

        with mock.patch(f"{MODULE}.log", autospec=True) as mock_log:
            payload = await handlers["lookup"]({"key": "a"})

        assert payload["is_error"] is True
        assert "PermissionError: role cannot read orders" in payload["content"][0]["text"]
        mock_log.warning.assert_called_once()
        assert "not driven by AirflowTools.run" in mock_log.warning.call_args.args[0]

    @pytest.mark.asyncio
    async def test_the_toolsets_calls_are_counted_as_claude_agent_sdk(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        tools, handlers = _build_tools(ts)

        with mock.patch(
            "airflow.providers.common.ai.utils.tool_metrics.Stats", mock.MagicMock(spec=["incr"])
        ) as stats:
            await handlers["list_tables"]({})

        assert stats.incr.call_args.kwargs["tags"]["framework"] == "claude_agent_sdk"

    @pytest.mark.enable_redact
    @pytest.mark.asyncio
    async def test_a_value_mask_secrets_cannot_descend_into_is_masked_after_rendering(
        self, registered_secret
    ):
        """
        ``mask_secrets`` leaves an opaque object (not a dict, list or dataclass) untouched, so
        only masking *after* it is turned into text -- what ``serialize_for_llm`` does --
        catches a secret in its ``repr``. ``json.dumps(value, default=str)`` masks first and
        renders after, so it would not catch this.
        """

        class _Row:
            def __init__(self, secret: str) -> None:
                self._secret = secret

            def __repr__(self) -> str:
                return f"_Row(secret={self._secret!r})"

        tools, handlers = _build_tools(_tool(ToolResult(_Row(f"key={registered_secret}"))))

        payload = await handlers["lookup"]({"key": "a"})

        text = payload["content"][0]["text"]
        assert registered_secret not in text
        assert "key=***" in text

    @pytest.mark.enable_redact
    @pytest.mark.asyncio
    async def test_a_secret_in_a_plain_string_result_is_masked(self, registered_secret):
        tools, handlers = _build_tools(_tool(ToolResult(f"key={registered_secret}")))

        payload = await handlers["lookup"]({"key": "a"})

        assert registered_secret not in payload["content"][0]["text"]
        assert payload["content"][0]["text"] == "key=***"


class TestRun:
    @pytest.mark.asyncio
    async def test_run_returns_the_result_message(self):
        tools, handlers = _build_tools(_tool(ToolResult("ok")))
        final = _result_message(result="done")

        result = await tools.run(_fake_cli(handlers, [[("lookup", {"key": "a"})]], final=final))

        assert result is final

    @pytest.mark.asyncio
    async def test_run_raises_without_a_result_message(self):
        tools, handlers = _build_tools()

        async def _no_result() -> AsyncIterator[Message]:
            return
            yield  # pragma: no cover - makes this an async generator function

        with pytest.raises(RuntimeError, match="ended without a ResultMessage"):
            await tools.run(_no_result())

    @pytest.mark.asyncio
    async def test_a_failure_before_the_stream_ends_without_a_result_message_still_raises_it(self):
        """
        The CLI can end the stream right after a failed tool call without sending another
        message at all -- no further ``UserMessage``, no ``ResultMessage``. The in-loop check
        (see the test above) never runs again once the stream is exhausted, so the failure
        has to be checked once more after the loop, before falling back to the generic
        "ended without a ResultMessage" error.
        """
        tools, handlers = _build_tools(_tool(PermissionError("role cannot read orders")))

        async def _cli_ends_right_after_the_failure() -> AsyncIterator[Message]:
            yield _assistant_message()
            await handlers["lookup"]({"key": "a"})
            # The stream ends here: no further message, no ResultMessage.

        with pytest.raises(ToolCallError, match="PermissionError: role cannot read orders"):
            await tools.run(_cli_ends_right_after_the_failure())

    @pytest.mark.asyncio
    async def test_any_other_failure_fails_the_run_at_the_next_message(self):
        """
        The handler answers the model within the turn -- ``ran`` has one call, and the model
        did read the masked failure -- but ``run`` still raises at the next message rather
        than letting the stream reach a ``ResultMessage``.
        """
        ran: list[dict[str, Any]] = []
        tools, handlers = _build_tools(_tool(PermissionError("role cannot read orders"), ran))

        with pytest.raises(ToolCallError, match="PermissionError: role cannot read orders"):
            await tools.run(_fake_cli(handlers, [[("lookup", {"key": "a"})]]))

        assert len(ran) == 1

    @pytest.mark.asyncio
    async def test_a_failure_in_one_turn_stops_the_stream_before_the_next_turn_starts(self):
        ran: list[dict[str, Any]] = []
        turn_two_started: list[bool] = []
        tools, handlers = _build_tools(_tool(PermissionError("role cannot read orders"), ran))

        async def _two_turns() -> AsyncIterator[Message]:
            yield _assistant_message()
            await handlers["lookup"]({"key": "a"})
            yield _user_message()
            turn_two_started.append(True)
            yield _assistant_message()
            await handlers["lookup"]({"key": "b"})
            yield _user_message()
            yield _result_message()

        with pytest.raises(ToolCallError, match="PermissionError: role cannot read orders"):
            await tools.run(_two_turns())

        assert turn_two_started == []
        assert len(ran) == 1

    @pytest.mark.asyncio
    async def test_calls_after_a_failure_in_the_same_turn_do_not_run(self):
        ran: list[dict[str, Any]] = []
        tools, handlers = _build_tools(_tool(PermissionError("role cannot read orders"), ran))

        async def _three_calls_one_turn() -> AsyncIterator[Message]:
            yield _assistant_message()
            for _ in range(3):
                await handlers["lookup"]({"key": "a"})
            yield _user_message()
            yield _result_message()

        with pytest.raises(ToolCallError, match="PermissionError"):
            await tools.run(_three_calls_one_turn())

        assert len(ran) == 1

    @pytest.mark.asyncio
    async def test_an_error_result_message_raises_instead_of_being_returned(self):
        tools, _ = _build_tools()

        async def _ends_at_the_result() -> AsyncIterator[Message]:
            yield _result_message(
                is_error=True,
                subtype="error_max_turns",
                result="partial answer",
                errors=["hit the turn limit"],
                num_turns=3,
            )

        with pytest.raises(RuntimeError) as exc_info:
            await tools.run(_ends_at_the_result())

        message = str(exc_info.value)
        assert "error_max_turns" in message
        assert "partial answer" in message
        assert "hit the turn limit" in message

    @pytest.mark.asyncio
    async def test_an_error_turn_followed_by_a_successful_turn_still_raises(self):
        tools, _ = _build_tools()

        async def _error_then_success() -> AsyncIterator[Message]:
            yield _result_message(is_error=True, subtype="error_during_execution", result="first turn failed")
            yield _result_message()

        with pytest.raises(RuntimeError, match="first turn failed"):
            await tools.run(_error_then_success())

    @pytest.mark.asyncio
    async def test_the_first_error_turn_is_reported_when_several_turns_fail(self):
        tools, _ = _build_tools()

        async def _two_errors_then_success() -> AsyncIterator[Message]:
            yield _result_message(is_error=True, subtype="error_during_execution", result="first turn failed")
            yield _result_message(
                is_error=True, subtype="error_during_execution", result="second turn failed"
            )
            yield _result_message()

        with pytest.raises(RuntimeError, match="first turn failed") as exc_info:
            await tools.run(_two_errors_then_success())

        assert "second turn failed" not in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_an_error_result_message_bounds_its_free_text_fields(self):
        tools, _ = _build_tools()

        async def _runaway() -> AsyncIterator[Message]:
            yield _result_message(
                is_error=True,
                subtype="error_max_turns",
                result="x" * 100_000,
                errors=[f"error-{i}" for i in range(1_000)],
            )

        with pytest.raises(RuntimeError) as exc_info:
            await tools.run(_runaway())

        message = str(exc_info.value)
        assert len(message) < 2_000
        assert "error_max_turns" in message
        assert "(+99502 chars)" in message
        assert "error-4" in message
        assert "error-5" not in message
        assert "(+995 more)" in message

    @pytest.mark.asyncio
    async def test_an_api_failure_reported_as_a_success_subtype_still_raises(self):
        tools, _ = _build_tools()

        async def _api_failure() -> AsyncIterator[Message]:
            yield _result_message(is_error=True, subtype="success", api_error_status=529)

        with pytest.raises(RuntimeError, match="529"):
            await tools.run(_api_failure())

    @pytest.mark.asyncio
    async def test_an_error_result_message_leaves_unset_fields_out_of_the_message(self):
        tools, _ = _build_tools()

        async def _bare_error() -> AsyncIterator[Message]:
            yield _result_message(is_error=True, subtype="error_during_execution")

        with pytest.raises(RuntimeError) as exc_info:
            await tools.run(_bare_error())

        message = str(exc_info.value)
        assert "error_during_execution" in message
        for unset in ("result=", "errors=", "stop_reason=", "api_error_status=", "terminal_reason="):
            assert unset not in message

    @pytest.mark.asyncio
    async def test_a_tool_failure_takes_priority_over_an_error_result_message(self):
        tools, handlers = _build_tools(_tool(PermissionError("role cannot read orders")))

        async def _failure_then_error_result() -> AsyncIterator[Message]:
            yield _assistant_message()
            await handlers["lookup"]({"key": "a"})
            yield _result_message(is_error=True, subtype="error_max_turns")

        with pytest.raises(ToolCallError, match="PermissionError: role cannot read orders"):
            await tools.run(_failure_then_error_result())

    @pytest.mark.asyncio
    async def test_a_tool_that_keeps_failing_fails_the_run_once_its_retries_are_spent(self):
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)
        tools, handlers = _build_tools(step)

        with pytest.raises(ToolCallError, match="kept failing after 1 correction"):
            await tools.run(_fake_cli(handlers, [[("step", {})], [("step", {})]]))

    @pytest.mark.asyncio
    async def test_each_run_of_a_reused_instance_starts_with_a_fresh_retry_budget(self):
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)
        tools, handlers = _build_tools(step)

        for _ in range(2):
            result = await tools.run(_fake_cli(handlers, [[("step", {})]]))
            assert isinstance(result, ResultMessage)

    @pytest.mark.asyncio
    async def test_failures_in_one_model_turn_count_as_one(self):
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)
        tools, handlers = _build_tools(step)

        result = await tools.run(_fake_cli(handlers, [[("step", {}), ("step", {})]], message_ids=["msg_1"]))

        assert isinstance(result, ResultMessage)

    @pytest.mark.asyncio
    async def test_failures_in_different_model_turns_count_separately(self):
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)
        tools, handlers = _build_tools(step)

        with pytest.raises(ToolCallError, match="kept failing after 1 correction"):
            await tools.run(
                _fake_cli(handlers, [[("step", {})], [("step", {})]], message_ids=["msg_1", "msg_2"])
            )

    @pytest.mark.asyncio
    async def test_sequential_failures_count_separately_when_the_turn_has_no_message_id(self):
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)
        tools, handlers = _build_tools(step)

        with pytest.raises(ToolCallError, match="kept failing after 1 correction"):
            await tools.run(_fake_cli(handlers, [[("step", {}), ("step", {})]], message_ids=[None]))

    @pytest.mark.asyncio
    async def test_simultaneous_failures_count_as_one_when_the_turn_has_no_message_id(self):
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)
        tools, handlers = _build_tools(step)

        async def _two_calls_at_once() -> AsyncIterator[Message]:
            yield _assistant_message()
            await asyncio.gather(handlers["step"]({}), handlers["step"]({}))
            yield _user_message()
            yield _result_message()

        result = await tools.run(_two_calls_at_once())

        assert isinstance(result, ResultMessage)

    @pytest.mark.asyncio
    async def test_the_real_server_path_sees_the_turn_the_run_recorded(self):
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)
        tools = AirflowTools(step)

        async def _two_failures_in_one_turn() -> AsyncIterator[Message]:
            yield _assistant_message("msg_1")
            await _call_real_server(tools, "step", {})
            await _call_real_server(tools, "step", {})
            yield _user_message()
            yield _result_message()

        result = await tools.run(_two_failures_in_one_turn())

        assert isinstance(result, ResultMessage)

    @pytest.mark.asyncio
    async def test_the_stream_is_closed_while_the_run_is_still_active(self):
        ran: list[dict[str, Any]] = []
        tools, handlers = _build_tools(_tool(PermissionError("role cannot read orders"), ran))
        closed_during_run: list[bool] = []

        async def _failing_cli() -> AsyncIterator[Message]:
            try:
                yield _assistant_message()
                await handlers["lookup"]({"key": "a"})
                yield _user_message()
                yield _result_message()
            finally:
                closed_during_run.append(tools._run is not None)

        stream = _failing_cli()
        with pytest.raises(ToolCallError):
            await tools.run(stream)

        assert closed_during_run == [True]
        assert tools._run is None

    @pytest.mark.asyncio
    async def test_a_call_after_a_successful_run_has_ended_does_not_run(self):
        ran: list[dict[str, Any]] = []
        tools, handlers = _build_tools(_tool(ToolResult("ok"), ran))
        await tools.run(_fake_cli(handlers, [[("lookup", {"key": "a"})]]))
        assert len(ran) == 1

        payload = await handlers["lookup"]({"key": "b"})

        assert payload["is_error"] is True
        assert payload["content"][0]["text"].startswith("Not run")
        assert "failed" not in payload["content"][0]["text"]
        assert len(ran) == 1

    @pytest.mark.asyncio
    async def test_a_call_after_a_failed_run_has_ended_does_not_run(self):
        ran: list[dict[str, Any]] = []
        tools, handlers = _build_tools(_tool(PermissionError("role cannot read orders"), ran))
        with pytest.raises(ToolCallError):
            await tools.run(_fake_cli(handlers, [[("lookup", {"key": "a"})]]))

        payload = await handlers["lookup"]({"key": "b"})

        assert payload["is_error"] is True
        assert payload["content"][0]["text"] == "Not run: an earlier Airflow tool call in this run failed."
        assert len(ran) == 1

    @pytest.mark.asyncio
    async def test_a_new_run_is_not_blocked_by_the_previous_ended_run(self):
        ran: list[dict[str, Any]] = []
        tools, handlers = _build_tools(_tool(ToolResult("ok"), ran))
        await tools.run(_fake_cli(handlers, [[("lookup", {"key": "a"})]]))

        await tools.run(_fake_cli(handlers, [[("lookup", {"key": "b"})]]))

        assert ran == [{"key": "a"}, {"key": "b"}]

    @pytest.mark.asyncio
    async def test_a_call_landing_after_the_stream_is_closed_is_refused(self):
        ran: list[dict[str, Any]] = []
        tools, handlers = _build_tools(_tool(ToolResult("ok"), ran))
        late_calls: list[asyncio.Task[dict[str, Any]]] = []

        async def _cli_whose_teardown_calls_a_tool_later() -> AsyncIterator[Message]:
            try:
                yield _assistant_message()
                yield _result_message()
            finally:
                late_calls.append(asyncio.create_task(handlers["lookup"]({"key": "late"})))

        await tools.run(_cli_whose_teardown_calls_a_tool_later())
        assert tools._run is None

        payload = await late_calls[0]

        assert payload["is_error"] is True
        assert payload["content"][0]["text"].startswith("Not run")
        assert ran == []

    @pytest.mark.asyncio
    async def test_a_stream_without_aclose_is_driven_to_the_end(self):
        class _PlainStream:
            def __init__(self, messages: list[Message]) -> None:
                self._messages = messages

            def __aiter__(self) -> AsyncIterator[Message]:
                async def _iterate() -> AsyncIterator[Message]:
                    for message in self._messages:
                        yield message

                return _iterate()

        final = _result_message(result="done")

        result = await AirflowTools(_tool(ToolResult("ok"))).run(_PlainStream([_assistant_message(), final]))

        assert result is final

    @pytest.mark.asyncio
    async def test_a_failing_aclose_does_not_hide_the_tools_failure(self):
        tools, handlers = _build_tools(_tool(PermissionError("role cannot read orders")))

        class _StubbornStream:
            def __aiter__(self) -> AsyncIterator[Message]:
                return _fake_cli(handlers, [[("lookup", {"key": "a"})]])

            async def aclose(self) -> None:
                raise OSError("close failed")

        with pytest.raises(ToolCallError, match="PermissionError: role cannot read orders"):
            await tools.run(_StubbornStream())

        assert tools._run is None

    @pytest.mark.asyncio
    async def test_a_failing_aclose_after_a_clean_run_raises(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))

        class _StubbornStream:
            def __aiter__(self) -> AsyncIterator[Message]:
                return _fake_cli({}, [])

            async def aclose(self) -> None:
                raise OSError("close failed")

        with pytest.raises(OSError, match="close failed"):
            await tools.run(_StubbornStream())

        assert tools._run is None

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.log", autospec=True)
    async def test_a_failing_aclose_is_logged_when_it_does_not_hide_the_tools_failure(self, mock_log):
        tools, handlers = _build_tools(_tool(PermissionError("role cannot read orders")))

        class _StubbornStream:
            def __aiter__(self) -> AsyncIterator[Message]:
                return _fake_cli(handlers, [[("lookup", {"key": "a"})]])

            async def aclose(self) -> None:
                raise OSError("close failed")

        with pytest.raises(ToolCallError):
            await tools.run(_StubbornStream())

        mock_log.warning.assert_called_once()
        assert "Closing the Claude Agent SDK stream failed" in mock_log.warning.call_args.args[0]
        assert str(mock_log.warning.call_args.args[1]) == "close failed"

    @pytest.mark.asyncio
    async def test_a_cancelled_run_closes_the_stream_and_stays_cancelled(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))
        started = asyncio.Event()
        aclose_calls: list[bool] = []

        class _BlockedStream:
            def __aiter__(self) -> AsyncIterator[Message]:
                return self._iterate()

            async def _iterate(self) -> AsyncIterator[Message]:
                yield _assistant_message()
                started.set()
                await asyncio.Event().wait()
                yield _result_message()

            async def aclose(self) -> None:
                aclose_calls.append(True)

        task = asyncio.ensure_future(tools.run(_BlockedStream()))
        await started.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert aclose_calls == [True]
        assert tools._run is None

    @pytest.mark.asyncio
    async def test_concurrent_runs_on_one_instance_are_rejected(self):
        tools, handlers = _build_tools(_tool(ToolResult("ok")))
        gate = asyncio.Event()

        async def _blocked_cli() -> AsyncIterator[Message]:
            yield _assistant_message()
            await gate.wait()
            yield _result_message()

        first = asyncio.ensure_future(tools.run(_blocked_cli()))
        await asyncio.sleep(0)

        with pytest.raises(RuntimeError, match="already running"):
            await tools.run(_fake_cli(handlers, []))

        gate.set()
        await first


class TestRealMcpServer:
    @pytest.mark.asyncio
    async def test_a_tool_result_marked_as_an_error_reaches_the_model_as_isError(self):
        tools = AirflowTools(_tool(ToolResult("not found", is_error=True)))

        result = await _call_real_server(tools, "lookup", {"key": "a"})

        assert result["isError"] is True
        assert "not found" in result["content"][0]["text"]

    @pytest.mark.asyncio
    async def test_a_successful_tool_result_is_not_marked_isError(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        result = await _call_real_server(tools, "lookup", {"key": "a"})

        assert result["isError"] is False

    @pytest.mark.asyncio
    async def test_the_sdk_returns_an_airflow_tool_failure_to_the_model_instead_of_raising(self):
        tools = AirflowTools(_tool(PermissionError("role cannot read orders")))

        result = await _call_real_server(tools, "lookup", {"key": "a"})

        assert result["isError"] is True
        assert "PermissionError: role cannot read orders" in result["content"][0]["text"]

    @pytest.mark.asyncio
    async def test_the_sdk_catches_an_exception_escaping_the_handler(self):
        tools = AirflowTools(_tool(ToolResult("ok")))

        with mock.patch.object(AirflowTool, "call", autospec=True, side_effect=RuntimeError("boom")):
            result = await _call_real_server(tools, "lookup", {"key": "a"})

        assert result["isError"] is True
        assert "boom" in result["content"][0]["text"]


class TestResultTruncation:
    @pytest.mark.asyncio
    async def test_a_result_over_the_ceiling_is_cut_to_fit_with_a_notice(self):
        _, handlers = _build_tools(_tool(ToolResult("x" * 1_200_000)))

        payload = await handlers["lookup"]({"key": "a"})

        text = payload["content"][0]["text"]
        # Booleans keep a failing assertion from rendering a 500k-character diff.
        assert len(text) <= 500_000
        assert ("Truncated: this result had 1200000 characters" in text) is True
        assert text.startswith("x" * 1000) is True
        assert "is_error" not in payload

    @pytest.mark.asyncio
    async def test_a_result_at_the_ceiling_is_returned_as_is(self):
        _, handlers = _build_tools(_tool(ToolResult("x" * 500_000)))

        payload = await handlers["lookup"]({"key": "a"})

        text = payload["content"][0]["text"]
        assert len(text) == 500_000
        assert ("Truncated" in text) is False

    @pytest.mark.asyncio
    async def test_a_result_one_over_the_ceiling_is_cut(self):
        _, handlers = _build_tools(_tool(ToolResult("x" * 500_001)))

        payload = await handlers["lookup"]({"key": "a"})

        text = payload["content"][0]["text"]
        assert len(text) <= 500_000
        assert ("Truncated" in text) is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("text", "is_cut"),
        [
            pytest.param("\N{GRINNING FACE}" * 250_000, False, id="at-the-ceiling-in-utf16-units"),
            pytest.param("\N{GRINNING FACE}" * 250_001, True, id="one-emoji-over-the-ceiling"),
            pytest.param("\N{GRINNING FACE}" * 300_000, True, id="under-the-ceiling-in-code-points"),
        ],
    )
    async def test_the_ceiling_is_measured_in_utf16_units(self, text, is_cut):
        _, handlers = _build_tools(_tool(ToolResult(text)))

        payload = await handlers["lookup"]({"key": "a"})

        result = payload["content"][0]["text"]
        assert (("Truncated" in result) is is_cut) is True
        assert (len(result.encode("utf-16-le")) // 2 <= 500_000) is True

    @pytest.mark.asyncio
    async def test_a_cut_never_splits_a_surrogate_pair(self):
        # The leading "a" shifts every emoji to an odd UTF-16 offset, so the cut falls between the
        # two halves of a pair unless it is moved back to a code point boundary.
        _, handlers = _build_tools(_tool(ToolResult("a" + "\N{GRINNING FACE}" * 300_000)))

        payload = await handlers["lookup"]({"key": "a"})

        result = payload["content"][0]["text"]
        kept = result[: result.index("\n\n[Truncated")]
        # Comparing the whole string would make pytest diff ~250k characters on failure.
        assert kept[0] == "a"
        assert set(kept[1:]) == {"\N{GRINNING FACE}"}
        assert (len(result.encode("utf-16-le")) // 2 <= 500_000) is True

    @pytest.mark.asyncio
    async def test_a_small_result_is_returned_as_is(self):
        _, handlers = _build_tools(_tool(ToolResult("short")))

        payload = await handlers["lookup"]({"key": "a"})

        assert payload == {"content": [{"type": "text", "text": "short"}]}

    @pytest.mark.asyncio
    async def test_a_truncated_error_result_stays_an_error(self):
        _, handlers = _build_tools(_tool(ToolResult("x" * 600_000, is_error=True)))

        payload = await handlers["lookup"]({"key": "a"})

        assert len(payload["content"][0]["text"]) <= 500_000
        assert payload["is_error"] is True


async def _decide_tool_use(tool_name: str, input_data: dict[str, Any], context: Any) -> Any:
    """Shape of a ``can_use_tool`` callback, for ``create_autospec``."""


class TestOptions:
    def test_safe_defaults(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))

        options = tools.build_options()

        assert options.tools == []
        assert options.setting_sources == []
        assert options.strict_mcp_config is True
        assert options.mcp_servers == {"airflow": tools.server}
        assert options.allowed_tools == tools.allowed_tools
        assert options.verbatim_prompts is True
        assert options.permission_mode is None
        assert options.max_buffer_size == 16 * 1024 * 1024

    def test_explicit_max_buffer_size_is_kept(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))

        options = tools.build_options(max_buffer_size=1024)

        assert options.max_buffer_size == 1024

    def test_explicit_kwargs_override_the_defaults(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))

        options = tools.build_options(
            tools=["Bash"], setting_sources=["project"], strict_mcp_config=False, verbatim_prompts=False
        )

        assert options.verbatim_prompts is False
        assert options.tools == ["Bash"]
        assert options.setting_sources == ["project"]
        assert options.strict_mcp_config is False

    def test_other_kwargs_pass_through(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))

        options = tools.build_options(model="claude-sonnet-5", max_turns=10)

        assert options.model == "claude-sonnet-5"
        assert options.max_turns == 10

    def test_mcp_servers_and_allowed_tools_merge_with_the_callers(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))
        other_server = mock.sentinel.other_server

        options = tools.build_options(mcp_servers={"other": other_server}, allowed_tools=["other_tool"])

        assert options.mcp_servers == {"other": other_server, "airflow": tools.server}
        assert options.allowed_tools == ["other_tool", *tools.allowed_tools]

    def test_a_can_use_tool_callback_keeps_the_airflow_tools_out_of_allowed_tools(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))
        can_use_tool = mock.create_autospec(_decide_tool_use)

        options = tools.build_options(can_use_tool=can_use_tool, allowed_tools=["other_tool"])

        assert options.allowed_tools == ["other_tool"]
        assert options.can_use_tool is can_use_tool
        assert options.permission_mode == "default"
        assert tools.allowed_tools == ["mcp__airflow__lookup"]

    def test_a_can_use_tool_callback_does_not_override_an_explicit_permission_mode(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))

        options = tools.build_options(
            can_use_tool=mock.create_autospec(_decide_tool_use), permission_mode="acceptEdits"
        )

        assert options.permission_mode == "acceptEdits"

    def test_a_none_can_use_tool_is_the_same_as_not_passing_one(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))

        options = tools.build_options(can_use_tool=None)

        assert options.allowed_tools == tools.allowed_tools
        assert options.permission_mode is None

    def test_a_colliding_server_name_raises(self):
        tools, _ = _build_tools(_tool(ToolResult("ok")))

        with pytest.raises(ValueError, match="already has a server named 'airflow'"):
            tools.build_options(mcp_servers={"airflow": mock.sentinel.other_server})


def test_missing_sdk_raises_optional_feature_exception() -> None:
    with mock.patch.dict(sys.modules, {"claude_agent_sdk": None}):
        sys.modules.pop(MODULE, None)
        try:
            with pytest.raises(AirflowOptionalProviderFeatureException):
                importlib.import_module(MODULE)
        finally:
            sys.modules.pop(MODULE, None)
