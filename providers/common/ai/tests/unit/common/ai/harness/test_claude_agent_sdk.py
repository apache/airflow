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

import json
from typing import Any
from unittest import mock

import pytest

pytest.importorskip("claude_agent_sdk")

from claude_agent_sdk import ResultMessage

from airflow.models.connection import Connection
from airflow.providers.common.ai.harness.base import HarnessRequest, HarnessRunError
from airflow.providers.common.ai.harness.claude_agent_sdk import ClaudeAgentSDKBackend, _build_tool_handler
from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult

MODULE = "airflow.providers.common.ai.harness.claude_agent_sdk"


def _connection(
    *,
    password: str | None = "sk-test",
    conn_type: str = "pydanticai",
    host: str | None = None,
    extra: dict[str, Any] | None = None,
) -> Connection:
    return Connection(
        conn_id="anthropic_default",
        conn_type=conn_type,
        password=password,
        host=host,
        extra=json.dumps({"model": "anthropic:claude-x"} if extra is None else extra),
    )


def _result_message(
    *,
    subtype: str = "success",
    is_error: bool = False,
    result: str | None = "done",
    session_id: str = "session-1",
    num_turns: int = 1,
    total_cost_usd: float | None = 0.01,
    usage: dict[str, Any] | None = None,
) -> ResultMessage:
    return ResultMessage(
        subtype=subtype,
        duration_ms=100,
        duration_api_ms=90,
        is_error=is_error,
        num_turns=num_turns,
        session_id=session_id,
        total_cost_usd=total_cost_usd,
        usage={"input_tokens": 10, "output_tokens": 5} if usage is None else usage,
        result=result,
    )


def _query_side_effect(*messages: Any, before_yield: Any = None):
    """Build a ``query`` replacement: a plain function returning a fresh async generator."""

    def _query(*, prompt: str, options: Any, transport: Any = None):
        async def _gen():
            if before_yield is not None:
                await before_yield()
            for message in messages:
                yield message

        return _gen()

    return _query


def _tool(outcome: ToolResult | Exception, *, name: str = "lookup") -> AirflowTool:
    async def function(arguments: dict[str, Any]) -> ToolResult:
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return AirflowTool(
        name=name,
        description="A tool.",
        parameters={"type": "object", "properties": {}},
        function=function,
    )


@mock.patch(f"{MODULE}.query", autospec=True)
@mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
def test_options_safe_defaults(mock_get_connection: mock.MagicMock, mock_query: mock.MagicMock) -> None:
    mock_get_connection.return_value = _connection()
    mock_query.side_effect = _query_side_effect(_result_message())

    ClaudeAgentSDKBackend().run(HarnessRequest(prompt="go", llm_conn_id="anthropic_default"))

    options = mock_query.call_args.kwargs["options"]
    assert options.tools == []
    assert options.setting_sources == []
    assert options.allowed_tools == ["mcp__airflow__*"]
    assert options.env == {"ANTHROPIC_API_KEY": "sk-test"}
    assert options.model == "claude-x"


@mock.patch(f"{MODULE}.query", autospec=True)
@mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
def test_run_maps_success_result(mock_get_connection: mock.MagicMock, mock_query: mock.MagicMock) -> None:
    mock_get_connection.return_value = _connection()
    message = _result_message(
        result="the answer", session_id="session-xyz", num_turns=3, total_cost_usd=0.42, usage={"x": 1}
    )
    mock_query.side_effect = _query_side_effect(message)

    result = ClaudeAgentSDKBackend().run(HarnessRequest(prompt="go", llm_conn_id="anthropic_default"))

    assert result.output == "the answer"
    assert result.is_error is False
    assert result.subtype == "success"
    assert result.session_id == "session-xyz"
    assert result.num_turns == 3
    assert result.cost_usd == 0.42
    assert result.usage == {"x": 1}


@pytest.mark.parametrize("subtype", ["error_max_turns", "error_max_budget_usd", "error_during_execution"])
@mock.patch(f"{MODULE}.query", autospec=True)
@mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
def test_run_returns_error_result_without_raising(
    mock_get_connection: mock.MagicMock, mock_query: mock.MagicMock, subtype: str
) -> None:
    mock_get_connection.return_value = _connection()
    mock_query.side_effect = _query_side_effect(_result_message(subtype=subtype, is_error=True))

    result = ClaudeAgentSDKBackend().run(HarnessRequest(prompt="go", llm_conn_id="anthropic_default"))

    assert result.is_error is True
    assert result.subtype == subtype


@mock.patch(f"{MODULE}.query", autospec=True)
@mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
def test_run_raises_when_stream_has_no_result(
    mock_get_connection: mock.MagicMock, mock_query: mock.MagicMock
) -> None:
    mock_get_connection.return_value = _connection()
    mock_query.side_effect = _query_side_effect()  # no messages at all

    with pytest.raises(HarnessRunError, match="ended without a result"):
        ClaudeAgentSDKBackend().run(HarnessRequest(prompt="go", llm_conn_id="anthropic_default"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("content", "is_error", "expected_text"),
    [
        ("a plain answer", False, "a plain answer"),
        ({"rows": [1, 2]}, False, json.dumps({"rows": [1, 2]})),
        ("no such column", True, "no such column"),
    ],
)
async def test_tool_handler_maps_result(content: Any, is_error: bool, expected_text: str) -> None:
    handler = _build_tool_handler(_tool(ToolResult(content=content, is_error=is_error)), [], run=object())

    payload = await handler({})

    assert payload["content"] == [{"type": "text", "text": expected_text}]
    assert payload.get("is_error", False) is is_error


@pytest.mark.asyncio
async def test_tool_handler_records_tool_call_error() -> None:
    failures: list[ToolCallError] = []
    handler = _build_tool_handler(_tool(ToolCallError("boom")), failures, run=object())

    payload = await handler({})

    assert payload == {"content": [{"type": "text", "text": "boom"}], "is_error": True}
    assert len(failures) == 1
    assert isinstance(failures[0], ToolCallError)


@mock.patch(f"{MODULE}.create_sdk_mcp_server", autospec=True)
@mock.patch(f"{MODULE}.tool", autospec=True)
@mock.patch(f"{MODULE}.query", autospec=True)
@mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
def test_run_reraises_tool_call_error(
    mock_get_connection: mock.MagicMock,
    mock_query: mock.MagicMock,
    mock_tool: mock.MagicMock,
    mock_create_server: mock.MagicMock,
) -> None:
    mock_get_connection.return_value = _connection()
    # A pass-through decorator: capture (name, description, schema) via call_args and hand
    # the handler straight to create_sdk_mcp_server, so it can be invoked directly below.
    mock_tool.side_effect = lambda name, description, schema: lambda handler: handler
    captured: dict[str, Any] = {}

    def _capture_server(*, name: str, version: str = "1.0.0", tools: Any = None) -> Any:
        captured["handler"] = tools[0]
        return mock.sentinel.server

    mock_create_server.side_effect = _capture_server

    async def _call_handler_before_yielding() -> None:
        await captured["handler"]({})

    mock_query.side_effect = _query_side_effect(_result_message(), before_yield=_call_handler_before_yielding)

    airflow_tool = _tool(ToolCallError("boom"))
    backend = ClaudeAgentSDKBackend()

    with pytest.raises(ToolCallError, match="boom"):
        backend.run(HarnessRequest(prompt="go", llm_conn_id="anthropic_default", tools=[airflow_tool]))


@mock.patch(f"{MODULE}.create_sdk_mcp_server", autospec=True)
@mock.patch(f"{MODULE}.tool", autospec=True)
@mock.patch(f"{MODULE}.query", autospec=True)
@mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
def test_mcp_server_built_from_tools(
    mock_get_connection: mock.MagicMock,
    mock_query: mock.MagicMock,
    mock_tool: mock.MagicMock,
    mock_create_server: mock.MagicMock,
) -> None:
    mock_get_connection.return_value = _connection()
    mock_tool.side_effect = lambda name, description, schema: lambda handler: handler
    mock_create_server.return_value = mock.sentinel.server
    mock_query.side_effect = _query_side_effect(_result_message())

    airflow_tool = _tool(ToolResult(content="ok"), name="lookup")
    ClaudeAgentSDKBackend().run(
        HarnessRequest(prompt="go", llm_conn_id="anthropic_default", tools=[airflow_tool])
    )

    name, description, schema = mock_tool.call_args.args
    assert (name, description) == ("lookup", "A tool.")
    assert schema == airflow_tool.parameters
    assert schema is not airflow_tool.parameters

    mock_create_server.assert_called_once()
    assert mock_create_server.call_args.kwargs["name"] == "airflow"
    options = mock_query.call_args.kwargs["options"]
    assert options.mcp_servers == {"airflow": mock.sentinel.server}


@mock.patch(f"{MODULE}.query", autospec=True)
@mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
def test_mcp_server_omitted_without_tools(
    mock_get_connection: mock.MagicMock, mock_query: mock.MagicMock
) -> None:
    mock_get_connection.return_value = _connection()
    mock_query.side_effect = _query_side_effect(_result_message())

    ClaudeAgentSDKBackend().run(HarnessRequest(prompt="go", llm_conn_id="anthropic_default"))

    options = mock_query.call_args.kwargs["options"]
    assert "airflow" not in options.mcp_servers


@pytest.mark.parametrize(
    ("connection_kwargs", "match"),
    [
        ({"password": None}, "no password"),
        ({"host": "https://my-custom-endpoint.example"}, "custom host"),
        ({"conn_type": "pydanticai_bedrock"}, "pydanticai_bedrock"),
        # A real Bedrock/Vertex connection usually has no password either (it authenticates
        # some other way), so the conn_type check must run first: otherwise this gets the
        # less specific "no password set" message instead of naming the unsupported platform.
        ({"conn_type": "pydanticai_bedrock", "password": None}, "pydanticai_bedrock"),
        ({"extra": {"model": "openai:gpt-5"}}, "only talks to Anthropic directly"),
        ({"extra": {}}, "No model specified"),
    ],
)
@mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
def test_rejects_unsupported_connection(
    mock_get_connection: mock.MagicMock, connection_kwargs: dict[str, Any], match: str
) -> None:
    mock_get_connection.return_value = _connection(**connection_kwargs)

    with pytest.raises(ValueError, match=match):
        ClaudeAgentSDKBackend().run(HarnessRequest(prompt="go", llm_conn_id="anthropic_default"))


@mock.patch(f"{MODULE}.query", autospec=True)
@mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
def test_model_id_overrides_connection_model(
    mock_get_connection: mock.MagicMock, mock_query: mock.MagicMock
) -> None:
    mock_get_connection.return_value = _connection(extra={"model": "anthropic:claude-connection-model"})
    mock_query.side_effect = _query_side_effect(_result_message())

    ClaudeAgentSDKBackend().run(
        HarnessRequest(prompt="go", llm_conn_id="anthropic_default", model_id="claude-request-model")
    )

    assert mock_query.call_args.kwargs["options"].model == "claude-request-model"


@mock.patch(f"{MODULE}.ClaudeAgentSDKBackend", autospec=True)
def test_default_harness_is_claude_agent_sdk_backend(mock_backend_cls: mock.MagicMock) -> None:
    """``HarnessOperator(harness=None)`` lazily builds a ``ClaudeAgentSDKBackend``.

    Lives here, not in ``tests/operators/test_harness.py``: the patch target is this
    module, which raises ``AirflowOptionalProviderFeatureException`` at import time
    without the ``claude-agent-sdk`` extra, so the operator's own test file -- which
    has no reason to require that extra -- would fail to collect without it.
    """
    from airflow.providers.common.ai.operators.harness import HarnessOperator

    mock_backend_cls.return_value.run.return_value = mock.MagicMock(
        output="hi", is_error=False, session_id=None, num_turns=None, cost_usd=None, subtype=None, usage=None
    )
    op = HarnessOperator(task_id="t", prompt="go", llm_conn_id="anthropic_default")

    op.execute({"task_instance": mock.MagicMock(do_xcom_push=True)})

    mock_backend_cls.assert_called_once_with()
