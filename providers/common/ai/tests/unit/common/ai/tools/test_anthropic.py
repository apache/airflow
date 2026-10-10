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
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

pytest.importorskip("anthropic")

import httpx2
from anthropic import Anthropic, AsyncAnthropic, DefaultAsyncHttpxClient, DefaultHttpxClient
from anthropic.types.beta import BetaTextBlock
from pydantic_ai.exceptions import ModelRetry

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult
from airflow.providers.common.ai.tools.anthropic import AirflowTools, AsyncAirflowTools
from airflow.providers.common.ai.toolsets.sql import SQLToolset

from unit.common.ai.tools.test__from_toolset import _scripted
from unit.common.ai.toolsets.test_sql import _make_mock_db_hook

if TYPE_CHECKING:
    from anthropic.lib.tools import BetaToolRunner
    from anthropic.types.beta.parsed_beta_message import ParsedBetaMessage


class _ScriptedModel:
    """
    Stands in for the Messages API: asks for ``tool_name`` until it has ``calls`` results,
    then answers with the last result it got. Keeps every request body it receives.

    With a ``stop_reason`` other than ``tool_use``, the first turn asks for the tool but ends
    with that reason, and any later turn answers without asking again. ``per_turn`` is how
    many calls each asking turn makes at once.
    """

    def __init__(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        *,
        calls: int = 1,
        stop_reason: str = "tool_use",
        per_turn: int = 1,
    ) -> None:
        self.tool_name = tool_name
        self.tool_input = tool_input
        self.calls = calls
        self.stop_reason = stop_reason
        self.per_turn = per_turn
        self.requests: list[dict[str, Any]] = []
        # Unique across instances, since the retry limit tells turns apart by message id.
        self._prefix = uuid4().hex[:8]

    def results(self) -> list[dict[str, Any]]:
        """Every ``tool_result`` block sent to the model, in order."""
        messages = self.requests[-1]["messages"] if self.requests else []
        return [
            block
            for message in messages
            if message["role"] == "user" and isinstance(message["content"], list)
            for block in message["content"]
            if block.get("type") == "tool_result"
        ]

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(json.loads(request.content))
        turn = len(self.requests)
        results = self.results()
        if len(results) < self.calls and (turn == 1 or self.stop_reason == "tool_use"):
            content = [
                {
                    "type": "tool_use",
                    "id": f"toolu_{self._prefix}_{turn}_{i}",
                    "name": self.tool_name,
                    "input": self.tool_input,
                }
                for i in range(self.per_turn)
            ]
            stop_reason = self.stop_reason
        else:
            content = [{"type": "text", "text": json.dumps(results[-1] if results else None)}]
            stop_reason = "end_turn"
        return httpx2.Response(
            200,
            json={
                "id": f"msg_{self._prefix}_{turn}",
                "type": "message",
                "role": "assistant",
                "model": "claude-test",
                "content": content,
                "stop_reason": stop_reason,
                "stop_sequence": None,
                "usage": {"input_tokens": 1, "output_tokens": 1},
            },
        )


def _sync_runner(model: _ScriptedModel, tools: AirflowTools) -> BetaToolRunner:
    client = Anthropic(api_key="test", http_client=DefaultHttpxClient(transport=httpx2.MockTransport(model)))
    return client.beta.messages.tool_runner(
        model="claude-test", max_tokens=100, tools=tools.tools, messages=[{"role": "user", "content": "go"}]
    )


def _drive_sync(model: _ScriptedModel, tools: AirflowTools) -> ParsedBetaMessage:
    return tools.run(_sync_runner(model, tools))


def _drive_async(model: _ScriptedModel, tools: AsyncAirflowTools) -> ParsedBetaMessage:
    async def run() -> ParsedBetaMessage:
        client = AsyncAnthropic(
            api_key="test", http_client=DefaultAsyncHttpxClient(transport=httpx2.MockTransport(model))
        )
        runner = client.beta.messages.tool_runner(
            model="claude-test",
            max_tokens=100,
            tools=tools.tools,
            messages=[{"role": "user", "content": "go"}],
        )
        return await tools.run(runner)

    return asyncio.run(run())


def _answer(message: ParsedBetaMessage) -> Any:
    """The tool result the scripted model echoed back as its answer."""
    block = message.content[0]
    assert isinstance(block, BetaTextBlock)
    return json.loads(block.text)


@pytest.fixture(
    params=[
        pytest.param((AirflowTools, _drive_sync), id="sync"),
        pytest.param((AsyncAirflowTools, _drive_async), id="async"),
    ]
)
def flavour(request):
    """The adapter class and the function that drives a runner with it."""
    return request.param


@pytest.fixture
def run_agent(flavour):
    tools_class, drive = flavour

    def run(model: _ScriptedModel, *sources) -> ParsedBetaMessage:
        return drive(model, tools_class(*sources))

    return run


def _tool(result: ToolResult | Exception, ran: list[dict[str, Any]] | None = None) -> AirflowTool:
    """A ``lookup`` tool that returns or raises ``result``, recording its calls in ``ran``."""

    async def function(arguments: dict[str, Any]) -> ToolResult:
        if ran is not None:
            ran.append(arguments)
        if isinstance(result, Exception):
            raise result
        return result

    return AirflowTool(
        name="lookup",
        description="Look something up.",
        parameters={"type": "object", "properties": {"key": {"type": "string"}}, "required": ["key"]},
        function=function,
    )


@pytest.mark.parametrize("tools_class", [AirflowTools, AsyncAirflowTools])
def test_each_toolset_tool_becomes_a_runner_tool_with_its_schema(tools_class):
    tools = tools_class(_tool(ToolResult("ok")), SQLToolset("pg_default"))

    definitions = [tool.to_dict() for tool in tools.tools]

    assert [d["name"] for d in definitions] == ["lookup", "list_tables", "get_schema", "query", "check_query"]
    assert definitions[3]["input_schema"]["required"] == ["sql"]


@pytest.mark.parametrize("tools_class", [AirflowTools, AsyncAirflowTools])
def test_a_runner_tool_does_not_share_the_source_schema(tools_class):
    """A toolset's schema can be a module-level constant that the pydantic-ai path also uses."""
    source = _tool(ToolResult("ok"))
    before = copy.deepcopy(source.parameters)

    tools_class(source).tools[0].to_dict()["input_schema"]["properties"]["key"]["type"] = "integer"

    assert source.parameters == before


class TestRun:
    def test_the_model_reads_a_toolset_result(self, run_agent):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Ada")], last_description=[("id",), ("name",)])
        model = _ScriptedModel("query", {"sql": "SELECT id, name FROM users"})

        result = _answer(run_agent(model, ts))

        assert "is_error" not in result
        assert json.loads(result["content"])["rows"] == [[1, "Ada"]]

    def test_a_correctable_failure_reaches_the_model_as_an_error(self, run_agent):
        model = _ScriptedModel("lookup", {"key": "a"})

        result = _answer(run_agent(model, _tool(ToolResult("Unknown column 'nme'", is_error=True))))

        assert result["is_error"] is True
        assert "Unknown column 'nme'" in json.dumps(result["content"])

    def test_any_other_failure_fails_the_run_before_the_next_model_request(self, run_agent, caplog):
        """The runner alone would hand the exception to the model and ask it again."""
        model = _ScriptedModel("lookup", {"key": "a"})

        with pytest.raises(ToolCallError, match="PermissionError: role cannot read orders"):
            run_agent(model, _tool(PermissionError("role cannot read orders")))

        assert len(model.requests) == 1
        # The runner logs the exceptions it catches; this one reaches the task log once, as the failure.
        assert "Error occurred while calling tool" not in caplog.text

    def test_a_failure_goes_to_the_model_with_a_warning_when_run_is_not_used(self, caplog):
        model = _ScriptedModel("lookup", {"key": "a"})
        runner = _sync_runner(model, AirflowTools(_tool(PermissionError("role cannot read orders"))))

        runner.until_done()

        assert model.results()[0]["is_error"] is True
        assert "not driven by AirflowTools.run" in caplog.text

    def test_a_tool_that_keeps_failing_fails_the_run_once_its_retries_are_spent(self, run_agent):
        model = _ScriptedModel("step", {}, calls=5)
        step = _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1)

        with pytest.raises(ToolCallError, match="kept failing after 1 correction"):
            run_agent(model, step)

        # One failure the model may correct, then the second ends the run.
        assert len(model.requests) == 2

    @pytest.mark.parametrize(
        ("stop_reason", "model_requests"),
        [
            pytest.param("max_tokens", 1, id="cut-off-turn-ends-the-run"),
            pytest.param("pause_turn", 2, id="paused-turn-is-resumed"),
        ],
    )
    def test_a_turn_that_did_not_end_in_tool_use_runs_no_tools(self, run_agent, stop_reason, model_requests):
        """The runner itself only runs a turn's tool calls when it ended in ``tool_use``."""
        ran: list[dict[str, Any]] = []
        model = _ScriptedModel("lookup", {"key": "a"}, stop_reason=stop_reason)

        run_agent(model, _tool(ToolResult("ran"), ran))

        assert ran == []
        assert len(model.requests) == model_requests

    def test_calls_after_a_failure_in_the_same_turn_do_not_run(self, run_agent):
        """The turn's results are never sent, so a later call's side effects would be for nothing."""
        ran: list[dict[str, Any]] = []
        model = _ScriptedModel("lookup", {"key": "a"}, per_turn=3)

        with pytest.raises(ToolCallError, match="PermissionError"):
            run_agent(model, _tool(PermissionError("role cannot read orders"), ran))

        assert len(ran) == 1

    def test_the_sync_tools_fail_the_run_when_called_under_a_running_event_loop(self):
        """The sync tools run on a worker thread there, which must still see the run's state."""
        model = _ScriptedModel("lookup", {"key": "a"})

        async def caller() -> None:
            _drive_sync(model, AirflowTools(_tool(PermissionError("role cannot read orders"))))

        with pytest.raises(ToolCallError, match="PermissionError"):
            asyncio.run(caller())

        assert len(model.requests) == 1

    def test_a_structured_result_reaches_the_model_as_json(self, run_agent):
        model = _ScriptedModel("lookup", {"key": "a"})

        result = _answer(run_agent(model, _tool(ToolResult({"rows": [[1, None]]}))))

        assert json.loads(result["content"]) == {"rows": [[1, None]]}

    def test_failed_calls_in_one_turn_count_once_against_the_retry_limit(self, run_agent):
        model = _ScriptedModel("step", {}, calls=2, per_turn=2)

        run_agent(model, _scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1))

        assert [r.get("is_error") for r in model.results()] == [True, True]

    def test_each_run_of_a_reused_instance_starts_with_a_fresh_retry_budget(self, flavour):
        tools_class, drive = flavour
        tools = tools_class(_scripted(ModelRetry("no such key"), ModelRetry("no such key"), max_retries=1))

        first, second = _ScriptedModel("step", {}), _ScriptedModel("step", {})
        drive(first, tools)
        drive(second, tools)

        assert second.results()[0]["is_error"] is True

    @pytest.mark.enable_redact
    def test_a_secret_in_a_result_reaches_the_model_masked(self, run_agent, registered_secret):
        model = _ScriptedModel("lookup", {"key": "a"})

        run_agent(model, _tool(ToolResult(f"key={registered_secret}")))

        sent = json.dumps(model.requests)
        assert registered_secret not in sent
        assert "key=***" in sent

    def test_the_toolsets_calls_are_counted_as_anthropic(self, run_agent):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        with patch("airflow.providers.common.ai.utils.tool_metrics.Stats", MagicMock(spec=["incr"])) as stats:
            run_agent(_ScriptedModel("list_tables", {}), ts)

        assert stats.incr.call_args.kwargs["tags"]["framework"] == "anthropic"
