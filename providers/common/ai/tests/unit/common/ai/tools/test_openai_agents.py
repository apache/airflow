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

import copy
import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("agents")

from agents import Agent, FunctionTool, Model, ModelResponse, RunConfig, Runner, Tool, Usage
from openai.types.responses import ResponseFunctionToolCall, ResponseOutputMessage, ResponseOutputText
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.toolsets.function import FunctionToolset

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult
from airflow.providers.common.ai.tools._from_toolset import airflow_tools_from_toolset
from airflow.providers.common.ai.tools.openai_agents import AirflowTools
from airflow.providers.common.ai.toolsets.sql import SQLToolset

from unit.common.ai.toolsets.test_sql import _make_mock_db_hook


class _ToolCallingModel(Model):
    """
    Calls ``tool_name`` until it has had ``attempts`` results back, then answers with the last one.

    ``arguments`` is sent as the model would send it: a JSON string. Each turn makes
    ``calls_per_turn`` calls at once.
    """

    def __init__(self, tool_name: str, arguments: str, *, attempts: int = 1, calls_per_turn: int = 1) -> None:
        self._tool_name = tool_name
        self._arguments = arguments
        self._attempts = attempts
        self._calls_per_turn = calls_per_turn

    async def get_response(self, system_instructions, input, *args: Any, **kwargs: Any) -> ModelResponse:
        outputs = [
            item["output"]
            for item in (input if isinstance(input, list) else [])
            if isinstance(item, dict) and item.get("type") == "function_call_output"
        ]
        output: list[ResponseFunctionToolCall | ResponseOutputMessage]
        if len(outputs) < self._attempts:
            output = [
                ResponseFunctionToolCall(
                    id=f"fc-{len(outputs) + i}",
                    call_id=f"call-{len(outputs) + i}",
                    name=self._tool_name,
                    arguments=self._arguments,
                    type="function_call",
                )
                for i in range(self._calls_per_turn)
            ]
        else:
            text = ResponseOutputText(text=outputs[-1], type="output_text", annotations=[])
            output = [
                ResponseOutputMessage(
                    id="msg", role="assistant", status="completed", type="message", content=[text]
                )
            ]
        return ModelResponse(output=list(output), usage=Usage(), response_id=None)

    def stream_response(self, *args: Any, **kwargs: Any):
        raise NotImplementedError


def _tool(result: ToolResult | Exception) -> AirflowTool:
    async def function(arguments: dict[str, Any]) -> ToolResult:
        if isinstance(result, Exception):
            raise result
        return result

    return AirflowTool(
        name="lookup",
        description="Look something up.",
        parameters={"type": "object", "properties": {"key": {"type": "string"}}},
        function=function,
    )


def _function_tools(*sources: AirflowTool | SQLToolset) -> list[FunctionTool]:
    tools = []
    for tool in AirflowTools(*sources):
        assert isinstance(tool, FunctionTool)
        tools.append(tool)
    return tools


def _run(agent: Agent) -> str:
    # Tracing off, so no test tries to export spans to OpenAI.
    return Runner.run_sync(agent, "go", run_config=RunConfig(tracing_disabled=True)).final_output


def _run_agent(tools: list[Tool], tool_name: str, arguments: dict[str, Any] | str) -> str:
    if not isinstance(arguments, str):
        arguments = json.dumps(arguments)
    return _run(Agent(name="analyst", model=_ToolCallingModel(tool_name, arguments), tools=tools))


def _always_retrying_tools() -> AirflowTools:
    def lookup(key: str) -> str:
        """Look a key up."""
        raise ModelRetry("no such key")

    return AirflowTools(*airflow_tools_from_toolset(FunctionToolset([lookup], max_retries=1)))


class TestAirflowTools:
    def test_holds_a_function_tool_for_every_tool_of_a_toolset(self):
        tools = _function_tools(SQLToolset("pg_default"))

        assert [t.name for t in tools] == ["list_tables", "get_schema", "query", "check_query"]
        query = next(t for t in tools if t.name == "query")
        assert query.params_json_schema["required"] == ["sql"]

    def test_keeps_the_source_schema_out_of_strict_mode(self):
        """Strict mode would make every argument required, so the schema goes over unchanged."""
        source = _tool(ToolResult("ok"))

        (tool,) = _function_tools(source)

        assert tool.strict_json_schema is False
        assert tool.params_json_schema == source.parameters

    def test_accepts_individual_tools_alongside_toolsets(self):
        tools = AirflowTools(_tool(ToolResult("a")), SQLToolset("pg_default"))

        assert [t.name for t in tools][:2] == ["lookup", "list_tables"]

    def test_the_tool_does_not_share_the_source_schema(self):
        source = _tool(ToolResult("ok"))
        before = copy.deepcopy(source.parameters)

        (tool,) = _function_tools(source)
        tool.params_json_schema["properties"]["key"]["description"] = "changed"

        assert source.parameters == before


class TestAgentRun:
    def test_the_agent_reads_a_toolset_result(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Ada")], last_description=[("id",), ("name",)])

        answer = _run_agent(AirflowTools(ts), "query", {"sql": "SELECT id, name FROM users"})

        assert json.loads(answer)["rows"] == [[1, "Ada"]]

    def test_a_correctable_failure_reaches_the_model_as_an_error(self):
        tools = AirflowTools(_tool(ToolResult("Unknown column 'nme'", is_error=True)))

        assert json.loads(_run_agent(tools, "lookup", {"key": "a"})) == {"error": "Unknown column 'nme'"}

    @pytest.mark.parametrize(
        ("arguments", "error"),
        [
            pytest.param('{"key": ', "The arguments are not valid JSON", id="malformed"),
            pytest.param('["a"]', "The arguments must be a JSON object.", id="not-an-object"),
        ],
    )
    def test_arguments_that_are_not_a_json_object_reach_the_model_as_an_error(self, arguments, error):
        tools = AirflowTools(_tool(ToolResult("ok")))

        answer = json.loads(_run_agent(tools, "lookup", arguments))

        assert answer["error"].startswith(error)

    def test_empty_arguments_call_the_tool_with_no_arguments(self):
        """A model calling a tool that takes no arguments may send an empty string."""
        received = []

        async def function(arguments: dict[str, Any]) -> ToolResult:
            received.append(arguments)
            return ToolResult("ok")

        tool = AirflowTool(name="ping", description="Ping.", parameters={"type": "object"}, function=function)

        assert _run_agent(AirflowTools(tool), "ping", "") == "ok"
        assert received == [{}]

    def test_any_other_failure_fails_the_run_with_the_tool_call_error(self):
        """The runner wraps a tool's exception in a UserError unless it is one of its own types."""
        tools = AirflowTools(_tool(PermissionError("role cannot read orders")))

        with pytest.raises(ToolCallError, match="PermissionError: role cannot read orders"):
            _run_agent(tools, "lookup", {"key": "a"})

    def test_a_failure_inside_an_agent_used_as_a_tool_fails_the_outer_run(self):
        inner = Agent(
            name="inner",
            model=_ToolCallingModel("lookup", '{"key": "a"}'),
            tools=AirflowTools(_tool(PermissionError("role cannot read orders"))),
        )
        outer = Agent(
            name="outer",
            model=_ToolCallingModel("ask_inner", '{"input": "go"}'),
            tools=[inner.as_tool("ask_inner", "Ask the inner agent.", failure_error_function=None)],
        )

        with pytest.raises(ToolCallError, match="role cannot read orders"):
            _run(outer)

    def test_a_tool_still_failing_once_its_retries_are_spent_fails_the_run(self):
        agent = Agent(
            name="analyst",
            model=_ToolCallingModel("lookup", '{"key": "a"}', attempts=3),
            tools=_always_retrying_tools(),
        )

        with pytest.raises(ToolCallError, match="lookup kept failing after 1 correction"):
            _run(agent)

    def test_failed_calls_in_one_turn_count_once_against_the_retry_limit(self):
        """With one correction allowed, two failures in the same turn must not use up the budget."""
        agent = Agent(
            name="analyst",
            model=_ToolCallingModel("lookup", '{"key": "a"}', attempts=2, calls_per_turn=2),
            tools=_always_retrying_tools(),
        )

        assert json.loads(_run(agent)) == {"error": "no such key"}

    def test_each_run_of_a_reused_agent_starts_with_a_fresh_retry_budget(self):
        agent = Agent(
            name="analyst", model=_ToolCallingModel("lookup", '{"key": "a"}'), tools=_always_retrying_tools()
        )

        assert json.loads(_run(agent)) == {"error": "no such key"}
        assert json.loads(_run(agent)) == {"error": "no such key"}

    @pytest.mark.enable_redact
    def test_a_secret_in_a_result_reaches_the_model_masked(self, registered_secret):
        answer = _run_agent(
            AirflowTools(_tool(ToolResult(f"key={registered_secret}"))), "lookup", {"key": "a"}
        )

        assert registered_secret not in answer
        assert answer == "key=***"

    def test_the_toolsets_calls_are_counted_as_openai_agents(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        with patch("airflow.providers.common.ai.utils.tool_metrics.Stats", MagicMock(spec=["incr"])) as stats:
            _run_agent(AirflowTools(ts), "list_tables", {})

        assert stats.incr.call_args.kwargs["tags"]["framework"] == "openai_agents"
