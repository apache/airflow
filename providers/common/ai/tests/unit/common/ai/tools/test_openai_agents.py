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
from agents.tracing import (
    CustomSpanData,
    GenerationSpanData,
    GuardrailSpanData,
    HandoffSpanData,
    ResponseSpanData,
    custom_span,
    response_span,
    set_trace_processors,
    trace as sdk_trace,
)
from agents.tracing.processors import default_processor
from openai.types.responses import (
    Response,
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.toolsets.function import FunctionToolset

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult
from airflow.providers.common.ai.tools._from_toolset import airflow_tools_from_toolset
from airflow.providers.common.ai.tools.openai_agents import (
    AirflowTools,
    AirflowTraceProcessor,
    _describe,
    _mask_content,
)
from airflow.providers.common.ai.tools.tracing import agent_framework_tracing
from airflow.providers.common.ai.toolsets.sql import SQLToolset

from tests_common.test_utils.config import conf_vars
from unit.common.ai.tools.test_tracing import TI
from unit.common.ai.toolsets.test_sql import _make_mock_db_hook

TRACING = "airflow.providers.common.ai.tools.tracing"
MODULE = "airflow.providers.common.ai.tools.openai_agents"
_OK = ToolResult("ok")
CAPTURE_CONTENT = {("common.ai", "otel_export_enabled"): "True", ("common.ai", "capture_content"): "True"}


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


def _run(agent: Agent, *, tracing_disabled: bool = True) -> str:
    # Tracing off unless a test routes it to its own processor, so none exports spans to OpenAI.
    return Runner.run_sync(agent, "go", run_config=RunConfig(tracing_disabled=tracing_disabled)).final_output


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
    def test_a_secret_in_a_result_reaches_the_model_mask_content(self, registered_secret):
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


@pytest.fixture
def recorded():
    """Route the SDK's traces through an AirflowTraceProcessor into an in-memory exporter."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    set_trace_processors([AirflowTraceProcessor(tracer_provider=provider)])
    try:
        yield exporter, provider
    finally:
        set_trace_processors([default_processor()])


def _traced_lookup(arguments: str = '{"key": "a"}', result: ToolResult | Exception = _OK) -> None:
    """Run an agent that calls ``lookup`` once, with the SDK's tracing on."""
    agent = Agent(
        name="analyst", model=_ToolCallingModel("lookup", arguments), tools=AirflowTools(_tool(result))
    )
    _run(agent, tracing_disabled=False)


def _span(exporter: InMemorySpanExporter, name: str = "execute_tool lookup") -> ReadableSpan:
    (span,) = [span for span in exporter.get_finished_spans() if span.name == name]
    return span


class TestAirflowTraceProcessor:
    def test_a_run_becomes_a_span_tree_under_the_current_span(self, recorded):
        exporter, provider = recorded

        with provider.get_tracer("test").start_as_current_span("task"):
            _traced_lookup()

        spans = exporter.get_finished_spans()
        by_id = {span.context.span_id: span for span in spans}
        ancestors = []
        span: ReadableSpan | None = _span(exporter)
        while span is not None and span.parent is not None:
            span = by_id.get(span.parent.span_id)
            ancestors.append(span.name if span is not None else None)
        assert ancestors == [
            "openai_agents.turn 1",
            "invoke_agent analyst",
            "openai_agents.task Agent workflow",
            "Agent workflow",
            "task",
        ]
        invoke, tool = _span(exporter, "invoke_agent analyst"), _span(exporter)
        assert invoke.attributes["gen_ai.agent.name"] == "analyst"
        assert tool.attributes["gen_ai.operation.name"] == "execute_tool"
        assert tool.attributes["gen_ai.tool.name"] == "lookup"

    def test_a_span_started_inside_a_tool_call_nests_under_it(self, recorded):
        """Spans of other instrumentation, such as an HTTP client's, belong to the call that made them."""
        exporter, provider = recorded

        async def function(arguments: dict[str, Any]) -> ToolResult:
            with provider.get_tracer("http").start_as_current_span("GET"):
                return ToolResult("ok")

        tool = AirflowTool(
            name="lookup", description="Look up.", parameters={"type": "object"}, function=function
        )
        agent = Agent(name="analyst", model=_ToolCallingModel("lookup", "{}"), tools=AirflowTools(tool))

        _run(agent, tracing_disabled=False)

        request, call = _span(exporter, "GET"), _span(exporter)
        assert request.parent is not None
        assert request.parent.span_id == call.context.span_id

    @pytest.mark.parametrize(
        ("export", "capture"),
        [("False", "False"), ("True", "False"), ("False", "True")],
    )
    def test_leaves_content_out_unless_export_and_capture_are_both_on(self, recorded, export, capture):
        exporter, _ = recorded

        with conf_vars(
            {("common.ai", "otel_export_enabled"): export, ("common.ai", "capture_content"): capture}
        ):
            _traced_lookup()

        attributes = _span(exporter).attributes
        assert "gen_ai.tool.call.arguments" not in attributes
        assert "gen_ai.tool.call.result" not in attributes

    @pytest.mark.enable_redact
    @conf_vars(CAPTURE_CONTENT)
    def test_captured_content_passes_through_the_secret_masker(self, recorded, registered_secret):
        exporter, _ = recorded

        _traced_lookup(json.dumps({"key": registered_secret}), ToolResult(f"seen {registered_secret}"))

        attributes = _span(exporter).attributes
        assert attributes["gen_ai.tool.call.arguments"] == json.dumps({"key": "***"})
        assert attributes["gen_ai.tool.call.result"] == "seen ***"

    def test_spans_carry_the_task_whichever_provider_records_them(self, recorded):
        """The processor's own provider gets no identity processor from agent_framework_tracing."""
        exporter, _ = recorded

        with (
            patch(f"{TRACING}._live_tracer_provider", autospec=True, return_value=None),
            patch(f"{TRACING}.get_current_context", autospec=True, return_value={"ti": TI}),
            agent_framework_tracing(),
        ):
            _traced_lookup()

        spans = exporter.get_finished_spans()
        assert spans
        assert {span.attributes["airflow.dag_id"] for span in spans} == {TI.dag_id}
        assert {span.attributes["airflow.task_id"] for span in spans} == {TI.task_id}

    @pytest.mark.parametrize(
        ("settings", "description"),
        [
            pytest.param({}, None, id="content-off"),
            pytest.param(CAPTURE_CONTENT, "Error running tool", id="content-on"),
        ],
    )
    def test_a_failed_tool_call_marks_its_span_as_an_error(self, recorded, settings, description):
        """The SDK's error message can be app-supplied text, so it follows the content rule."""
        exporter, _ = recorded

        with conf_vars(settings), pytest.raises(ToolCallError):
            _traced_lookup(result=PermissionError("role cannot read orders"))

        status = _span(exporter).status
        assert status.status_code == StatusCode.ERROR
        assert status.description == description

    def test_a_response_span_is_named_for_its_model_once_the_response_arrives(self, recorded):
        exporter, _ = recorded

        with sdk_trace("workflow"), response_span() as span:
            span.span_data.response = Response.model_construct(id="resp_1", model="gpt-5.6-luna", output=[])
            span.span_data.usage = {"input_tokens": 12, "output_tokens": 3}

        chat = _span(exporter, "chat gpt-5.6-luna")
        assert chat.kind == SpanKind.CLIENT
        assert chat.attributes["gen_ai.provider.name"] == "openai"
        assert chat.attributes["gen_ai.usage.input_tokens"] == 12
        assert chat.attributes["gen_ai.usage.output_tokens"] == 3

    def test_a_span_the_sdk_never_finished_ends_with_its_trace(self, recorded):
        """A run that is cancelled or raises can leave a span unfinished."""
        exporter, _ = recorded

        with sdk_trace("workflow"):
            custom_span("left open").start()

        assert _span(exporter, "openai_agents.custom").end_time is not None

    def test_a_span_ends_even_when_describing_it_fails(self, recorded):
        exporter, _ = recorded

        with patch(
            f"{MODULE}._content_capture_enabled", autospec=True, side_effect=RuntimeError("bad config")
        ):
            _traced_lookup()

        assert _span(exporter).end_time is not None


class TestDescribe:
    def test_a_generation_span_names_the_server_it_called(self):
        """The Chat Completions client can point at any compatible server, so no provider is assumed."""
        data = GenerationSpanData(model="m", model_config={"base_url": "https://llm.example.com/v1/"})

        attributes = _describe(data, capture=False)[1]

        assert attributes["server.address"] == "llm.example.com"
        assert "gen_ai.provider.name" not in attributes

    def test_a_response_output_is_not_serialized_unless_captured(self):
        class _Unserializable:
            def model_dump(self, **kwargs: Any) -> Any:
                raise AssertionError("serialized although content capture is off")

        data = ResponseSpanData(
            response=Response.model_construct(id="r", model="m", output=[_Unserializable()])
        )

        assert "gen_ai.output.messages" not in _describe(data, capture=False)[1]

    @pytest.mark.parametrize(
        ("data", "name", "attributes"),
        [
            pytest.param(
                HandoffSpanData(from_agent="triage", to_agent="billing"),
                "openai_agents.handoff",
                {"openai_agents.handoff.from_agent": "triage", "openai_agents.handoff.to_agent": "billing"},
                id="handoff",
            ),
            pytest.param(
                GuardrailSpanData(name="no_pii", triggered=True),
                "openai_agents.guardrail no_pii",
                {"openai_agents.guardrail.triggered": True},
                id="guardrail",
            ),
            pytest.param(
                CustomSpanData(name="lookup cache", data={}), "openai_agents.custom", {}, id="other"
            ),
        ],
    )
    def test_names_the_sdks_other_spans_for_their_kind(self, data, name, attributes):
        assert _describe(data, capture=False) == (name, attributes)

    def test_a_generation_span_carries_its_model_and_token_counts(self):
        data = GenerationSpanData(model="gpt-5.6-luna", usage={"input_tokens": 12, "output_tokens": 3})

        assert _describe(data, capture=False) == (
            "chat gpt-5.6-luna",
            {
                "gen_ai.operation.name": "chat",
                "gen_ai.request.model": "gpt-5.6-luna",
                "gen_ai.usage.input_tokens": 12,
                "gen_ai.usage.output_tokens": 3,
            },
        )

    def test_a_response_span_carries_the_response_model_and_id(self):
        response = Response.model_construct(id="resp_1", model="gpt-5.6-luna", output=[])
        data = ResponseSpanData(response=response, usage={"input_tokens": 12, "output_tokens": 3})

        name, attributes = _describe(data, capture=False)

        assert name == "chat gpt-5.6-luna"
        assert attributes == {
            "gen_ai.operation.name": "chat",
            "gen_ai.provider.name": "openai",
            "gen_ai.response.id": "resp_1",
            "gen_ai.response.model": "gpt-5.6-luna",
            "gen_ai.usage.input_tokens": 12,
            "gen_ai.usage.output_tokens": 3,
        }

    def test_captures_messages_only_when_asked(self):
        data = GenerationSpanData(model="m", input=[{"role": "user", "content": "hi"}], output=[])

        assert "gen_ai.input.messages" not in _describe(data, capture=False)[1]
        assert _describe(data, capture=True)[1]["gen_ai.input.messages"] == json.dumps(
            [{"role": "user", "content": "hi"}]
        )

    def test_captures_the_completion_of_a_response(self):
        text = ResponseOutputText(text="42 orders", type="output_text", annotations=[])
        message = ResponseOutputMessage(
            id="msg", role="assistant", status="completed", type="message", content=[text]
        )
        data = ResponseSpanData(
            response=Response.model_construct(id="resp_1", model="m", output=[message]),
            input=[{"role": "user", "content": "How many orders?"}],
        )

        attributes = _describe(data, capture=True)[1]

        assert json.loads(attributes["gen_ai.output.messages"])[0]["content"][0]["text"] == "42 orders"
        assert json.loads(attributes["gen_ai.input.messages"]) == [
            {"role": "user", "content": "How many orders?"}
        ]

    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(
                lambda secret: GenerationSpanData(
                    model="m", input=[{"role": "user", "content": f"pw={secret}"}]
                ),
                id="generation",
            ),
            pytest.param(
                lambda secret: ResponseSpanData(input=[{"role": "user", "content": f"pw={secret}"}]),
                id="response",
            ),
        ],
    )
    def test_masks_a_secret_in_captured_messages(self, registered_secret, data):
        attributes = _describe(data(registered_secret), capture=True)[1]

        assert registered_secret not in attributes["gen_ai.input.messages"]
        assert "pw=***" in attributes["gen_ai.input.messages"]


class TestMasked:
    @pytest.mark.enable_redact
    def test_masks_a_secret_that_json_escapes_inside_json_text(self, register_secret):
        """A quote in the secret is escaped inside the model's JSON arguments, so the raw text does not match."""
        secret = register_secret('pa"ss-91c3')

        masked = _mask_content(json.dumps({"password": secret}))

        assert json.loads(masked) == {"password": "***"}

    @pytest.mark.enable_redact
    @pytest.mark.parametrize("text", ["{secret}", '"{secret}"'], ids=["number", "json-string"])
    def test_masks_a_secret_that_is_the_whole_result(self, register_secret, text):
        """A bare number or string is valid JSON too; it must not escape the masker by parsing."""
        secret = register_secret("918273645")

        masked = _mask_content(text.format(secret=secret))

        assert secret not in masked

    @pytest.mark.enable_redact
    def test_masks_plain_text(self, registered_secret):
        assert _mask_content(f"connected with {registered_secret}") == "connected with ***"
