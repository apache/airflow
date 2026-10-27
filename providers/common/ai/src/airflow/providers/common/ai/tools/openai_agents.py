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
Give an OpenAI Agents SDK agent Airflow's tools, and record its traces as the task's spans.

See the `OpenAI Agents SDK <https://openai.github.io/openai-agents-python/>`__ documentation.

.. note:: Experimental; see :ref:`howto/stability`.
"""

from __future__ import annotations

import copy
import json
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from opentelemetry import context as otel_context, trace as otel_trace
from opentelemetry.trace import SpanKind, Status, StatusCode

try:
    from agents import FunctionTool, Tool
    from agents.exceptions import AgentsException
    from agents.tracing import (
        AgentSpanData,
        FunctionSpanData,
        GenerationSpanData,
        GuardrailSpanData,
        HandoffSpanData,
        ResponseSpanData,
        TaskSpanData,
        TracingProcessor,
        TurnSpanData,
    )
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.observability import _content_capture_enabled
from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, collect_tools
from airflow.providers.common.ai.tools._from_toolset import tool_call_scope
from airflow.providers.common.ai.tools.tracing import _task_identity
from airflow.providers.common.ai.utils.masking import dumps_masked, mask_secrets
from airflow.providers.common.ai.utils.tool_definition import serialize_for_llm
from airflow.providers.common.ai.utils.tool_metrics import calling_framework

if TYPE_CHECKING:
    from contextvars import Token

    from agents.tool_context import ToolContext
    from agents.tracing import Span, SpanData, Trace
    from opentelemetry.context import Context
    from opentelemetry.trace import Span as OtelSpan, TracerProvider

    from airflow.providers.common.ai.tools import ToolProvider

__all__ = ["AirflowTools", "AirflowTraceProcessor"]


class AirflowTools(list[Tool]):
    """
    Airflow's tools as OpenAI Agents SDK function tools, in a list to give an ``Agent``.

    Pass toolsets that implement
    :class:`~airflow.providers.common.ai.tools.ToolProvider`, such as ``SQLToolset``
    and ``HookToolset``, or individual
    :class:`~airflow.providers.common.ai.tools.AirflowTool` objects. The agent, its
    model and its runner stay plain OpenAI Agents SDK.

    .. code-block:: python

        from agents import Agent, Runner

        from airflow.providers.common.ai.tools.openai_agents import AirflowTools
        from airflow.providers.common.ai.toolsets import SQLToolset

        warehouse = SQLToolset("warehouse", allowed_tables=["orders"])
        agent = Agent(name="analyst", model=model, tools=AirflowTools(warehouse))
        answer = Runner.run_sync(agent, question).final_output

    It is a list of ``FunctionTool`` objects typed as ``Agent.tools`` is, so add your own
    tools with ``[*AirflowTools(warehouse), my_tool]``.

    Each tool keeps the source tool's name, description and argument schema, and every
    result passes through Airflow's secret masker first. The schema is passed as it is,
    with ``strict_json_schema=False``: the SDK's strict mode would make every argument
    required. A failure the model can correct, such as a query naming a missing column,
    reaches it as ``{"error": ...}``, counted against the tool's retry limit afresh on
    every run of the agent. Arguments that are not a JSON object also come back as
    ``{"error": ...}``, bounded by the run's ``max_turns`` rather than that limit. A tool
    still failing once the limit is used up, or any other failure, raises
    :class:`~airflow.providers.common.ai.tools.ToolCallError` out of ``Runner.run``, so
    the task fails and Airflow retries it. Tools a toolset marks sequential, such as the
    sandbox's, run one at a time in the order the model called them.

    An agent holding these tools that another agent calls through ``Agent.as_tool()`` must
    be given ``failure_error_function=None``: by default ``as_tool`` hands any exception
    from the inner run to the outer model as text, so the task would carry on.

    :param sources: Toolsets and tools to add, in order.
    """

    def __init__(self, *sources: ToolProvider | AirflowTool) -> None:
        super().__init__(_to_function_tool(tool) for tool in collect_tools(sources))


class _FailedToolCall(ToolCallError, AgentsException):
    """
    A :class:`ToolCallError` the runner lets through as it is.

    The runner wraps any other exception a tool raises in a ``UserError``; one of its own
    exception types it raises unchanged, so the task fails with the tool's error.
    """


class _SameRun:
    """
    The run a tool call belongs to, compared by identity.

    The runner gives every run a ``Usage`` of its own and hands that same object to each of
    the run's tool calls. ``Usage`` compares by value, so two runs that have used the same
    tokens would compare equal; this compares the object itself.
    """

    __slots__ = ("_usage",)

    def __init__(self, usage: object) -> None:
        self._usage = usage

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _SameRun) and other._usage is self._usage

    def __hash__(self) -> int:
        return id(self._usage)


def _to_function_tool(tool: AirflowTool) -> FunctionTool:
    async def call_airflow_tool(ctx: ToolContext[Any], arguments: str) -> str:
        # The SDK passes the model's arguments as it sent them, without parsing them first.
        try:
            parsed = json.loads(arguments) if arguments else {}
        except json.JSONDecodeError as e:
            return serialize_for_llm({"error": f"The arguments are not valid JSON: {e}"})
        if not isinstance(parsed, dict):
            return serialize_for_llm({"error": "The arguments must be a JSON object."})
        # The SDK does not identify the model turn, so calls that run at the same time count once.
        try:
            with calling_framework("openai_agents"), tool_call_scope(run=_SameRun(ctx.usage)):
                result = await tool.call(parsed)
        except ToolCallError as e:
            # Already masked, and logged to the task log with its traceback by AirflowTool.call.
            raise _FailedToolCall(str(e)) from None
        if result.is_error:
            return serialize_for_llm({"error": result.content})
        return serialize_for_llm(result.content)

    return FunctionTool(
        name=tool.name,
        description=tool.description,
        # The source schema can be a toolset's module-level constant, so hand over a copy.
        params_json_schema=copy.deepcopy(tool.parameters),
        on_invoke_tool=call_airflow_tool,
        strict_json_schema=False,
    )


class AirflowTraceProcessor(TracingProcessor):
    """
    An OpenAI Agents SDK trace processor that records the SDK's traces as OpenTelemetry spans.

    The SDK has a tracing system of its own rather than OpenTelemetry. Register this
    processor with it and every trace becomes a tree of OpenTelemetry spans under the span
    that was current when the trace started, so a run inside a task nests under the task's
    span:

    .. code-block:: python

        from agents import Runner, set_trace_processors

        from airflow.providers.common.ai.tools.openai_agents import AirflowTraceProcessor
        from airflow.providers.common.ai.tools.tracing import agent_framework_tracing

        set_trace_processors([AirflowTraceProcessor()])
        with agent_framework_tracing():
            answer = Runner.run_sync(agent, question).final_output

    ``set_trace_processors`` replaces every processor registered with the SDK, including
    its default one, which sends every trace to OpenAI. To keep that export, or a processor
    another tracing library registered, add this one with ``add_trace_processor`` instead.
    Run the agent inside
    :func:`~airflow.providers.common.ai.tools.tracing.agent_framework_tracing` so the spans
    carry the task's identity. While an SDK span runs it is the current span, so spans
    other instrumentation starts inside a tool or model call, such as an HTTP request,
    nest under it.

    Spans use the names of the OpenTelemetry GenAI conventions: ``invoke_agent`` for an
    agent, ``chat`` for a model call with its model and token counts, and ``execute_tool``
    for a tool call. The SDK's other spans, such as one per ``Runner.run`` and one per turn
    of the agent loop, are named ``openai_agents.<kind>``. Prompts, completions, tool
    arguments and results, and the SDK's error messages are recorded only when
    ``[common.ai] otel_export_enabled`` and ``capture_content`` are both on, and they pass
    through Airflow's secret masker first. The SDK records that content only while
    ``RunConfig(trace_include_sensitive_data=True)``, its default; with it off, a model
    call through the Responses API also carries no model name, since the SDK withholds the
    whole response.

    :param tracer_provider: The OpenTelemetry tracer provider to record spans with. By
        default, the global one, which core tracing sets up on the worker.
    """

    def __init__(self, tracer_provider: TracerProvider | None = None) -> None:
        self._tracer = otel_trace.get_tracer(__name__, tracer_provider=tracer_provider)
        # The open OpenTelemetry span for each SDK trace and span, by the SDK's ID, the token
        # that made each SDK span current, and the trace each span belongs to. Each access is
        # one dict operation, which is safe across the threads the SDK may call from.
        self._open: dict[str, OtelSpan] = {}
        self._made_current: dict[str, Token[Context]] = {}
        self._trace_of: dict[str, str] = {}

    def on_trace_start(self, trace: Trace) -> None:
        # Started as a child of whatever span is current, the task's span inside a task.
        self._open[trace.trace_id] = self._start(trace.name, {"openai_agents.trace_id": trace.trace_id})

    def on_trace_end(self, trace: Trace) -> None:
        # A span the SDK never finished, such as one of a run that was cancelled, ends with its trace.
        for span_id in [
            span_id for span_id, trace_id in self._trace_of.items() if trace_id == trace.trace_id
        ]:
            self._trace_of.pop(span_id, None)
            # Its token belongs to a context that has gone, so it is dropped rather than detached.
            self._made_current.pop(span_id, None)
            if (leftover := self._open.pop(span_id, None)) is not None:
                leftover.end()
        if (otel_span := self._open.pop(trace.trace_id, None)) is not None:
            otel_span.end()

    def on_span_start(self, span: Span[Any]) -> None:
        parent = self._open.get(span.parent_id or span.trace_id)
        context = otel_trace.set_span_in_context(parent) if parent is not None else None
        name, _ = _describe(span.span_data, capture=False)
        # A model call is a request to a service outside the task, as the GenAI conventions model it.
        kind = (
            SpanKind.CLIENT
            if isinstance(span.span_data, GenerationSpanData | ResponseSpanData)
            else SpanKind.INTERNAL
        )
        otel_span = self._start(name, {}, context, kind=kind)
        self._open[span.span_id] = otel_span
        self._trace_of[span.span_id] = span.trace_id
        self._made_current[span.span_id] = otel_context.attach(otel_trace.set_span_in_context(otel_span))

    def on_span_end(self, span: Span[Any]) -> None:
        self._trace_of.pop(span.span_id, None)
        if (token := self._made_current.pop(span.span_id, None)) is not None:
            otel_context.detach(token)
        if (otel_span := self._open.pop(span.span_id, None)) is None:
            return
        try:
            # Model, token counts and results are filled in while the span runs, so describe it now.
            capture = _content_capture_enabled()
            name, attributes = _describe(span.span_data, capture=capture)
            otel_span.update_name(name)
            otel_span.set_attributes(attributes)
            if span.error is not None:
                # The message can be app-supplied text, such as the reason a tool call was rejected.
                message = _mask_content(span.error["message"]) if capture else None
                otel_span.set_status(Status(StatusCode.ERROR, message))
        finally:
            otel_span.end()

    def _start(
        self,
        name: str,
        attributes: dict[str, Any],
        context: Context | None = None,
        *,
        kind: SpanKind = SpanKind.INTERNAL,
    ) -> OtelSpan:
        # The task's identity, when inside agent_framework_tracing, whichever provider records the span.
        identity = _task_identity.get() or {}
        return self._tracer.start_span(
            name, context=context, kind=kind, attributes={**identity, **attributes}
        )

    def shutdown(self) -> None:
        # The tracer provider owns export, so there is nothing of this processor's to flush.
        pass

    def force_flush(self) -> None:
        pass


def _describe(data: SpanData, *, capture: bool) -> tuple[str, dict[str, Any]]:
    """Return the OpenTelemetry span name and attributes for one SDK span, by its kind."""
    attributes: dict[str, Any] = {}
    content: dict[str, Any] = {}
    if isinstance(data, AgentSpanData):
        name = f"invoke_agent {data.name}"
        attributes = {"gen_ai.operation.name": "invoke_agent", "gen_ai.agent.name": data.name}
    elif isinstance(data, GenerationSpanData):
        name = f"chat {data.model}" if data.model else "chat"
        attributes = {"gen_ai.operation.name": "chat", **_build_usage_attributes(data.usage)}
        if data.model:
            attributes["gen_ai.request.model"] = data.model
        # The Chat Completions client can point at any compatible server, so name the server
        # rather than assume the provider.
        if base_url := (data.model_config or {}).get("base_url"):
            attributes["server.address"] = urlsplit(str(base_url)).hostname
        content = {"gen_ai.input.messages": data.input, "gen_ai.output.messages": data.output}
    elif isinstance(data, ResponseSpanData):
        model = data.response.model if data.response is not None else None
        name = f"chat {model}" if model else "chat"
        attributes = {
            "gen_ai.operation.name": "chat",
            "gen_ai.provider.name": "openai",
            **_build_usage_attributes(data.usage),
        }
        if data.response is not None:
            attributes |= {"gen_ai.response.id": data.response.id, "gen_ai.response.model": model}
        output = None
        if capture and data.response is not None:
            output = [item.model_dump(mode="json") for item in data.response.output]
        content = {"gen_ai.input.messages": data.input, "gen_ai.output.messages": output}
    elif isinstance(data, FunctionSpanData):
        name = f"execute_tool {data.name}"
        attributes = {"gen_ai.operation.name": "execute_tool", "gen_ai.tool.name": data.name}
        content = {"gen_ai.tool.call.arguments": data.input, "gen_ai.tool.call.result": data.output}
    elif isinstance(data, TaskSpanData):
        name = f"openai_agents.task {data.name}"
    elif isinstance(data, TurnSpanData):
        name = f"openai_agents.turn {data.turn}"
        attributes = {"gen_ai.agent.name": data.agent_name, "openai_agents.turn": data.turn}
    elif isinstance(data, HandoffSpanData):
        name = "openai_agents.handoff"
        attributes = {
            "openai_agents.handoff.from_agent": data.from_agent,
            "openai_agents.handoff.to_agent": data.to_agent,
        }
    elif isinstance(data, GuardrailSpanData):
        name = f"openai_agents.guardrail {data.name}"
        attributes = {"openai_agents.guardrail.triggered": data.triggered}
    else:
        name = f"openai_agents.{data.type}"
    if capture:
        attributes |= {key: _mask_content(value) for key, value in content.items() if value is not None}
    # OpenTelemetry rejects None attribute values.
    return name, {key: value for key, value in attributes.items() if value is not None}


def _mask_content(value: Any) -> str:
    """
    Render content as text with registered secrets masked.

    Tool arguments, and many tool results, are JSON text. A secret holding a quote or a
    backslash appears escaped inside it and would not match, so a JSON object or array is
    parsed and masked as values before it is rendered again. Any other text is masked as
    it is, so a result that is a bare number or string keeps its exact text.
    """
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return mask_secrets(value)
        if not isinstance(parsed, dict | list):
            return mask_secrets(value)
        value = parsed
    return dumps_masked(value)


def _build_usage_attributes(usage: dict[str, Any] | None) -> dict[str, Any]:
    if not usage:
        return {}
    return {
        "gen_ai.usage.input_tokens": usage.get("input_tokens"),
        "gen_ai.usage.output_tokens": usage.get("output_tokens"),
    }
