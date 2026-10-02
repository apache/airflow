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

import os
import threading
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from opentelemetry import baggage, context as otel_context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from airflow.providers.common.ai.tools.tracing import agent_framework_tracing

from tests_common.test_utils.config import conf_vars

MODULE = "airflow.providers.common.ai.tools.tracing"
TI = SimpleNamespace(
    dag_id="reports", task_id="summarize", run_id="manual__1", try_number=2, map_index=-1, id=uuid.uuid4()
)
UNSAMPLED = SpanContext(
    trace_id=0x4BF92F3577B34DA6A3CE929D0E0E4736,
    span_id=0x00F067AA0BA902B7,
    is_remote=True,
    trace_flags=TraceFlags(TraceFlags.DEFAULT),
)
# The Dag run's trace context, propagated to the task and marked as not sampled.
TI_UNSAMPLED = SimpleNamespace(
    **vars(TI), context_carrier={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"}
)
CONTENT_SWITCHES = (
    "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT",
    "ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS",
    "OTEL_SEMCONV_STABILITY_OPT_IN",
)


@pytest.fixture
def exporter():
    """A tracer provider standing in for the worker's, recording what it would export."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with (
        patch(f"{MODULE}._live_tracer_provider", autospec=True, return_value=provider),
        patch(f"{MODULE}.get_current_context", autospec=True, return_value={"ti": TI}),
    ):
        yield exporter, provider


@pytest.fixture
def clean_environment(monkeypatch):
    for name in CONTENT_SWITCHES:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.usefixtures("clean_environment")
class TestContentSwitches:
    def test_content_is_off_inside_the_block_and_the_environment_is_restored(self, exporter):
        with agent_framework_tracing():
            inside = {name: os.environ.get(name) for name in CONTENT_SWITCHES}

        assert inside == {
            "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT": "false",
            "ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS": "false",
            "OTEL_SEMCONV_STABILITY_OPT_IN": "gen_ai_unredacted_attributes=",
        }
        assert all(name not in os.environ for name in CONTENT_SWITCHES)

    def test_keeps_the_deployments_own_switches(self, exporter, monkeypatch):
        monkeypatch.setenv("ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS", "true")
        monkeypatch.setenv("OTEL_SEMCONV_STABILITY_OPT_IN", "gen_ai_latest_experimental")

        with agent_framework_tracing():
            assert os.environ["ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS"] == "true"
            assert os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] == (
                "gen_ai_latest_experimental,gen_ai_unredacted_attributes="
            )

        assert os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] == "gen_ai_latest_experimental"

    def test_overlapping_blocks_keep_content_off_until_the_last_one_exits(self, exporter):
        """Two agents running in threads of one task open blocks that need not close in order."""
        second_open, first_closed = threading.Event(), threading.Event()
        seen_after_first_closed: list[str | None] = []

        def first():
            with agent_framework_tracing():
                second_open.wait(5)
            first_closed.set()

        def second():
            with agent_framework_tracing():
                second_open.set()
                first_closed.wait(5)
                seen_after_first_closed.append(os.environ.get("ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS"))

        threads = [threading.Thread(target=first), threading.Thread(target=second)]
        threads[0].start()
        threads[1].start()
        for thread in threads:
            thread.join()

        assert seen_after_first_closed == ["false"]
        assert all(name not in os.environ for name in CONTENT_SWITCHES)

    @conf_vars({("common.ai", "otel_export_enabled"): "True", ("common.ai", "capture_content"): "True"})
    def test_captures_content_when_the_deployment_asks_for_it(self, exporter):
        with agent_framework_tracing():
            assert all(name not in os.environ for name in CONTENT_SWITCHES)

    @conf_vars({("common.ai", "otel_export_enabled"): "False", ("common.ai", "capture_content"): "True"})
    def test_capture_content_alone_leaves_content_out(self, exporter):
        """capture_content has no effect unless otel_export_enabled is on, as for AgentOperator."""
        with agent_framework_tracing():
            assert os.environ.get("ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS") == "false"


class TestTaskIdentity:
    def test_spans_started_inside_the_block_carry_the_task(self, exporter):
        spans, provider = exporter
        tracer = provider.get_tracer("agent_framework")

        with agent_framework_tracing():
            tracer.start_span("inside").end()
        tracer.start_span("outside").end()

        by_name = {span.name: dict(span.attributes) for span in spans.get_finished_spans()}
        assert by_name["inside"]["airflow.dag_id"] == "reports"
        assert by_name["inside"]["airflow.task_instance.try_number"] == 2
        assert by_name["inside"]["airflow.task_instance.id"] == str(TI.id)
        assert "airflow.dag_id" not in by_name["outside"]

    def test_the_processor_is_added_once_per_provider(self, exporter):
        _, provider = exporter

        with agent_framework_tracing():
            pass
        with agent_framework_tracing():
            pass

        processors = provider._active_span_processor._span_processors
        assert sum(type(p).__name__ == "_TaskIdentityProcessor" for p in processors) == 1


def _current_inside_block(current: SpanContext, ti: SimpleNamespace, ctx=None):
    token = otel_context.attach(trace.set_span_in_context(NonRecordingSpan(current), ctx))
    try:
        with (
            patch(f"{MODULE}.get_current_context", new=lambda: {"ti": ti}),
            agent_framework_tracing(),
        ):
            inside = trace.get_current_span().get_span_context()
            inside_baggage = baggage.get_baggage("k")
        after = trace.get_current_span().get_span_context()
    finally:
        otel_context.detach(token)
    return inside, inside_baggage, after


class TestUnsampledParent:
    def test_the_propagated_context_is_not_the_parent_when_no_provider_made_a_task_span(self, exporter):
        inside, _, after = _current_inside_block(UNSAMPLED, TI_UNSAMPLED)

        assert not inside.is_valid
        assert after == UNSAMPLED

    def test_a_task_span_under_the_propagated_context_keeps_the_sampling_decision(self, exporter):
        """Core tracing or auto-instrumentation made the task's span; the Dag run was sampled out."""
        task_span = SpanContext(
            trace_id=UNSAMPLED.trace_id,
            span_id=0x1111111111111111,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.DEFAULT),
        )

        inside, _, _ = _current_inside_block(task_span, TI_UNSAMPLED)

        assert inside == task_span

    def test_without_a_propagated_context_nothing_is_detached(self, exporter):
        """Airflow 3.0 and 3.1 propagate no trace context to the task."""
        inside, _, _ = _current_inside_block(UNSAMPLED, TI)

        assert inside == UNSAMPLED


class TestParentContext:
    def test_a_sampled_propagated_context_is_kept(self, exporter):
        sampled = SpanContext(
            trace_id=UNSAMPLED.trace_id,
            span_id=UNSAMPLED.span_id,
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
        ti = SimpleNamespace(
            **vars(TI),
            context_carrier={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
        )

        inside, _, _ = _current_inside_block(sampled, ti)

        assert inside == sampled

    def test_detaching_the_propagated_context_keeps_baggage(self, exporter):
        inside, inside_baggage, _ = _current_inside_block(
            UNSAMPLED, TI_UNSAMPLED, baggage.set_baggage("k", "v")
        )

        assert not inside.is_valid
        assert inside_baggage == "v"
