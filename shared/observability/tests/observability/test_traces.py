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

from unittest import mock

import pytest
from opentelemetry import trace as otel_trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from airflow_shared.observability import traces as traces_module
from airflow_shared.observability.traces import cli_span


@pytest.fixture
def in_memory_tracer():
    """Install a real tracer provider with an in-memory exporter for assertion."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")
    with mock.patch.object(otel_trace, "get_tracer", return_value=tracer):
        yield exporter


class TestCliSpan:
    def test_emits_span_without_traceparent(self, in_memory_tracer):
        with cli_span("cli.dags.test", attributes={"airflow.dag_id": "demo"}, environ={}):
            pass
        spans = in_memory_tracer.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.name == "cli.dags.test"
        assert span.attributes["airflow.dag_id"] == "demo"
        # Root span has no parent
        assert span.parent is None

    def test_extracts_parent_from_traceparent(self, in_memory_tracer):
        # W3C traceparent: 00-<trace-id>-<span-id>-<flags>, all lowercase hex.
        trace_id_hex = "0af7651916cd43dd8448eb211c80319c"
        span_id_hex = "b7ad6b7169203331"
        traceparent = f"00-{trace_id_hex}-{span_id_hex}-01"
        with cli_span(
            "cli.tasks.test",
            attributes={"airflow.dag_id": "demo"},
            environ={"TRACEPARENT": traceparent},
        ):
            pass
        spans = in_memory_tracer.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.parent is not None
        # The injected parent context should be linked: same trace id, child span id matches header.
        assert format(span.context.trace_id, "032x") == trace_id_hex
        assert format(span.parent.span_id, "016x") == span_id_hex

    def test_propagates_tracestate(self, in_memory_tracer):
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        with cli_span(
            "cli.tasks.test",
            environ={"TRACEPARENT": traceparent, "TRACESTATE": "vendor=value"},
        ):
            pass
        spans = in_memory_tracer.get_finished_spans()
        assert len(spans) == 1
        # tracestate is preserved on the child span's context
        assert spans[0].context.trace_state.to_header() == "vendor=value"

    def test_ignores_malformed_traceparent(self, in_memory_tracer):
        with cli_span(
            "cli.tasks.test",
            environ={"TRACEPARENT": "garbage"},
        ):
            pass
        spans = in_memory_tracer.get_finished_spans()
        assert len(spans) == 1
        # Falls back to root span since the propagator rejects the malformed header.
        assert spans[0].parent is None

    def test_default_environ_uses_os_environ(self, in_memory_tracer, monkeypatch):
        trace_id_hex = "0af7651916cd43dd8448eb211c80319c"
        span_id_hex = "b7ad6b7169203331"
        monkeypatch.setenv("TRACEPARENT", f"00-{trace_id_hex}-{span_id_hex}-01")
        with cli_span("cli.dags.trigger"):
            pass
        spans = in_memory_tracer.get_finished_spans()
        assert len(spans) == 1
        assert format(spans[0].context.trace_id, "032x") == trace_id_hex

    def test_helper_safe_when_otel_disabled(self):
        # No mock here: default global tracer provider may be no-op, but the
        # helper must not raise and must execute the body.
        called = False
        with cli_span("cli.dags.trigger", environ={}):
            called = True
        assert called

    def test_module_exports(self):
        """Public helper is exposed on the module."""
        assert hasattr(traces_module, "cli_span")
