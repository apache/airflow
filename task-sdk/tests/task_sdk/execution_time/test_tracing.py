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
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from airflow.sdk._shared.observability.traces import new_dagrun_trace_carrier
from airflow.sdk.execution_time.tracing import detail_span


class TestDetailSpan:
    """Tests for the detail_span decorator / context manager."""

    @pytest.fixture(autouse=True)
    def _sampled_carrier_provider(self):
        """Make new_dagrun_trace_carrier produce a SAMPLED carrier.

        new_dagrun_trace_carrier consults the global tracer provider's sampler to
        decide the carrier's SAMPLED flag. In the test process the global provider
        is a no-op ProxyTracerProvider (no sampler) -> unsampled carrier, which
        would make the parent span (and its detail children) non-recording. Patch
        the lookup to a real SDK provider whose default sampler
        (parentbased_always_on) samples the root, mirroring "otel on" in production.
        """
        provider = TracerProvider()
        with mock.patch(
            "airflow._shared.observability.traces.trace.get_tracer_provider",
            return_value=provider,
        ):
            yield

    def test_level_1_no_child_span_as_context_manager(self):
        """At detail level 1, entering detail_span should not create a real recorded span."""
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        t = provider.get_tracer("test")
        carrier = new_dagrun_trace_carrier(task_span_detail_level=1)
        parent_ctx = TraceContextTextMapPropagator().extract(carrier)

        with mock.patch("airflow.sdk.execution_time.tracing.tracer", t):
            with t.start_as_current_span("parent", context=parent_ctx):
                with detail_span("child") as span:
                    assert span is trace.INVALID_SPAN

        # Only the "parent" span should be recorded; no "child".
        names = [s.name for s in exporter.get_finished_spans()]
        assert "child" not in names

    def test_level_2_creates_child_span_as_context_manager(self):
        """At detail level 2, detail_span should create a real recorded child span."""
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        t = provider.get_tracer("test")
        carrier = new_dagrun_trace_carrier(task_span_detail_level=2)
        parent_ctx = TraceContextTextMapPropagator().extract(carrier)

        with mock.patch("airflow.sdk.execution_time.tracing.tracer", t):
            with t.start_as_current_span("parent", context=parent_ctx):
                with detail_span("child"):
                    pass

        names = [s.name for s in exporter.get_finished_spans()]
        assert "child" in names

    def test_decorator_at_level_1_does_not_create_span(self):
        """@detail_span at level 1 should not produce a recorded span."""
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        t = provider.get_tracer("test")
        carrier = new_dagrun_trace_carrier(task_span_detail_level=1)
        parent_ctx = TraceContextTextMapPropagator().extract(carrier)

        @detail_span("decorated")
        def my_func():
            return 42

        with mock.patch("airflow.sdk.execution_time.tracing.tracer", t):
            with t.start_as_current_span("parent", context=parent_ctx):
                result = my_func()

        assert result == 42
        names = [s.name for s in exporter.get_finished_spans()]
        assert "decorated" not in names

    def test_decorator_at_level_2_creates_span_and_preserves_return_value(self):
        """@detail_span at level 2 creates a span and the wrapped function's return value is preserved."""
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        t = provider.get_tracer("test")
        carrier = new_dagrun_trace_carrier(task_span_detail_level=2)
        parent_ctx = TraceContextTextMapPropagator().extract(carrier)

        @detail_span("decorated")
        def my_func(x):
            return x * 2

        with mock.patch("airflow.sdk.execution_time.tracing.tracer", t):
            with t.start_as_current_span("parent", context=parent_ctx):
                result = my_func(7)

        assert result == 14
        names = [s.name for s in exporter.get_finished_spans()]
        assert "decorated" in names

    def test_exception_in_context_manager_propagates(self):
        """Exceptions inside `with detail_span(...)` propagate normally."""
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        t = provider.get_tracer("test")
        carrier = new_dagrun_trace_carrier(task_span_detail_level=2)
        parent_ctx = TraceContextTextMapPropagator().extract(carrier)

        with mock.patch("airflow.sdk.execution_time.tracing.tracer", t):
            with t.start_as_current_span("parent", context=parent_ctx):
                with pytest.raises(ValueError, match="boom"):
                    with detail_span("child"):
                        raise ValueError("boom")
