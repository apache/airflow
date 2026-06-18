#
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

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    ParentBased,
    TraceIdRatioBased,
)
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from airflow_shared.observability.traces import (
    DEFAULT_TASK_SPAN_DETAIL_LEVEL,
    TASK_SPAN_DETAIL_LEVEL_KEY,
    build_trace_state_entries,
    get_task_span_detail_level,
    new_dagrun_trace_carrier,
)


def _carrier_is_sampled(carrier: dict[str, str]) -> bool:
    ctx = TraceContextTextMapPropagator().extract(carrier)
    return trace.get_current_span(ctx).get_span_context().trace_flags.sampled


class TestBuildTraceStateEntries:
    def test_with_integer_level(self):
        entries = build_trace_state_entries(2)
        assert entries == [(TASK_SPAN_DETAIL_LEVEL_KEY, "2")]

    def test_with_string_level(self):
        entries = build_trace_state_entries("3")
        assert entries == [(TASK_SPAN_DETAIL_LEVEL_KEY, "3")]

    def test_with_none(self):
        assert build_trace_state_entries(None) == []

    def test_with_zero(self):
        # 0 is falsy — treated as no detail level
        assert build_trace_state_entries(0) == []

    def test_with_invalid_string(self):
        # Non-integer string should not raise; returns empty
        assert build_trace_state_entries("not-a-number") == []


class TestNewDagrunTraceCarrier:
    def test_with_detail_level_embeds_level_in_trace_state(self):
        carrier = new_dagrun_trace_carrier(task_span_detail_level=2)
        ctx = TraceContextTextMapPropagator().extract(carrier)
        from opentelemetry import trace

        span_ctx = trace.get_current_span(ctx).get_span_context()
        assert span_ctx.trace_state.get(TASK_SPAN_DETAIL_LEVEL_KEY) == "2"

    def test_without_detail_level_has_empty_trace_state(self):
        carrier = new_dagrun_trace_carrier()
        ctx = TraceContextTextMapPropagator().extract(carrier)
        from opentelemetry import trace

        span_ctx = trace.get_current_span(ctx).get_span_context()
        assert span_ctx.trace_state.get(TASK_SPAN_DETAIL_LEVEL_KEY) is None


class TestNewDagrunTraceCarrierSampling:
    """The carrier's SAMPLED flag should reflect the configured sampler's root decision."""

    @pytest.fixture
    def with_sampler(self, monkeypatch):
        """Install a TracerProvider with the given sampler for new_dagrun_trace_carrier."""

        def _install(sampler):
            provider = TracerProvider(sampler=sampler)
            monkeypatch.setattr(
                "airflow_shared.observability.traces.trace.get_tracer_provider",
                lambda: provider,
            )

        return _install

    def test_no_sampler_provider_not_sampled(self, monkeypatch):
        """A proxy/no-op provider (otel off) has no ``sampler`` attribute -> not sampled."""

        class _NoSamplerProvider:
            pass

        monkeypatch.setattr(
            "airflow_shared.observability.traces.trace.get_tracer_provider",
            lambda: _NoSamplerProvider(),
        )
        assert _carrier_is_sampled(new_dagrun_trace_carrier()) is False

    def test_default_provider_is_sampled(self):
        """The SDK default provider (parentbased_always_on) samples the root -> backcompat."""
        # No monkeypatching: rely on whatever default provider is configured.
        # A bare TracerProvider() defaults to parentbased_always_on.
        provider = TracerProvider()
        assert provider.sampler is not None
        result = provider.sampler.should_sample(parent_context=None, trace_id=1234, name="dag_run")
        from opentelemetry.sdk.trace.sampling import Decision

        assert result.decision == Decision.RECORD_AND_SAMPLE

    def test_always_on_is_sampled(self, with_sampler):
        with_sampler(ParentBased(ALWAYS_ON))
        assert _carrier_is_sampled(new_dagrun_trace_carrier()) is True

    def test_always_off_is_not_sampled(self, with_sampler):
        with_sampler(ALWAYS_OFF)
        assert _carrier_is_sampled(new_dagrun_trace_carrier()) is False

    def test_traceidratio_is_deterministic_per_trace_id(self, with_sampler):
        """A ratio sampler makes a deterministic decision keyed on trace_id."""
        with_sampler(TraceIdRatioBased(0.5))
        # Generate a batch; with ratio 0.5 we expect a mix, and each individual
        # decision must be stable for its own trace_id.
        carriers = [new_dagrun_trace_carrier() for _ in range(50)]
        decisions = [_carrier_is_sampled(c) for c in carriers]
        # Re-evaluating the same trace_id yields the same decision (determinism):
        sampler = TraceIdRatioBased(0.5)
        from opentelemetry.sdk.trace.sampling import Decision

        for carrier, decided in zip(carriers, decisions):
            ctx = TraceContextTextMapPropagator().extract(carrier)
            trace_id = trace.get_current_span(ctx).get_span_context().trace_id
            redo = sampler.should_sample(parent_context=None, trace_id=trace_id, name="dag_run")
            assert (redo.decision == Decision.RECORD_AND_SAMPLE) == decided
        # Check: ratio 0.5 over 50 should produce a mix of both outcomes.
        assert any(decisions)
        assert not all(decisions)

    def test_ratio_zero_never_sampled(self, with_sampler):
        with_sampler(TraceIdRatioBased(0.0))
        assert all(_carrier_is_sampled(new_dagrun_trace_carrier()) is False for _ in range(20))

    def test_detail_level_roundtrips_when_sampled(self, with_sampler):
        with_sampler(ParentBased(ALWAYS_ON))
        carrier = new_dagrun_trace_carrier(task_span_detail_level=3)
        ctx = TraceContextTextMapPropagator().extract(carrier)
        span = trace.get_current_span(ctx)
        assert get_task_span_detail_level(span) == 3
        assert _carrier_is_sampled(carrier) is True

    def test_detail_level_roundtrips_when_not_sampled(self, with_sampler):
        """Detail-level tracestate must survive even for an unsampled carrier."""
        with_sampler(ALWAYS_OFF)
        carrier = new_dagrun_trace_carrier(task_span_detail_level=2)
        ctx = TraceContextTextMapPropagator().extract(carrier)
        span = trace.get_current_span(ctx)
        assert get_task_span_detail_level(span) == 2
        assert _carrier_is_sampled(carrier) is False


class TestGetTaskSpanDetailLevel:
    def _make_span_with_trace_state(self, entries: list[tuple[str, str]]) -> NonRecordingSpan:
        from opentelemetry.sdk.trace.id_generator import RandomIdGenerator

        gen = RandomIdGenerator()
        span_ctx = SpanContext(
            trace_id=gen.generate_trace_id(),
            span_id=gen.generate_span_id(),
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(entries=entries),
        )
        return NonRecordingSpan(span_ctx)

    def test_returns_default_when_no_trace_state(self):
        span = self._make_span_with_trace_state([])
        assert get_task_span_detail_level(span) == DEFAULT_TASK_SPAN_DETAIL_LEVEL

    def test_reads_level_from_trace_state(self):
        span = self._make_span_with_trace_state([(TASK_SPAN_DETAIL_LEVEL_KEY, "2")])
        assert get_task_span_detail_level(span) == 2

    def test_fallback_on_invalid_value(self):
        span = self._make_span_with_trace_state([(TASK_SPAN_DETAIL_LEVEL_KEY, "bad")])
        assert get_task_span_detail_level(span) == DEFAULT_TASK_SPAN_DETAIL_LEVEL

    def test_roundtrip_via_carrier(self):
        """Level set in new_dagrun_trace_carrier is readable by get_task_span_detail_level."""
        carrier = new_dagrun_trace_carrier(task_span_detail_level=3)
        ctx = TraceContextTextMapPropagator().extract(carrier)
        from opentelemetry import trace

        span = trace.get_current_span(ctx)
        assert get_task_span_detail_level(span) == 3
