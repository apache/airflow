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

from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.context import Context
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from airflow_shared.observability.traces.otel_tracer import OtelTrace


@pytest.fixture
def otel_trace():
    """Create an OtelTrace instance with mocked span exporter."""
    mock_exporter = MagicMock()
    return OtelTrace(
        span_exporter=mock_exporter,
        use_simple_processor=True,
        otel_service="test-service",
    )


class TestOtelTraceExtract:
    """Tests for OtelTrace.extract() method."""

    def test_extract_with_none_carrier_returns_empty_dict(self, otel_trace):
        """Test that extract() handles None carrier gracefully.

        This can happen when context_carrier is NULL in DB (tasks created
        before OTel was enabled, then cleared/backfilled).
        """
        result = otel_trace.extract(None)
        assert result == {}

    def test_extract_with_empty_carrier_returns_empty_context(self, otel_trace):
        """Test that extract() handles empty dict carrier."""
        result = otel_trace.extract({})
        # Empty carrier should return an empty Context (no span info)
        assert isinstance(result, Context)

    def test_extract_with_valid_carrier(self, otel_trace):
        """Test that extract() works with a valid traceparent carrier."""
        carrier = {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}
        result = otel_trace.extract(carrier)
        assert isinstance(result, Context)


class TestOtelTraceStartChildSpan:
    """Tests for OtelTrace.start_child_span() method."""

    def test_start_child_span_with_none_parent_context(self, otel_trace):
        """Test that start_child_span() works when parent_context is None."""
        # Should use current span context as parent
        with patch.object(otel_trace, "_new_span") as mock_new_span:
            mock_new_span.return_value = MagicMock()
            otel_trace.start_child_span("test-span", parent_context=None)
            mock_new_span.assert_called_once()

    def test_start_child_span_with_empty_context(self, otel_trace):
        """Test that start_child_span() handles empty Context gracefully.

        This can happen when context_carrier is {} (empty dict), which causes
        extract() to return an empty Context. Previously this would raise
        StopIteration from next(iter(parent_context.values())).
        """
        empty_context = Context()

        with patch.object(otel_trace, "_new_span") as mock_new_span:
            mock_new_span.return_value = MagicMock()
            # Should not raise StopIteration
            otel_trace.start_child_span("test-span", parent_context=empty_context)
            mock_new_span.assert_called_once()

    def test_start_child_span_with_valid_parent_context(self, otel_trace):
        """Test that start_child_span() works with a valid parent context."""
        # Create a valid span context
        span_context = SpanContext(
            trace_id=0x0AF7651916CD43DD8448EB211C80319C,
            span_id=0xB7AD6B7169203331,
            is_remote=True,
            trace_flags=TraceFlags(0x01),
        )
        non_recording_span = NonRecordingSpan(span_context)

        # Create a context with the span
        from opentelemetry import trace

        parent_context = trace.set_span_in_context(non_recording_span)

        with patch.object(otel_trace, "_new_span") as mock_new_span:
            mock_new_span.return_value = MagicMock()
            otel_trace.start_child_span("test-span", parent_context=parent_context)
            mock_new_span.assert_called_once()
            # Verify that links include the parent trace
            call_kwargs = mock_new_span.call_args[1]
            assert len(call_kwargs["links"]) == 1
            assert call_kwargs["links"][0].attributes["from"] == "parenttrace"
