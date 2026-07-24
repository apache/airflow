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

import base64
import io
import json

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import Status, StatusCode

from airflow.providers.common.ai._otlp_json import (
    OTLPJsonStreamExporter,
    _any_value,
    encode_traces_data,
)


class TestAnyValue:
    """OTLP AnyValue encoding rules (spec: intValue is a decimal string, bool before int)."""

    def test_bool_encodes_as_bool_not_int(self):
        # bool is an int subclass; the isinstance order must catch it first, or
        # True would wrongly serialize as {"intValue": "1"}.
        assert _any_value(True) == {"boolValue": True}
        assert _any_value(False) == {"boolValue": False}

    def test_int_encodes_as_decimal_string(self):
        assert _any_value(42) == {"intValue": "42"}
        assert _any_value(-1) == {"intValue": "-1"}

    def test_float_encodes_as_double(self):
        assert _any_value(1.5) == {"doubleValue": 1.5}

    def test_str_encodes_as_string(self):
        assert _any_value("hi") == {"stringValue": "hi"}

    def test_bytes_encode_base64(self):
        assert _any_value(b"ab") == {"bytesValue": base64.b64encode(b"ab").decode()}

    def test_list_encodes_as_arrayvalue(self):
        assert _any_value([1, "x"]) == {"arrayValue": {"values": [{"intValue": "1"}, {"stringValue": "x"}]}}

    def test_mapping_encodes_as_kvlist(self):
        assert _any_value({"k": 1}) == {"kvlistValue": {"values": [{"key": "k", "value": {"intValue": "1"}}]}}


def _spans_from(build) -> list[dict]:
    """Run ``build(tracer)`` through the exporter and return the raw span dicts written."""
    buf = io.StringIO()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(OTLPJsonStreamExporter(buf)))
    tracer = provider.get_tracer("pydantic-ai", "1.99.0")
    build(tracer)
    provider.shutdown()
    spans: list[dict] = []
    for line in buf.getvalue().splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        for rs in data.get("resourceSpans") or []:
            for ss in rs.get("scopeSpans") or []:
                spans.extend(ss.get("spans") or [])
    return spans


class TestEncodeSpans:
    def test_ids_are_hex_and_parent_links(self):
        def build(tracer):
            with tracer.start_as_current_span("root"):
                with tracer.start_as_current_span("child"):
                    pass

        spans = {s["name"]: s for s in _spans_from(build)}
        root, child = spans["root"], spans["child"]
        assert len(root["traceId"]) == 32
        assert all(c in "0123456789abcdef" for c in root["traceId"])
        assert len(root["spanId"]) == 16
        # Root carries no parentSpanId; child points at root; shared trace id.
        assert "parentSpanId" not in root
        assert child["parentSpanId"] == root["spanId"]
        assert child["traceId"] == root["traceId"]

    def test_times_and_kind(self):
        def build(tracer):
            with tracer.start_as_current_span("s"):
                pass

        (span,) = _spans_from(build)
        # int64 nanos as decimal strings; end after start.
        assert isinstance(span["startTimeUnixNano"], str)
        assert int(span["endTimeUnixNano"]) >= int(span["startTimeUnixNano"]) > 0
        # SDK SpanKind.INTERNAL (0) shifts to OTLP 1 (0 is UNSPECIFIED on the wire).
        assert span["kind"] == 1

    def test_attributes_round_trip_values(self):
        def build(tracer):
            with tracer.start_as_current_span("s") as s:
                s.set_attribute("gen_ai.usage.input_tokens", 51)
                s.set_attribute("gen_ai.request.model", "claude-opus-4-8")
                s.set_attribute("flag", True)

        (span,) = _spans_from(build)
        attrs = {kv["key"]: kv["value"] for kv in span["attributes"]}
        assert attrs["gen_ai.usage.input_tokens"] == {"intValue": "51"}
        assert attrs["gen_ai.request.model"] == {"stringValue": "claude-opus-4-8"}
        assert attrs["flag"] == {"boolValue": True}

    def test_unset_status_emits_empty_status(self):
        # A span with no explicit status must not carry a code -- the reader
        # treats a missing/zero code as non-error.
        def build(tracer):
            with tracer.start_as_current_span("s"):
                pass

        (span,) = _spans_from(build)
        assert span["status"] == {}

    def test_error_status_carries_code_and_message(self):
        def build(tracer):
            with tracer.start_as_current_span("s") as s:
                s.set_status(Status(StatusCode.ERROR, "boom"))

        (span,) = _spans_from(build)
        assert span["status"] == {"code": 2, "message": "boom"}

    def test_recorded_exception_becomes_event(self):
        def build(tracer):
            with tracer.start_as_current_span("s") as s:
                try:
                    raise ValueError("nope")
                except ValueError as e:
                    s.record_exception(e)

        (span,) = _spans_from(build)
        assert span["events"], "record_exception should produce an event"
        assert any(ev["name"] == "exception" for ev in span["events"])


class TestEncodeTracesDataGrouping:
    def test_empty_input_yields_empty_resource_spans(self):
        assert encode_traces_data([]) == {"resourceSpans": []}
