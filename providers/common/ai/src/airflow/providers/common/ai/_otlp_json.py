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
Minimal OTLP/JSON span encoder for the ``[common.ai] trace_store_path`` store.

Writes one OTLP ``TracesData`` JSON object per line, following the OTLP
JSON-Protobuf encoding rules (https://opentelemetry.io/docs/specs/otlp/):
trace/span ids hex-encoded, 64-bit integers as decimal strings, enums as
integers, camelCase field names. That makes the files replayable into any real
backend later via an OpenTelemetry Collector's ``otlpjsonfilereceiver``.

This is vendored rather than depended on: the upstream
``opentelemetry-exporter-otlp-json-file`` exporter (0.64b0, the only release)
is uninstallable from PyPI -- its ``opentelemetry-proto-json`` dependency was
never published (verified 2026-07-04). Swap this module for the official
``FileSpanExporter(stream=...)`` once a fixed upstream release exists.

Only imported when the trace store is configured, so the OpenTelemetry SDK
import below never taxes the tracing-off path.
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Mapping, Sequence
from typing import IO, TYPE_CHECKING, Any

from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import StatusCode

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import ReadableSpan

log = logging.getLogger(__name__)

# SDK enum value -> OTLP wire value (OTLP reserves 0 for UNSPECIFIED, so the
# SDK's INTERNAL=0..CONSUMER=4 shift up by one).
_OTLP_STATUS = {StatusCode.UNSET: 0, StatusCode.OK: 1, StatusCode.ERROR: 2}


def _any_value(value: Any) -> dict[str, Any]:
    # bool before int: bool is an int subclass.
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, (bytes, bytearray)):
        return {"bytesValue": base64.b64encode(value).decode()}
    if isinstance(value, Mapping):
        return {"kvlistValue": {"values": _key_values(value)}}
    if isinstance(value, Sequence) and not isinstance(value, str):
        return {"arrayValue": {"values": [_any_value(v) for v in value]}}
    return {"stringValue": str(value)}


def _key_values(attributes: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [{"key": k, "value": _any_value(v)} for k, v in attributes.items()]


def _encode_span(span: ReadableSpan) -> dict[str, Any]:
    ctx = span.get_span_context()
    out: dict[str, Any] = {
        "traceId": format(ctx.trace_id if ctx else 0, "032x"),
        "spanId": format(ctx.span_id if ctx else 0, "016x"),
        "name": span.name,
        "kind": (span.kind.value if span.kind is not None else 0) + 1,
        "startTimeUnixNano": str(span.start_time or 0),
        "endTimeUnixNano": str(span.end_time or 0),
        "attributes": _key_values(span.attributes or {}),
        "status": {},
    }
    if span.parent is not None:
        out["parentSpanId"] = format(span.parent.span_id, "016x")
    if span.events:
        # Exception details land here via Span.record_exception.
        out["events"] = [
            {
                "timeUnixNano": str(event.timestamp),
                "name": event.name,
                "attributes": _key_values(event.attributes or {}),
            }
            for event in span.events
        ]
    if span.status is not None:
        code = _OTLP_STATUS.get(span.status.status_code, 0)
        if code:
            out["status"]["code"] = code
        if span.status.description:
            out["status"]["message"] = span.status.description
    return out


def encode_traces_data(spans: Sequence[ReadableSpan]) -> dict[str, Any]:
    """Encode finished spans as one OTLP ``TracesData`` object, grouped by resource/scope."""
    grouped: dict[Any, dict[Any, list[dict[str, Any]]]] = {}
    resources: dict[Any, Any] = {}
    scopes: dict[Any, Any] = {}
    for span in spans:
        resource, scope = span.resource, span.instrumentation_scope
        resources[id(resource)] = resource
        scopes[id(scope)] = scope
        grouped.setdefault(id(resource), {}).setdefault(id(scope), []).append(_encode_span(span))
    return {
        "resourceSpans": [
            {
                "resource": {"attributes": _key_values(getattr(resources[rid], "attributes", None) or {})},
                "scopeSpans": [
                    {
                        "scope": {
                            "name": getattr(scopes[sid], "name", "") or "",
                            **(
                                {"version": scopes[sid].version}
                                if getattr(scopes[sid], "version", None)
                                else {}
                            ),
                        },
                        "spans": scope_spans,
                    }
                    for sid, scope_spans in by_scope.items()
                ],
            }
            for rid, by_scope in grouped.items()
        ]
    }


class OTLPJsonStreamExporter(SpanExporter):
    """
    Write each export batch as one OTLP/JSON ``TracesData`` line to a text stream.

    Flushes after every line so spans are readable while the task still runs.
    On ``file://`` this makes each finished span immediately visible; object
    stores (s3/gcs) buffer locally and upload on close, so their content lands
    at task-process exit -- acceptable for a dev/test store.
    """

    def __init__(self, stream: IO[str]) -> None:
        self._stream = stream

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        try:
            self._stream.write(json.dumps(encode_traces_data(spans), separators=(",", ":")) + "\n")
            self._stream.flush()
        except Exception:
            log.exception("Failed to write %d span(s) to the common.ai trace store", len(spans))
            return SpanExportResult.FAILURE
        return SpanExportResult.SUCCESS

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        self._stream.flush()
        return True

    def shutdown(self) -> None:
        self._stream.flush()
