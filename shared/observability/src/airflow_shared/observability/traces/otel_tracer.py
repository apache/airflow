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

import logging
import warnings
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING

import pendulum
from opentelemetry import trace
from opentelemetry.context import attach
from opentelemetry.trace import Link, NonRecordingSpan, SpanContext, TraceFlags, Tracer, TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.trace.span import INVALID_SPAN_ID, INVALID_TRACE_ID, Span

from ..exceptions import RemovedInAirflow4Warning
from .utils import (
    datetime_to_nano,
)

if TYPE_CHECKING:
    from opentelemetry.context.context import Context

log = logging.getLogger(__name__)

tracer = trace.get_tracer(__name__)


class OtelTrace:
    """
    DO NOT USE THIS CLASS.

    We don't use this anymore.  It's just here for backcompat.

    If you are creating spans in tasks, import trace from airflow.sdk.opentelemetry and call
    tracer = trace.get_tracer(__name__).
    """

    def __init__(self, **kwargs):
        warnings.warn(
            "`OtelTrace` is deprecated! Do not use it! Use trace.get_tracer() instead.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )
        super().__init__(**kwargs)

    @staticmethod
    def _gen_context(trace_id: int, span_id: int):
        """Generate a remote span context for given trace and span id."""
        span_ctx = SpanContext(
            trace_id=trace_id, span_id=span_id, is_remote=True, trace_flags=TraceFlags(0x01)
        )
        return span_ctx

    def _gen_links_from_kv_list(self, kv_list):
        """Convert list of kv dic of trace_id and span_id and generate list of SpanContext."""
        result = []
        for a in kv_list:
            trace_id = a["trace_id"]  # string of hexa
            span_id = a["span_id"]  # string of hexa
            span_ctx = self._gen_context(trace_id, span_id)
            a_link = Link(
                context=span_ctx,
                attributes={"meta.annotation_type": "link"},
            )
            result.append(a_link)
        return result

    def get_otel_tracer_provider(
        self,
        trace_id: int | None = None,
        span_id: int | None = None,
    ) -> TracerProvider:
        warnings.warn(
            "The OtelTrace class is deprecated.  Do not use it! Use trace.get_tracer() instead.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )

        return trace.get_tracer_provider()

    def get_tracer(
        self,
        component: str = None,
        trace_id: int | None = None,
        span_id: int | None = None,
    ) -> Tracer:
        warnings.warn(
            "The OtelTrace class is deprecated.  Do not use it! Use trace.get_tracer() instead.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )
        return tracer

    def get_current_span(self):
        warnings.warn(
            "The OtelTrace class is deprecated.  Do not use it! Use trace.get_tracer() instead.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )
        return trace.get_current_span()

    def use_span(self, span: Span):
        warnings.warn(
            "The OtelTrace class is deprecated.  Do not use it! Use trace.get_tracer() instead.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )
        return trace.use_span(span=span)

    def start_span(
        self,
        span_name: str,
        component: str | None = None,
        parent_sc: SpanContext | None = None,
        span_id=None,
        links=None,
        start_time=None,
    ):
        warnings.warn(
            "The OtelTrace class is deprecated.  Do not use it! Use trace.get_tracer() instead.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )
        if links is not None:
            _links = self._gen_links_from_kv_list(links)
        else:
            _links = []

        if start_time is not None:
            start_time = datetime_to_nano(start_time)

        if parent_sc is not None:
            ctx = trace.set_span_in_context(NonRecordingSpan(parent_sc))
            span = tracer.start_as_current_span(span_name, context=ctx, links=_links, start_time=start_time)
        else:
            span = tracer.start_as_current_span(span_name, links=_links, start_time=start_time)
        return span

    def start_root_span(
        self,
        span_name: str,
        component: str | None = None,
        links=None,
        start_time=None,
        start_as_current: bool = True,
    ):
        warnings.warn(
            "The OtelTrace class is deprecated.  Do not use it! Use trace.get_tracer() instead.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )
        invalid_span_ctx = SpanContext(
            trace_id=INVALID_TRACE_ID, span_id=INVALID_SPAN_ID, is_remote=True, trace_flags=TraceFlags(0x01)
        )
        invalid_ctx = trace.set_span_in_context(NonRecordingSpan(invalid_span_ctx))

        if links is None:
            _links = []
        else:
            _links = links

        return self._new_span(
            span_name=span_name,
            parent_context=invalid_ctx,
            component=component,
            links=_links,
            start_time=start_time,
            start_as_current=start_as_current,
        )

    def start_child_span(
        self,
        span_name: str,
        parent_context: Context | None = None,
        component: str | None = None,
        links=None,
        start_time=None,
        start_as_current: bool = True,
    ):
        """Start a child span."""
        if parent_context is None:
            # If no context is passed, then use the current.
            parent_span_context = trace.get_current_span().get_span_context()
            parent_context = trace.set_span_in_context(NonRecordingSpan(parent_span_context))
        else:
            context_val = next(iter(parent_context.values()))
            parent_span_context = None
            if isinstance(context_val, NonRecordingSpan):
                parent_span_context = context_val.get_span_context()

        if links is None:
            _links = []
        else:
            _links = links

        if parent_span_context is not None:
            _links.append(
                Link(
                    context=parent_span_context,
                    attributes={"meta.annotation_type": "link", "from": "parenttrace"},
                )
            )

        return self._new_span(
            span_name=span_name,
            parent_context=parent_context,
            component=component,
            links=_links,
            start_time=start_time,
            start_as_current=start_as_current,
        )

    def _new_span(
        self,
        span_name: str,
        parent_context: Context | None = None,
        component: str | None = None,
        links=None,
        start_time=None,
        start_as_current: bool = True,
    ) -> AbstractContextManager[trace.span.Span] | trace.span.Span:
        warnings.warn(
            "The OtelTrace class is deprecated.  Do not use it! Use trace.get_tracer() instead.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )
        if start_time is None:
            start_time = pendulum.now(tz=pendulum.UTC)

        if links is None:
            links = []

        if start_as_current:
            return tracer.start_as_current_span(
                name=span_name,
                context=parent_context,
                links=links,
                start_time=datetime_to_nano(start_time),
            )

        span = tracer.start_span(  # type: ignore[assignment]
            name=span_name,
            context=parent_context,
            links=links,
            start_time=datetime_to_nano(start_time),
        )
        current_span_ctx = trace.set_span_in_context(NonRecordingSpan(span.get_span_context()))  # type: ignore[attr-defined]
        # We have to manually make the span context as the active context.
        # If the span needs to be injected into the carrier, then this is needed to make sure
        # that the injected context will point to the span context that was just created.
        attach(current_span_ctx)
        return span

    def inject(self) -> dict:
        """Inject the current span context into a carrier and return it."""
        warnings.warn(
            "The OtelTrace class is deprecated.  Do not use it! Use trace.get_tracer() instead.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )
        carrier: dict[str, str] = {}
        TraceContextTextMapPropagator().inject(carrier)
        return carrier

    def extract(self, carrier: dict) -> Context:
        """Extract the span context from a provided carrier."""
        warnings.warn(
            "The OtelTrace class is deprecated.  Do not use it! Use trace.get_tracer() instead.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )
        return TraceContextTextMapPropagator().extract(carrier)
