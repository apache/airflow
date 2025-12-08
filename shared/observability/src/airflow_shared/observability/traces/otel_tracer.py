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
import os
import random
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING

import pendulum
from opentelemetry import trace
from opentelemetry.context import attach, create_key
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import Span, SpanProcessor, Tracer as OpenTelemetryTracer, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.trace import Link, NonRecordingSpan, SpanContext, TraceFlags, Tracer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.trace.span import INVALID_SPAN_ID, INVALID_TRACE_ID

from .utils import (
    datetime_to_nano,
    parse_traceparent,
    parse_tracestate,
)

if TYPE_CHECKING:
    from opentelemetry.context.context import Context

log = logging.getLogger(__name__)

_NEXT_ID = create_key("next_id")


class OtelTrace:
    """
    Handle all tracing requirements such as getting the tracer, and starting a new span.

    When OTEL is enabled, the Trace class will be replaced by this class.
    """

    def __init__(
        self,
        span_exporter: OTLPSpanExporter,
        use_simple_processor: bool,
        tag_string: str | None = None,
        otel_service: str | None = None,
        debug: bool = False,
    ):
        self.span_exporter = span_exporter
        self.use_simple_processor = use_simple_processor
        if self.use_simple_processor:
            # With a BatchSpanProcessor, spans are exported at an interval.
            # A task can run fast and finish before spans have enough time to get exported to the collector.
            # When creating spans from inside a task, a SimpleSpanProcessor needs to be used because
            # it exports the spans immediately after they are created.
            log.info("(otel_tracer.__init__) - [SimpleSpanProcessor] is being used")
            self.span_processor: SpanProcessor = SimpleSpanProcessor(self.span_exporter)
        else:
            log.info("(otel_tracer.__init__) - [BatchSpanProcessor] is being used")
            self.span_processor = BatchSpanProcessor(self.span_exporter)
        self.tag_string = tag_string
        self.otel_service = otel_service
        self.resource = Resource.create(attributes={SERVICE_NAME: self.otel_service})
        self.debug = debug

    def get_otel_tracer_provider(
        self,
        trace_id: int | None = None,
        span_id: int | None = None,
    ) -> TracerProvider:
        """
        Tracer that will use special AirflowOtelIdGenerator to control producing certain span and trace id.

        It can be used to get a tracer and directly create spans, or for auto-instrumentation.
        """
        if trace_id or span_id:
            # in case where trace_id or span_id was given
            tracer_provider = TracerProvider(
                resource=self.resource,
                id_generator=AirflowOtelIdGenerator(span_id=span_id, trace_id=trace_id),
            )
        else:
            tracer_provider = TracerProvider(resource=self.resource)
        if self.debug is True:
            log.info("[ConsoleSpanExporter] is being used")
            if self.use_simple_processor:
                log.info("[SimpleSpanProcessor] is being used")
                span_processor_for_tracer_prov: SpanProcessor = SimpleSpanProcessor(ConsoleSpanExporter())
            else:
                log.info("[BatchSpanProcessor] is being used")
                span_processor_for_tracer_prov = BatchSpanProcessor(ConsoleSpanExporter())
        else:
            span_processor_for_tracer_prov = self.span_processor

        tracer_provider.add_span_processor(span_processor_for_tracer_prov)
        return tracer_provider

    def get_tracer(
        self,
        component: str,
        trace_id: int | None = None,
        span_id: int | None = None,
    ) -> OpenTelemetryTracer | Tracer:
        tracer_provider = self.get_otel_tracer_provider(trace_id=trace_id, span_id=span_id)
        tracer = tracer_provider.get_tracer(component)
        """
        Tracer will produce a single ID value if value is provided. Note that this is one-time only, so any
        subsequent call will produce the normal random ids.
        """
        return tracer

    def get_current_span(self):
        return trace.get_current_span()

    def use_span(self, span: Span):
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
        """Start a span; if service_name is not given, otel_service is used."""
        if component is None:
            component = self.otel_service

        trace_id = self.get_current_span().get_span_context().trace_id
        tracer = self.get_tracer(component=component, trace_id=trace_id, span_id=span_id)

        attributes = parse_tracestate(self.tag_string) if self.tag_string else {}

        if links is not None:
            _links = gen_links_from_kv_list(links)
        else:
            _links = []

        if start_time is not None:
            start_time = datetime_to_nano(start_time)

        if parent_sc is not None:
            ctx = trace.set_span_in_context(NonRecordingSpan(parent_sc))
            span = tracer.start_as_current_span(
                span_name, context=ctx, attributes=attributes, links=_links, start_time=start_time
            )
        else:
            span = tracer.start_as_current_span(
                span_name, attributes=attributes, links=_links, start_time=start_time
            )
        return span

    def start_root_span(
        self,
        span_name: str,
        component: str | None = None,
        links=None,
        start_time=None,
        start_as_current: bool = True,
    ):
        """Start a root span."""
        # If no context is passed to the new span,
        # then it will try to get the context of the current active span.
        # Due to that, the context parameter can't be empty.
        # It needs an invalid context in order to declare the new span as root.
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
        if component is None:
            component = self.otel_service

        tracer = self.get_tracer(component=component)

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
        carrier: dict[str, str] = {}
        TraceContextTextMapPropagator().inject(carrier)
        return carrier

    def extract(self, carrier: dict) -> Context:
        """Extract the span context from a provided carrier."""
        return TraceContextTextMapPropagator().extract(carrier)


def gen_context(trace_id: int, span_id: int):
    """Generate a remote span context for given trace and span id."""
    span_ctx = SpanContext(trace_id=trace_id, span_id=span_id, is_remote=True, trace_flags=TraceFlags(0x01))
    return span_ctx


def gen_links_from_kv_list(kv_list):
    """Convert list of kv dic of trace_id and span_id and generate list of SpanContext."""
    result = []
    for a in kv_list:
        trace_id = a["trace_id"]  # string of hexa
        span_id = a["span_id"]  # string of hexa
        span_ctx = gen_context(trace_id, span_id)
        a_link = Link(
            context=span_ctx,
            attributes={"meta.annotation_type": "link"},
        )
        result.append(a_link)
    return result


def gen_link_from_traceparent(traceparent: str):
    """Generate Link object from provided traceparent string."""
    if traceparent is None:
        return None

    trace_ctx = parse_traceparent(traceparent)
    trace_id = trace_ctx["trace_id"]
    span_id = trace_ctx["parent_id"]
    span_ctx = gen_context(int(trace_id, 16), int(span_id, 16))
    return Link(context=span_ctx, attributes={"meta.annotation_type": "link", "from": "traceparent"})


def get_otel_tracer(
    cls,
    use_simple_processor: bool = False,
    *,
    host: str | None = None,
    port: int | None = None,
    ssl_active: bool = False,
    otel_service: str | None = None,
    debug: bool = False,
) -> OtelTrace:
    """Get OTEL tracer from airflow configuration."""
    tag_string = cls.get_constant_tags()

    protocol = "https" if ssl_active else "http"
    # Allow transparent support for standard OpenTelemetry SDK environment variables.
    # https://opentelemetry.io/docs/specs/otel/protocol/exporter/#configuration-options
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", f"{protocol}://{host}:{port}/v1/traces")

    log.info("[OTLPSpanExporter] Connecting to OpenTelemetry Collector at %s", endpoint)
    log.info("Should use simple processor: %s", use_simple_processor)
    return OtelTrace(
        span_exporter=OTLPSpanExporter(endpoint=endpoint),
        use_simple_processor=use_simple_processor,
        tag_string=tag_string,
        otel_service=otel_service,
        debug=debug,
    )


class AirflowOtelIdGenerator(IdGenerator):
    """
    ID Generator for span id and trace id.

    The specific purpose of this ID generator is to generate a given span_id when the
    generate_span_id is called for the FIRST time. Any subsequent calls to the generate_span_id()
    will then fall back into producing random ones. As for the trace_id, the class is designed
    to produce the provided trace id (and not anything random)
    """

    def __init__(self, span_id=None, trace_id=None):
        super().__init__()
        self.span_id = span_id
        self.trace_id = trace_id

    def generate_span_id(self) -> int:
        if self.span_id is not None:
            id = self.span_id
            self.span_id = None
            return id
        new_id = random.getrandbits(64)
        return new_id

    def generate_trace_id(self) -> int:
        if self.trace_id is not None:
            id = self.trace_id
            return id
        new_id = random.getrandbits(128)
        return new_id
