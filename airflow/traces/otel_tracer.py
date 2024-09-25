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
import random

from opentelemetry import trace
from opentelemetry.context import create_key
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import HOST_NAME, SERVICE_NAME, Resource
from opentelemetry.sdk.trace import Span, Tracer as OpenTelemetryTracer, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.trace import Link, NonRecordingSpan, SpanContext, TraceFlags, Tracer
from opentelemetry.trace.span import INVALID_SPAN_ID, INVALID_TRACE_ID

from airflow.configuration import conf
from airflow.traces import (
    TRACEPARENT,
    TRACESTATE,
)
from airflow.traces.utils import (
    gen_dag_span_id,
    gen_span_id,
    gen_trace_id,
    parse_traceparent,
    parse_tracestate,
)
from airflow.utils.dates import datetime_to_nano
from airflow.utils.net import get_hostname

log = logging.getLogger(__name__)

_NEXT_ID = create_key("next_id")


class OtelTrace:
    """
    Handle all tracing requirements such as getting the tracer, and starting a new span.

    When OTEL is enabled, the Trace class will be replaced by this class.
    """

    def __init__(self, span_exporter: ConsoleSpanExporter | OTLPSpanExporter, tag_string: str | None = None):
        self.span_exporter = span_exporter
        self.span_processor = BatchSpanProcessor(self.span_exporter)
        self.tag_string = tag_string
        self.otel_service = conf.get("traces", "otel_service")

    def get_tracer(
        self, component: str, trace_id: int | None = None, span_id: int | None = None
    ) -> OpenTelemetryTracer | Tracer:
        """Tracer that will use special AirflowOtelIdGenerator to control producing certain span and trace id."""
        resource = Resource(attributes={HOST_NAME: get_hostname(), SERVICE_NAME: self.otel_service})
        if trace_id or span_id:
            # in case where trace_id or span_id was given
            tracer_provider = TracerProvider(
                resource=resource, id_generator=AirflowOtelIdGenerator(span_id=span_id, trace_id=trace_id)
            )
        else:
            tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(self.span_processor)
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

    def start_span_from_dagrun(
        self, dagrun, span_name: str | None = None, component: str = "dagrun", links=None
    ):
        """Produce a span from dag run."""
        # check if dagrun has configs
        conf = dagrun.conf
        trace_id = int(gen_trace_id(dag_run=dagrun, as_int=True))
        span_id = int(gen_dag_span_id(dag_run=dagrun, as_int=True))

        tracer = self.get_tracer(component=component, span_id=span_id, trace_id=trace_id)

        tag_string = self.tag_string if self.tag_string else ""
        tag_string = tag_string + ("," + conf.get(TRACESTATE) if (conf and conf.get(TRACESTATE)) else "")

        if span_name is None:
            span_name = dagrun.dag_id

        _links = gen_links_from_kv_list(links) if links else []

        _links.append(
            Link(
                context=trace.get_current_span().get_span_context(),
                attributes={"meta.annotation_type": "link", "from": "parenttrace"},
            )
        )

        if conf and conf.get(TRACEPARENT):
            # add the trace parent as the link
            _links.append(gen_link_from_traceparent(conf.get(TRACEPARENT)))

        span_ctx = SpanContext(
            trace_id=INVALID_TRACE_ID, span_id=INVALID_SPAN_ID, is_remote=True, trace_flags=TraceFlags(0x01)
        )
        ctx = trace.set_span_in_context(NonRecordingSpan(span_ctx))
        span = tracer.start_as_current_span(
            name=span_name,
            context=ctx,
            links=_links,
            start_time=datetime_to_nano(dagrun.queued_at),
            attributes=parse_tracestate(tag_string),
        )
        return span

    def start_span_from_taskinstance(
        self,
        ti,
        span_name: str | None = None,
        component: str = "taskinstance",
        child: bool = False,
        links=None,
    ):
        """
        Create and start span from given task instance.

        Essentially the span represents the ti itself if child == True, it will create a 'child' span under the given span.
        """
        dagrun = ti.dag_run
        trace_id = int(gen_trace_id(dag_run=dagrun, as_int=True))
        span_id = int(gen_span_id(ti=ti, as_int=True))
        if span_name is None:
            span_name = ti.task_id

        parent_id = span_id if child else int(gen_dag_span_id(dag_run=dagrun, as_int=True))

        span_ctx = SpanContext(
            trace_id=trace_id, span_id=parent_id, is_remote=True, trace_flags=TraceFlags(0x01)
        )

        _links = gen_links_from_kv_list(links) if links else []

        _links.append(
            Link(
                context=SpanContext(
                    trace_id=trace.get_current_span().get_span_context().trace_id,
                    span_id=span_id,
                    is_remote=True,
                    trace_flags=TraceFlags(0x01),
                ),
                attributes={"meta.annotation_type": "link", "from": "parenttrace"},
            )
        )

        if child is False:
            tracer = self.get_tracer(component=component, span_id=span_id, trace_id=trace_id)
        else:
            tracer = self.get_tracer(component=component)

        ctx = trace.set_span_in_context(NonRecordingSpan(span_ctx))
        span = tracer.start_as_current_span(
            name=span_name, context=ctx, start_time=datetime_to_nano(ti.queued_dttm), links=_links
        )
        return span


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


def get_otel_tracer(cls) -> OtelTrace:
    """Get OTEL tracer from airflow configuration."""
    host = conf.get("traces", "otel_host")
    port = conf.getint("traces", "otel_port")
    debug = conf.getboolean("traces", "otel_debugging_on")
    ssl_active = conf.getboolean("traces", "otel_ssl_active")
    tag_string = cls.get_constant_tags()

    if debug is True:
        log.info("[ConsoleSpanExporter] is being used")
        return OtelTrace(span_exporter=ConsoleSpanExporter(), tag_string=tag_string)
    else:
        protocol = "https" if ssl_active else "http"
        endpoint = f"{protocol}://{host}:{port}/v1/traces"
        log.info("[OTLPSpanExporter] Connecting to OpenTelemetry Collector at %s", endpoint)
        return OtelTrace(
            span_exporter=OTLPSpanExporter(endpoint=endpoint, headers={"Content-Type": "application/json"}),
            tag_string=tag_string,
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
        else:
            new_id = random.getrandbits(64)
            return new_id

    def generate_trace_id(self) -> int:
        if self.trace_id is not None:
            id = self.trace_id
            return id
        else:
            new_id = random.getrandbits(128)
            return new_id
