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
from typing import Iterator
from contextlib import contextmanager
from opentelemetry.trace.span import INVALID_SPAN_ID, INVALID_TRACE_ID
from opentelemetry import context as context_api
from opentelemetry.context.context import Context
from opentelemetry.context import create_key, get_value, set_value
from opentelemetry.trace import (
    format_span_id,
    format_trace_id,
    Link,
)
from opentelemetry.sdk.trace import Event
from airflow.configuration import conf
from airflow.utils.net import get_hostname
from airflow.traces import (
    TRACEPARENT,
    TRACESTATE,
)
"""
from airflow.models import (
    DagRun,
    TaskInstance,
)
"""
from airflow.traces.utils import (
    parse_traceparent, parse_tracestate, 
    gen_trace_id, 
    gen_dag_span_id,
    gen_span_id,
)

from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState
from opentelemetry.sdk.trace import Span
from opentelemetry.sdk.trace import Tracer as OpenTelemetryTracer
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, HOST_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry import context

log = logging.getLogger(__name__)

_NEXT_ID = create_key("next_id")

class OtelTrace:
    def __init__(self, span_exporter, tags=None):
        self.span_exporter = span_exporter
        self.span_processor = BatchSpanProcessor(self.span_exporter)
        self.tags = tags
        self.otel_service = conf.get('traces','otel_service')

    def get_tracer(self, component:str) -> OpenTelemetryTracer:
        """get tracer from a given component"""
        resource = Resource(attributes={
            HOST_NAME: get_hostname(),
            SERVICE_NAME: self.otel_service
        })
        tracer_provider = TracerProvider(resource=resource)
        # span_processor = BatchSpanProcessor(self.span_exporter)
        tracer_provider.add_span_processor(self.span_processor)
        tracer = tracer_provider.get_tracer(component)
        return tracer
    
    def get_tracer_with_id(self, component:str, trace_id:int=None, span_id:int=None) -> OpenTelemetryTracer:
        """tracer that will use special AirflowOtelIdGenerator to control producing certain span and trace id"""
        resource = Resource(attributes={
            HOST_NAME: get_hostname(),
            SERVICE_NAME: self.otel_service
        })
        tracer_provider = TracerProvider(resource=resource, id_generator=AirflowOtelIdGenerator(span_id=span_id, trace_id=trace_id))
        # span_processor = BatchSpanProcessor(self.span_exporter, schedule_delay_millis=1)
        tracer_provider.add_span_processor(self.span_processor)
        tracer = tracer_provider.get_tracer(component)
        """tracer will product a Single ID value if value is provided. Note that this is one-time only, so any
           subsequent call will produce the normal random ids
        """
        return tracer
    
    def get_current_span(self) -> Span:
        return trace.get_current_span()
    
    def use_span(self, span:Span) -> Span:
        return trace.use_span(span=span)

    def start_span(self, span_name:str, component:str=None, parent_sc:SpanContext=None, span_id=None, links=None, start_time=None) -> Span:
        """start a span. if service_name is not given, otel_service is used"""
        if component is None:
            component = self.otel_service

        trace_id = self.get_current_span().get_span_context().trace_id
        if span_id is not None:
            tracer = self.get_tracer_with_id(component=component, trace_id=trace_id, span_id=span_id)
        else:
            tracer = self.get_tracer(component)

        kvs = {}
        if self.tags is not None:
            kvs = parse_tracestate(self.tags)

        if links is not None:
            _links = gen_links_from_kv_list(links)
        else:
            _links = []

        if start_time is not None:
            start_time = int(start_time.timestamp() * 1000000000)

        if parent_sc is not None:
            ctx = trace.set_span_in_context(NonRecordingSpan(parent_sc))
            span = tracer.start_as_current_span(span_name, context=ctx, attributes=kvs, links=_links, start_time=start_time)
        else:
            span = tracer.start_as_current_span(span_name, attributes=kvs, links=_links, start_time=start_time)
        return span

    def start_span_from_dagrun(self, dagrun, span_name:str=None, component:str='dagrun', links=None) -> Span:
        """Produce a span from dag run"""
        # check if dagrun has configs
        conf = dagrun.conf
        trace_id = int(gen_trace_id(dag_run=dagrun), 16)
        span_id = int(gen_dag_span_id(dag_run=dagrun), 16)

        if conf is not None:
            traceparent = conf.get(TRACEPARENT)
            tracestate = conf.get(TRACESTATE)

        tracer = self.get_tracer_with_id(component=component, span_id=span_id, trace_id=trace_id)

        # merge attributes from tags and tracestate
        kvstr = None
        if self.tags is not None:
            kvstr = self.tags
        if tracestate is not None:
            if kvstr is None:
                kvstr = tracestate
            else:
                kvstr = kvstr + "," + tracestate
        kvs = parse_tracestate(kvstr)

        if span_name is None:
            span_name = dagrun.dag_id

        if links is not None:
            _links = gen_links_from_kv_list(links)
        else:
            _links = []

        a_link = Link(
            context=trace.get_current_span().get_span_context(),
            attributes={
                'meta.annotation_type': 'link',
                'from': 'parenttrace'
            },
        )
        _links.append(a_link)

        if traceparent is not None:
            # add the trace parent as linkages
            tp_link = gen_link_from_traceparent(traceparent)
            _links.append(tp_link)

        span_ctx = SpanContext(
            trace_id = INVALID_TRACE_ID,
            span_id = INVALID_SPAN_ID,
            is_remote = True,
            trace_flags = TraceFlags(0x01)
        )
        ctx = trace.set_span_in_context(NonRecordingSpan(span_ctx))
        span = tracer.start_as_current_span(name=span_name, context=ctx, links=_links, start_time=int(dagrun.queued_at.timestamp() * 1000000000), attributes=kvs)
        return span

    def start_span_from_taskinstance(self, ti, span_name:str=None, component:str='taskinstance', child:bool=False, links=None) -> Span:
        """create and start span from given task instance. essentially the span represents the ti itself
           if child == True, it will create a 'child' span under the given span
        """
        dagrun = ti.dag_run
        trace_id = int(gen_trace_id(dag_run=dagrun), 16)
        span_id = int(gen_span_id(ti=ti), 16)
        span_name = ti.task_id

        if child is False:
            parent_id = int(gen_dag_span_id(dag_run=dagrun), 16)
        else:
            parent_id = span_id
        
        span_ctx = SpanContext(
            trace_id = trace_id,
            span_id = parent_id,
            is_remote = True,
            trace_flags = TraceFlags(0x01)
        )

        if links is not None:
            _links = gen_links_from_kv_list(links)
        else:
            _links = []

        a_link = Link(
            context=trace.get_current_span().get_span_context(),
            attributes={
                'meta.annotation_type': 'link',
                'from': 'parenttrace'
            },
        )
        _links.append(a_link)

        if child is False:
            tracer = self.get_tracer_with_id(component=component, span_id=span_id, trace_id=trace_id)
        else:
            tracer = self.get_tracer(component=component)

        ctx = trace.set_span_in_context(NonRecordingSpan(span_ctx))
        span = tracer.start_as_current_span(name=span_name, context=ctx, start_time=int(ti.queued_dttm.timestamp() * 1000000000), links=_links)
        return span

def gen_context(trace_id, span_id):
    span_ctx = SpanContext(
        trace_id = trace_id,
        span_id = span_id,
        is_remote = True,
        trace_flags = TraceFlags(0x01)
    )
    return span_ctx

def gen_links_from_kv_list(kv_list):
    """convert list of kv dic of trace_id and span_id and generate list of SpanContext"""
    result = []
    for a in kv_list:
        trace_id = a['trace_id']    # string of hexa
        span_id = a['span_id']      # string of hexa
        span_ctx = gen_context(trace_id, span_id)
        a_link = Link(
            context=span_ctx,
            attributes={
                'meta.annotation_type': 'link'
            },
        )
        result.append(a_link)
    return result

def gen_link_from_traceparent(traceparent):
    if traceparent is not None:
        trace_ctx = parse_traceparent(traceparent)
        trace_id = trace_ctx['trace_id']
        span_id = trace_ctx['parent_id']
        span_ctx = gen_context(int(trace_id, 16), int(span_id, 16))
        a_link = Link(
            context=span_ctx,
            attributes={
                'meta.annotation_type': 'link',
                'from': 'traceparent'
            }
        )
        return a_link
    return None

def get_otel_tracer(cls) -> OtelTrace:
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import(
        OTLPSpanExporter
    )
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter
    
    host=conf.get('traces', 'otel_host')
    port=conf.getint('traces', 'otel_port')
    debug=conf.getboolean('traces','otel_debugging_on')
    ssl_active = conf.getboolean("traces", "otel_ssl_active")
    tags=cls.get_constant_tags()

    if debug == True:
        logging.info("[ConsoleSpanExporter] is being used")
        span_exporter = ConsoleSpanExporter()
    else:
        protocol = "https" if ssl_active else "http"
        endpoint = f"{protocol}://{host}:{port}/v1/traces"
        logging.info("[OTLPSpanExporter] Connecting to OpenTelemetry Collector at %s", endpoint)
        span_exporter = OTLPSpanExporter(endpoint=endpoint, headers={"Content-Type": "application/json"})

    return OtelTrace(span_exporter, tags=tags)

from opentelemetry.sdk.trace.id_generator import RandomIdGenerator, IdGenerator
import random

class AirflowOtelIdGenerator(IdGenerator):

    def __init__(self, span_id:int=None, trace_id:int=None):
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

