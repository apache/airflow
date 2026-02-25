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
from contextlib import contextmanager

from opentelemetry import context, trace
from opentelemetry.context import create_key
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator

log = logging.getLogger(__name__)

_NEXT_ID = create_key("next_id")


OVERRIDE_SPAN_ID_KEY = context.create_key("override_span_id")
OVERRIDE_TRACE_ID_KEY = context.create_key("override_trace_id")


class OverrideableRandomIdGenerator(RandomIdGenerator):
    """Lets you override the span id."""

    def generate_span_id(self):
        override = context.get_value(OVERRIDE_SPAN_ID_KEY)
        if override is not None:
            context.attach(context.set_value(OVERRIDE_SPAN_ID_KEY, None))
            return override
        return super().generate_span_id()

    def generate_trace_id(self):
        override = context.get_value(OVERRIDE_TRACE_ID_KEY)
        if override is not None:
            context.attach(context.set_value(OVERRIDE_TRACE_ID_KEY, None))
            return override
        return super().generate_trace_id()


@contextmanager
def override_ids(trace_id, span_id, ctx=None):
    ctx = context.set_value(OVERRIDE_TRACE_ID_KEY, trace_id, context=ctx)
    ctx = context.set_value(OVERRIDE_SPAN_ID_KEY, span_id, context=ctx)
    token = context.attach(ctx)
    try:
        yield
    finally:
        context.detach(token)


#
#
# port = conf.getint("traces", "otel_port", fallback=None)
# host = conf.get("traces", "otel_host", fallback=None)
# ssl_active = conf.getboolean("traces", "otel_ssl_active", fallback=False)
# otel_service = conf.get("traces", "otel_service", fallback=None)

# resource = Resource.create(attributes={SERVICE_NAME: otel_service})
# otel_env_config = load_traces_env_config()


def configure_otel():
    provider = TracerProvider(id_generator=OverrideableRandomIdGenerator())
    exporter = OTLPSpanExporter()
    span_processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(span_processor)
    trace.set_tracer_provider(provider)
