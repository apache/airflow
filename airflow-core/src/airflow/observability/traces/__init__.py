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
from contextlib import contextmanager
from importlib.metadata import entry_points

from opentelemetry import context, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from airflow.configuration import conf

log = logging.getLogger(__name__)

OVERRIDE_SPAN_ID_KEY = context.create_key("override_span_id")
OVERRIDE_TRACE_ID_KEY = context.create_key("override_trace_id")


class OverrideableRandomIdGenerator(RandomIdGenerator):
    """Lets you override the span id."""

    def generate_span_id(self):
        override = context.get_value(OVERRIDE_SPAN_ID_KEY)
        if override is not None:
            return override
        return super().generate_span_id()

    def generate_trace_id(self):
        override = context.get_value(OVERRIDE_TRACE_ID_KEY)
        if override is not None:
            return override
        return super().generate_trace_id()


def new_dagrun_trace_carrier() -> dict[str, str]:
    """Generate a fresh W3C traceparent carrier without creating a recordable span."""
    gen = RandomIdGenerator()
    span_ctx = SpanContext(
        trace_id=gen.generate_trace_id(),
        span_id=gen.generate_span_id(),
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    ctx = trace.set_span_in_context(NonRecordingSpan(span_ctx))
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier, context=ctx)
    return carrier


@contextmanager
def override_ids(trace_id, span_id, ctx=None):
    ctx = context.set_value(OVERRIDE_TRACE_ID_KEY, trace_id, context=ctx)
    ctx = context.set_value(OVERRIDE_SPAN_ID_KEY, span_id, context=ctx)
    token = context.attach(ctx)
    try:
        yield
    finally:
        context.detach(token)


def _get_backcompat_config() -> tuple[str | None, Resource | None]:
    """
    Possibly get deprecated Airflow configs for otel.

    Ideally we return (None, None) here.  But if the old configuration is there,
    then we will use it.
    """
    resource = None
    if not os.environ.get("OTEL_SERVICE_NAME") and not os.environ.get("OTEL_RESOURCE_ATTRIBUTES"):
        service_name = conf.get("traces", "otel_service", fallback=None)
        if service_name:
            resource = Resource({"service.name": service_name})

    endpoint = None
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") and not os.environ.get(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
    ):
        # this is only for backcompat!
        host = conf.get("traces", "otel_host", fallback=None)
        port = conf.get("traces", "otel_port", fallback=None)
        ssl_active = conf.getboolean("traces", "otel_ssl_active", fallback=False)
        if host and port:
            scheme = "https" if ssl_active else "http"
            endpoint = f"{scheme}://{host}:{port}/v1/traces"
    return endpoint, resource


def _load_exporter_from_env() -> SpanExporter:
    """
    Load a span exporter using the OTEL_TRACES_EXPORTER env var.

    Mirrors the entry-point mechanism used by the OTEL SDK auto-instrumentation
    configurator.  Supported values (from installed packages):
      - ``otlp`` (default) — OTLP/gRPC
      - ``otlp_proto_http`` — OTLP/HTTP
      - ``console`` — stdout (useful for debugging)
    """
    exporter_name = os.environ.get("OTEL_TRACES_EXPORTER", "otlp")
    eps = entry_points(group="opentelemetry_traces_exporter", name=exporter_name)
    ep = next(iter(eps), None)
    if ep is None:
        raise RuntimeError(
            f"No span exporter found for OTEL_TRACES_EXPORTER={exporter_name!r}. "
            f"Available: {[e.name for e in entry_points(group='opentelemetry_traces_exporter')]}"
        )
    return ep.load()()


def configure_otel():
    otel_on = conf.getboolean("traces", "otel_on", fallback=False)
    if not otel_on:
        return

    # ideally both endpoint and resource are None here
    # they would only be something other than None if user is using deprecated
    # Airflow-defined otel configs
    backcompat_endpoint, resource = _get_backcompat_config()

    # backcompat: if old-style host/port config provided an endpoint, set the
    # env var so the exporter (loaded below) picks it up automatically

    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    otlp_traces_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
    if backcompat_endpoint and not (otlp_endpoint or otlp_traces_endpoint):
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = backcompat_endpoint

    provider = TracerProvider(id_generator=OverrideableRandomIdGenerator(), resource=resource)
    provider.add_span_processor(BatchSpanProcessor(_load_exporter_from_env()))
    trace.set_tracer_provider(provider)
