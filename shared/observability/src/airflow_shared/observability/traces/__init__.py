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
from typing import TYPE_CHECKING

from opentelemetry import context, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.sdk.trace.sampling import Decision
from opentelemetry.trace import NonRecordingSpan, Span, SpanContext, TraceFlags, TraceState
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

if TYPE_CHECKING:
    from configparser import ConfigParser
log = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

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


TASK_SPAN_DETAIL_LEVEL_KEY = "airflow/task_span_detail_level"
DEFAULT_TASK_SPAN_DETAIL_LEVEL = 1
TRACE_SAMPLED_KEY = "airflow/trace_sampled"
DAGRUN_PARENT_TRACE_CONTEXT_KEY = "airflow/dagrun_parent_trace_context"


def new_dagrun_trace_carrier(
    task_span_detail_level=None, attributes=None, force_sampled=None, parent_context=None
) -> dict[str, str]:
    """
    Generate a fresh W3C traceparent carrier without creating a recordable span.

    The SAMPLED flag is set from an honest *root* sampling decision made by the
    configured tracer provider's sampler (driven by ``OTEL_TRACES_SAMPLER`` /
    ``OTEL_TRACES_SAMPLER_ARG``), rather than being hardcoded. This makes the
    carrier the head-sampling decision point for a DAG run: every downstream span
    (dag_run, task_run, worker) rides on this flag.

    ``attributes`` are forwarded to the sampler as ``should_sample`` attributes so
    a custom sampler can differentiate the decision by run kind (e.g. by
    ``airflow.dag_id`` / ``airflow.dag_run.run_type``). The built-in samplers ignore them.
    They are decision input only -- they are not persisted in the carrier.

    ``force_sampled`` overrides the sampler entirely: when not None it sets the
    SAMPLED flag directly (True = always trace this run, False = never) and the
    sampler is not consulted. Airflow wires this from the ``airflow/trace_sampled``
    run conf key; when None the configured sampler makes the decision.

    ``parent_context`` optionally embeds the run in an *external* trace: when it
    carries a valid span the carrier reuses that trace_id (so the whole run --
    dag_run, task_run, worker spans -- lives inside the external trace) and the
    sampling decision is made with that parent, so parent-based samplers inherit
    the external SAMPLED flag. Airflow wires this from the
    ``airflow/dagrun_parent_trace_context`` run conf key; when None the run is a root
    trace as before.
    """
    parent_span_context = (
        trace.get_current_span(context=parent_context).get_span_context() if parent_context else None
    )
    gen = RandomIdGenerator()
    if parent_span_context is not None and parent_span_context.is_valid:
        # Embed in the external trace: ride its trace_id, inherit its tracestate, and
        # let the sampler decide with this parent (so parent-based samplers follow it).
        trace_id = parent_span_context.trace_id
        parent_trace_state: TraceState | None = parent_span_context.trace_state
        sampler_parent_context = parent_context
    else:
        trace_id = gen.generate_trace_id()
        parent_trace_state = None
        sampler_parent_context = None

    if force_sampled is not None:
        sampled = force_sampled
        sampler_trace_state = None
    else:
        provider = trace.get_tracer_provider()
        sampler = getattr(provider, "sampler", None)
        if sampler is not None:
            result = sampler.should_sample(
                parent_context=sampler_parent_context,  # None => root decision; else inherit parent
                trace_id=trace_id,
                name="dag_run",
                attributes=attributes or {},
            )
            sampled = result.decision == Decision.RECORD_AND_SAMPLE
            sampler_trace_state = result.trace_state
        else:
            # No sampler attribute means a proxy/no-op provider (otel disabled).
            # Nothing exports in that case, so the flag is irrelevant; mirror the
            # observable behavior of today when otel is off.
            sampled = False
            sampler_trace_state = None

    # Preserve the detail-level tracestate by merging it onto whatever the sampler
    # returned, falling back to the external parent's tracestate when embedding
    # without a sampler decision. TraceState is immutable, so update() returns a new one.
    trace_state = sampler_trace_state
    if trace_state is None and parent_trace_state is not None:
        trace_state = parent_trace_state
    trace_state = trace_state or TraceState()
    for key, value in build_trace_state_entries(task_span_detail_level):
        trace_state = trace_state.update(key, value)

    span_ctx = SpanContext(
        trace_id=trace_id,
        span_id=gen.generate_span_id(),
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED if sampled else 0),
        trace_state=trace_state,
    )
    ctx = trace.set_span_in_context(NonRecordingSpan(span_ctx))
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier, context=ctx)
    return carrier


def new_task_run_carrier(dag_run_context_carrier):
    parent_context = (
        TraceContextTextMapPropagator().extract(dag_run_context_carrier) if dag_run_context_carrier else None
    )
    span = tracer.start_span("notused", context=parent_context)  # intentionally never closed
    new_ctx = trace.set_span_in_context(span)
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier, context=new_ctx)
    return carrier


def build_trace_state_entries(task_span_detail_level) -> list[tuple[str, str]]:
    trace_state_entries = []
    if task_span_detail_level is not None:
        try:
            level = int(task_span_detail_level)
        except (TypeError, ValueError):
            level = None
        if level:
            trace_state_entries.append((TASK_SPAN_DETAIL_LEVEL_KEY, str(level)))
    return trace_state_entries


def get_task_span_detail_level(span: Span):
    span_ctx = span.get_span_context()
    trace_state = span_ctx.trace_state
    raw = trace_state.get(TASK_SPAN_DETAIL_LEVEL_KEY)
    if raw is None:
        return DEFAULT_TASK_SPAN_DETAIL_LEVEL
    try:
        return int(raw)
    except (TypeError, ValueError):
        log.warning("%s config in dag run conf must be integer.", TASK_SPAN_DETAIL_LEVEL_KEY)
        return DEFAULT_TASK_SPAN_DETAIL_LEVEL


@contextmanager
def override_ids(trace_id, span_id, ctx=None):
    ctx = context.set_value(OVERRIDE_TRACE_ID_KEY, trace_id, context=ctx)
    ctx = context.set_value(OVERRIDE_SPAN_ID_KEY, span_id, context=ctx)
    token = context.attach(ctx)
    try:
        yield
    finally:
        context.detach(token)


def _get_backcompat_config(conf: ConfigParser) -> tuple[str | None, Resource | None]:
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


def configure_otel(conf: ConfigParser):
    otel_on = conf.getboolean("traces", "otel_on", fallback=False)
    if not otel_on:
        return

    # ideally both endpoint and resource are None here
    # they would only be something other than None if user is using deprecated
    # Airflow-defined otel configs
    backcompat_endpoint, resource = _get_backcompat_config(conf)

    # backcompat: if old-style host/port config provided an endpoint, set the
    # env var so the exporter (loaded below) picks it up automatically

    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    otlp_traces_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
    if backcompat_endpoint and not (otlp_endpoint or otlp_traces_endpoint):
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = backcompat_endpoint

    provider = TracerProvider(id_generator=OverrideableRandomIdGenerator(), resource=resource)
    provider.add_span_processor(BatchSpanProcessor(_load_exporter_from_env()))
    trace.set_tracer_provider(provider)
