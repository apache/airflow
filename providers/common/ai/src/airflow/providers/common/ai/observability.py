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
OpenTelemetry tracing for pydantic-ai agents created by this provider.

pydantic-ai ships native OpenTelemetry instrumentation that emits GenAI spans
(agent run, model call, tool call) following the OTel GenAI semantic
conventions. This module turns it on and points it at Airflow's existing
OpenTelemetry exporter so agent spans flow to whatever OTLP backend the
deployment already runs, nested under the worker's task span.

It deliberately does not configure an exporter or a ``TracerProvider`` of its
own: it reuses the global SDK provider that core tracing (``[traces] otel_on``)
installs in the worker process. When that provider is absent (core tracing off,
or not configured in this process) no spans are emitted.
"""

from __future__ import annotations

import copy
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, cast

from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from collections.abc import Iterator

    from opentelemetry.trace import Span, Tracer
    from pydantic_ai import Agent
    from pydantic_ai.models.instrumented import InstrumentationSettings

SECTION = "common.ai"

# OTel GenAI semantic-convention format version. Pinned so a change in the
# pydantic-ai default does not silently shift the emitted span/attribute format
# between provider releases. Version 5 is the current default in pydantic-ai 2.x;
# formats 2-4 still work but are deprecated. Note: independent of this version,
# pydantic-ai 2.x reports agent-run token usage under ``gen_ai.aggregated_usage.*``
# (model-request spans keep ``gen_ai.usage.*``) -- see docs/observability.rst.
_SEMCONV_VERSION: Literal[5] = 5


def _otel_export_enabled() -> bool:
    return conf.getboolean(SECTION, "otel_export_enabled", fallback=False)


def _capture_content() -> bool:
    return conf.getboolean(SECTION, "capture_content", fallback=False)


def _live_tracer_provider():
    """
    Return the worker's configured SDK ``TracerProvider``, or ``None``.

    Core tracing installs an ``opentelemetry.sdk`` ``TracerProvider`` via
    ``trace.set_tracer_provider()``. Until then ``get_tracer_provider()``
    returns the API's no-op proxy. Reusing the SDK provider is what makes the
    GenAI spans share the core OTLP exporter and nest under the task span; we
    never install our own. Returns ``None`` when the OpenTelemetry SDK is not
    installed or no real provider is configured in this process.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
    except ImportError:
        return None

    provider = trace.get_tracer_provider()
    return provider if isinstance(provider, TracerProvider) else None


def genai_instrumentation_settings() -> InstrumentationSettings | None:
    """
    Build pydantic-ai ``InstrumentationSettings`` for an agent run.

    Returns ``None`` (leave the agent un-instrumented, zero overhead) when
    export is disabled or no live OTLP ``TracerProvider`` is configured in this
    worker process. ``include_content`` is off by default so prompts,
    completions, and tool IO are never emitted unless explicitly opted in via
    ``[common.ai] capture_content``.
    """
    if not _otel_export_enabled():
        return None
    provider = _live_tracer_provider()
    if provider is None:
        return None

    # Imported here, not at module top: this module is imported by the hook on
    # every agent build, but the ``instrumented`` submodule is only needed when
    # tracing is actually on. Keeping it lazy avoids that cost on the common
    # tracing-off path.
    from pydantic_ai.models.instrumented import InstrumentationSettings

    return InstrumentationSettings(
        version=_SEMCONV_VERSION,
        include_content=_capture_content(),
        include_binary_content=False,
        tracer_provider=provider,
    )


def build_run_identity_attributes(ti: Any) -> dict[str, Any]:
    """
    Build the Airflow identity attributes to stamp on a run's GenAI spans.

    Reuses core's task-span attribute keys (see ``_make_task_span``) so agent
    spans filter identically to the task span they nest under, plus the
    per-attempt task-instance id as the run join key carried on every span.
    """
    return {
        "airflow.dag_id": ti.dag_id,
        "airflow.task_id": ti.task_id,
        "airflow.dag_run.run_id": ti.run_id,
        "airflow.task_instance.try_number": ti.try_number,
        "airflow.task_instance.map_index": ti.map_index if ti.map_index is not None else -1,
        "airflow.task_instance.id": str(ti.id),
    }


def stamp_identity_on_agent_spans(agent: Agent, attributes: dict[str, Any]) -> None:
    """
    Stamp *attributes* on every GenAI span *agent* emits during its run.

    pydantic-ai opens all of a run's agent/model/tool spans from
    ``InstrumentationSettings.tracer``, so wrapping that one tracer reaches them
    all without touching the shared core ``TracerProvider``. No-op when the
    agent is not instrumented with an ``InstrumentationSettings`` (tracing off,
    or the caller supplied its own non-settings ``instrument`` value).

    The settings object may be caller-owned and shared across agents (a single
    module-level ``InstrumentationSettings`` handed to several tasks) or reused
    across HITL re-runs. Mutating it in place would leak one run's identity into
    another and nest ``_IdentityTracer`` wrappers on each stamp, so we wrap a copy
    and swap it onto this agent, leaving the original untouched.
    """
    from pydantic_ai.models.instrumented import InstrumentationSettings

    instrument = agent.instrument
    if isinstance(instrument, InstrumentationSettings):
        # ``tracer`` is not a constructor arg, so copy then set the attribute.
        settings = copy.copy(instrument)
        # _IdentityTracer implements the Tracer surface structurally (it cannot
        # subclass Tracer without importing opentelemetry at module load).
        settings.tracer = cast("Tracer", _IdentityTracer(instrument.tracer, attributes))
        agent.instrument = settings


class _IdentityTracer:
    """OpenTelemetry ``Tracer`` wrapper that stamps fixed attributes on every span it starts."""

    def __init__(self, tracer: Tracer, attributes: dict[str, Any]) -> None:
        self._tracer = tracer
        self._attributes = attributes

    def start_span(self, *args: Any, **kwargs: Any) -> Span:
        span = self._tracer.start_span(*args, **kwargs)
        span.set_attributes(self._attributes)
        return span

    @contextmanager
    def start_as_current_span(self, *args: Any, **kwargs: Any) -> Iterator[Span]:
        with self._tracer.start_as_current_span(*args, **kwargs) as span:
            span.set_attributes(self._attributes)
            yield span

    def __getattr__(self, name: str) -> Any:
        # Delegate any other Tracer method to the wrapped tracer. Dunders are not
        # forwarded (so copy/pickle of the settings does not silently unwrap the
        # stamping), and the ``__dict__`` lookup guards against recursion before
        # ``_tracer`` is set.
        if name.startswith("__"):
            raise AttributeError(name)
        tracer = self.__dict__.get("_tracer")
        if tracer is None:
            raise AttributeError(name)
        return getattr(tracer, name)
