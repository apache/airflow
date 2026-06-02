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

from typing import TYPE_CHECKING, Literal

from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from pydantic_ai.models.instrumented import InstrumentationSettings

SECTION = "common.ai"

# OTel GenAI semantic-convention attribute set. Pinned so the emitted span and
# ``gen_ai.*`` attribute names stay stable regardless of the pydantic-ai default
# (which tracks the latest, still-evolving revision).
_SEMCONV_VERSION: Literal[4] = 4


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
