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

import atexit
import logging
import threading
from typing import TYPE_CHECKING, Any, Literal

from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from pydantic_ai.models.instrumented import InstrumentationSettings

log = logging.getLogger(__name__)

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


def _trace_store_path() -> str:
    return conf.get(SECTION, "trace_store_path", fallback="")


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


# One store stream/provider per task instance try, shared across every agent
# built in that task; closed at process exit (task processes are per-task).
# The lock makes the cache's check-then-create atomic: two agents built
# concurrently for the same TI must not both open the file with mode "w" (the
# second would truncate the first's spans) -- exactly one provider/stream is
# created and cached per key.
_STORE_PROVIDERS: dict[tuple[Any, ...], Any] = {}
_STORE_PROVIDERS_LOCK = threading.Lock()
_ATEXIT_REGISTERED = False


def _close_store_providers() -> None:
    for provider, stream in _STORE_PROVIDERS.values():
        try:
            provider.shutdown()
        except Exception:
            log.exception("Failed to shut down trace-store provider")
        try:
            stream.close()
        except Exception:
            log.exception("Failed to close trace-store stream")
    _STORE_PROVIDERS.clear()


def _store_tracer_provider():
    """
    Build (or reuse) a private ``TracerProvider`` writing to the trace store.

    Backend-free local-dev mode: when ``[common.ai] trace_store_path`` is set,
    GenAI spans are written as standard OTLP JSON lines (the small
    spec-compliant encoder in ``_otlp_json.py`` over an ``ObjectStoragePath``
    stream) under a task-instance-keyed layout::

        {store} / {dag_id} / {run_id} / {task_id} / {map_index} / {try_number}.jsonl

    The path IS the correlation, so this works with core tracing
    (``[traces] otel_on``) completely off -- no collector, no backend. When
    core tracing is on, the ambient task-span context still parents these
    spans, so trace ids line up with ``context_carrier`` too. Files are plain
    OTLP JSON: a collector's ``otlpjsonfilereceiver`` can replay them into any
    real backend later.
    """
    store = _trace_store_path()
    if not store:
        return None
    try:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.sampling import ALWAYS_ON

        from airflow.providers.common.ai._otlp_json import OTLPJsonStreamExporter
        from airflow.sdk import ObjectStoragePath, get_current_context
    except ImportError:
        log.warning(
            "[common.ai] trace_store_path is set but the OpenTelemetry SDK is not "
            "installed; install 'opentelemetry-sdk' to enable the trace store."
        )
        return None
    try:
        ti = get_current_context()["ti"]
    except Exception:
        # Not inside a task (e.g. parsing) -- nothing to key the store by.
        return None

    key = (ti.dag_id, ti.run_id, ti.task_id, ti.map_index, ti.try_number)
    # Lock the whole check-then-create so concurrent agent builds for the same
    # TI can't both open the file "w" (truncating each other) -- the second
    # caller sees the first's cached provider and reuses its stream.
    with _STORE_PROVIDERS_LOCK:
        cached = _STORE_PROVIDERS.get(key)
        if cached is not None:
            return cached[0]

        path = (
            ObjectStoragePath(store)
            / ti.dag_id
            / ti.run_id
            / ti.task_id
            / str(ti.map_index)
            / f"{ti.try_number}.jsonl"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        stream = path.open("w")
        # ALWAYS_ON, not the default ParentBased: the task runner attaches the
        # TI's context_carrier traceparent even when core tracing is off, and
        # that carrier has trace-flags 00 (unsampled). A parent-based sampler
        # would inherit the "don't sample" decision and record nothing.
        provider = TracerProvider(sampler=ALWAYS_ON)
        # Simple (per-span, synchronous) rather than Batch: nothing buffers in
        # the processor, so a task that dies mid-run has already written every
        # span that finished.
        provider.add_span_processor(SimpleSpanProcessor(OTLPJsonStreamExporter(stream)))

        global _ATEXIT_REGISTERED
        if not _ATEXIT_REGISTERED:
            atexit.register(_close_store_providers)
            _ATEXIT_REGISTERED = True
        _STORE_PROVIDERS[key] = (provider, stream)
    log.info("common.ai trace store active: writing GenAI spans to %s", path)
    return provider


def genai_instrumentation_settings() -> InstrumentationSettings | None:
    """
    Build pydantic-ai ``InstrumentationSettings`` for an agent run.

    Two sources, in order: the ObjectStorage trace store (when
    ``[common.ai] trace_store_path`` is set -- zero-infra local dev, needs no
    core tracing), else Airflow's live OTLP ``TracerProvider`` (when
    ``otel_export_enabled`` and core tracing are on). Returns ``None`` (agent
    left un-instrumented, zero overhead) when neither applies.
    ``include_content`` is off by default so prompts, completions, and tool IO
    are never emitted unless explicitly opted in via
    ``[common.ai] capture_content``.
    """
    provider = _store_tracer_provider()
    if provider is None:
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
