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
Make the OpenTelemetry spans of an agent framework other than Pydantic AI part of the task.

.. note:: Experimental; see :mod:`airflow.providers.common.ai.tools`.
"""

from __future__ import annotations

import os
import threading
import weakref
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from opentelemetry import context as otel_context, trace
from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from airflow.providers.common.ai.observability import (
    _capture_content,
    _live_tracer_provider,
    _otel_export_enabled,
    build_run_identity_attributes,
)
from airflow.providers.common.compat.sdk import get_current_context

if TYPE_CHECKING:
    from collections.abc import Iterator

    from opentelemetry.context import Context
    from opentelemetry.sdk.trace import Span

__all__ = ["agent_framework_tracing"]

# Spans started while this is set carry the task's identity. A span processor cannot be
# removed from a provider once added, so it is added once and does nothing outside the block.
_task_identity: ContextVar[dict[str, Any] | None] = ContextVar("common_ai_task_identity", default=None)
_providers_with_identity: weakref.WeakSet[Any] = weakref.WeakSet()

# The switches each framework reads to leave prompts, completions, and tool arguments and
# results out of its spans. Strands redacts every sensitive attribute when its
# ``gen_ai_unredacted_attributes`` token names none; ADK and OpenTelemetry's GenAI
# instrumentations read the other two.
_OPT_IN = "OTEL_SEMCONV_STABILITY_OPT_IN"
_REDACT_ALL = "gen_ai_unredacted_attributes="
_CONTENT_OFF = {
    "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT": "false",
    "ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS": "false",
}


@contextmanager
def agent_framework_tracing() -> Iterator[None]:
    """
    Make the OpenTelemetry spans of a Strands or Google ADK agent part of the Airflow task.

    Build and run the agent inside the block::

        with agent_framework_tracing():
            agent = Agent(model=model, plugins=[AirflowTools(warehouse)])
            answer = agent(question)

    Inside it:

    - Prompts, completions, and tool arguments and results are left out of the
      framework's spans unless ``[common.ai] otel_export_enabled`` and ``capture_content``
      are both on, as for ``AgentOperator``. A switch the deployment already set in the
      environment wins. Strands reads its switch once, when it creates its tracer, and
      ADK when a ``TelemetryConfig`` is built, so create both inside the block.
    - Every span started under the worker's OpenTelemetry tracer provider carries the
      task's Dag ID, run ID, task ID, map index, try number and task instance ID, the
      attributes ``AgentOperator``'s spans carry.
    - When no tracer provider made a span for the task, the Dag run's trace context is
      not made the parent of the framework's spans. That context is marked as not
      sampled, and a parent-based sampler, the OpenTelemetry default, would otherwise
      drop every span a tracer provider the framework installs starts. When core tracing
      or auto-instrumentation made the task's span, the Dag run's sampling decision
      holds.

    Where the spans go is up to the tracer provider: core tracing's exporter when
    ``[traces] otel_on`` is set, or the provider the framework's own telemetry setup
    installs.
    """
    provider = _live_tracer_provider()
    if provider is not None and provider not in _providers_with_identity:
        provider.add_span_processor(_TaskIdentityProcessor())
        _providers_with_identity.add(provider)

    ti = get_current_context()["ti"]
    identity = _task_identity.set(build_run_identity_attributes(ti))
    detach = None
    try:
        current = trace.get_current_span().get_span_context()
        if not current.trace_flags.sampled and _is_propagated_parent(current, ti):
            # Only the span is replaced, so baggage and the instrumentation-suppression key
            # stay in place.
            detach = otel_context.attach(trace.set_span_in_context(trace.INVALID_SPAN))
        with _content_off:
            yield
    finally:
        if detach is not None:
            otel_context.detach(detach)
        _task_identity.reset(identity)


def _content_switches() -> dict[str, str]:
    # The same rule as AgentOperator: content is captured only when both settings are on.
    if _otel_export_enabled() and _capture_content():
        return {}
    switches = {name: value for name, value in _CONTENT_OFF.items() if name not in os.environ}
    opt_in = os.environ.get(_OPT_IN, "")
    if _REDACT_ALL not in opt_in:
        switches[_OPT_IN] = ",".join(filter(None, (opt_in, _REDACT_ALL)))
    return switches


def _is_propagated_parent(current: trace.SpanContext, ti: Any) -> bool:
    """
    Whether the current span is the Dag run's propagated context rather than a span of the task.

    It is when no tracer provider made a span for the task, and the core still made the
    propagated context current, as Airflow 3.2's task span does with the no-op tracer.
    Airflow 3.0 and 3.1 propagate no context to the task.
    """
    carrier = getattr(ti, "context_carrier", None)
    if not current.is_valid or not carrier:
        return False
    propagated = trace.get_current_span(TraceContextTextMapPropagator().extract(carrier)).get_span_context()
    return (current.trace_id, current.span_id) == (propagated.trace_id, propagated.span_id)


class _ContentOff:
    """
    Hold the content-off switches in the process environment while any block is open.

    The frameworks read the switches only from the environment. Blocks can overlap, when a
    task runs agents in threads or concurrent coroutines, so the switches are set when the
    first block opens and restored when the last one closes, not by whichever exits first.
    Changing the environment is safe here because a task runs in a process of its own.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._open_blocks = 0
        self._previous: dict[str, str | None] = {}

    def __enter__(self) -> None:
        with self._lock:
            if self._open_blocks == 0:
                switches = _content_switches()
                self._previous = {name: os.environ.get(name) for name in switches}
                os.environ.update(switches)
            self._open_blocks += 1

    def __exit__(self, *args: object) -> None:
        with self._lock:
            self._open_blocks -= 1
            if self._open_blocks:
                return
            for name, value in self._previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


_content_off = _ContentOff()


class _TaskIdentityProcessor(SpanProcessor):
    """Stamp the task's identity on spans started inside ``agent_framework_tracing``."""

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        if (identity := _task_identity.get()) is not None:
            span.set_attributes(identity)
