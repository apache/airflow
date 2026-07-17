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

import threading
from typing import TYPE_CHECKING

import pluggy
import structlog
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from ..observability.traces import DEFAULT_TASK_SPAN_DETAIL_LEVEL, TASK_SPAN_DETAIL_LEVEL_KEY

if TYPE_CHECKING:
    from opentelemetry.trace import Span
    from pluggy._hooks import _HookRelay

log = structlog.get_logger(__name__)
tracer = trace.get_tracer(__name__)


def _detail_level(span: Span) -> int:
    raw = span.get_span_context().trace_state.get(TASK_SPAN_DETAIL_LEVEL_KEY)
    if raw is None:
        return DEFAULT_TASK_SPAN_DETAIL_LEVEL
    try:
        return int(raw)
    except (TypeError, ValueError):
        return DEFAULT_TASK_SPAN_DETAIL_LEVEL


_span_state = threading.local()


def _stack() -> list:
    stack = getattr(_span_state, "stack", None)
    if stack is None:
        stack = _span_state.stack = []
    return stack


def _before_hookcall(hook_name, hook_impls, kwargs):
    log.debug("Calling %r with %r", hook_name, kwargs)
    log.debug("Hook impls: %s", hook_impls)
    if not hook_impls or _detail_level(trace.get_current_span()) <= 1:
        _stack().append(None)
        return
    cm = tracer.start_as_current_span(f"listener.{hook_name}")
    span = cm.__enter__()
    _stack().append((cm, span))


def _after_hookcall(outcome, hook_name, hook_impls, kwargs):
    excinfo = getattr(outcome, "excinfo", None)
    if excinfo:
        log.debug("Hook %r raised %s", hook_name, excinfo[0].__name__)
    else:
        log.debug("Result from %r: %s", hook_name, outcome.get_result())
    entry = _stack().pop()
    if entry is None:
        return
    cm, span = entry
    if excinfo:
        exc_type, exc, _tb = excinfo
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, description=f"Exception: {exc_type.__name__}"))
    cm.__exit__(None, None, None)


class ListenerManager:
    """
    Manage listener registration and provides hook property for calling them.

    This class provides base infra for listener system. The consumers / components
    wanting to register listeners should initialise its own ListenerManager and
    register the hook specs relevant to that component using add_hookspecs.
    """

    def __init__(self):
        self.pm = pluggy.PluginManager("airflow")
        self.pm.add_hookcall_monitoring(_before_hookcall, _after_hookcall)

    def add_hookspecs(self, spec_module) -> None:
        """
        Register hook specs from a module.

        :param spec_module: A module containing functions decorated with @hookspec.
        """
        self.pm.add_hookspecs(spec_module)

    @property
    def has_listeners(self) -> bool:
        return bool(self.pm.get_plugins())

    @property
    def hook(self) -> _HookRelay:
        """Return hook, on which plugin methods specified in spec can be called."""
        return self.pm.hook

    def add_listener(self, listener):
        if self.pm.is_registered(listener):
            return
        self.pm.register(listener)

    def clear(self):
        """Remove registered plugins."""
        for plugin in self.pm.get_plugins():
            self.pm.unregister(plugin)
