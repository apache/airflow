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
import socket
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

from airflow.configuration import conf
from airflow.typing_compat import Protocol

log = logging.getLogger(__name__)


def gen_context(trace_id, span_id):
    """Generate span context from trace_id and span_id."""
    from airflow.traces.otel_tracer import gen_context as otel_gen_context

    return otel_gen_context(trace_id, span_id)


def gen_links_from_kv_list(list):
    """Generate links from kv list of {trace_id:int, span_id:int}."""
    from airflow.traces.otel_tracer import gen_links_from_kv_list

    return gen_links_from_kv_list(list)


def add_span(func):
    """Decorate a function with span."""
    func_name = func.__name__
    qual_name = func.__qualname__
    module_name = func.__module__
    component = qual_name.rsplit(".", 1)[0] if "." in qual_name else module_name

    @wraps(func)
    def wrapper(*args, **kwargs):
        with Trace.start_span(span_name=func_name, component=component):
            return func(*args, **kwargs)

    return wrapper


class EmptyContext:
    """If no Tracer is configured, EmptyContext is used as a fallback."""

    trace_id = 1


class EmptySpan:
    """If no Tracer is configured, EmptySpan is used as a fallback."""

    def __enter__(self):
        """Enter."""
        return self

    def __exit__(self, *args, **kwargs):
        """Exit."""
        pass

    def __call__(self, obj):
        """Call."""
        return obj

    def get_span_context(self):
        """Get span context."""
        return EMPTY_CTX

    def set_attribute(self, key, value) -> None:
        """Set an attribute to the span."""
        pass

    def set_attributes(self, attributes) -> None:
        """Set multiple attributes at once."""
        pass

    def is_recording(self):
        return False

    def add_event(
        self,
        name: str,
        attributes: Any | None = None,
        timestamp: int | None = None,
    ) -> None:
        """Add event to span."""
        pass

    def add_link(
        self,
        context: Any,
        attributes: Any | None = None,
    ) -> None:
        """Add link to the span."""
        pass

    def end(self, end_time=None, *args, **kwargs) -> None:
        """End."""
        pass


EMPTY_SPAN = EmptySpan()
EMPTY_CTX = EmptyContext()


class Tracer(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, etc)."""

    instance: Tracer | EmptyTrace | None = None

    @classmethod
    def get_tracer(cls, component):
        """Get a tracer."""
        raise NotImplementedError()

    @classmethod
    def start_span(
        cls,
        span_name: str,
        component: str | None = None,
        parent_sc=None,
        span_id=None,
        links=None,
        start_time=None,
    ):
        """Start a span."""
        raise NotImplementedError()

    @classmethod
    def use_span(cls, span):
        """Use a span as current."""
        raise NotImplementedError()

    @classmethod
    def get_current_span(self):
        raise NotImplementedError()

    @classmethod
    def start_span_from_dagrun(
        cls,
        dagrun,
        span_name=None,
        service_name=None,
        component=None,
        links=None,
    ):
        """Start a span from dagrun."""
        raise NotImplementedError()

    @classmethod
    def start_span_from_taskinstance(
        cls,
        ti,
        span_name=None,
        component=None,
        child=False,
        links=None,
    ):
        """Start a span from taskinstance."""
        raise NotImplementedError()


class EmptyTrace:
    """If no Tracer is configured, EmptyTracer is used as a fallback."""

    @classmethod
    def get_tracer(
        cls,
        component: str,
        trace_id: int | None = None,
        span_id: int | None = None,
    ):
        """Get a tracer using provided node id and trace id."""
        return cls

    @classmethod
    def start_span(
        cls,
        span_name: str,
        component: str | None = None,
        parent_sc=None,
        span_id=None,
        links=None,
        start_time=None,
    ) -> EmptySpan:
        """Start a span."""
        return EMPTY_SPAN

    @classmethod
    def use_span(cls, span) -> EmptySpan:
        """Use a span as current."""
        return EMPTY_SPAN

    @classmethod
    def get_current_span(self) -> EmptySpan:
        """Get the current span."""
        return EMPTY_SPAN

    @classmethod
    def start_span_from_dagrun(
        cls,
        dagrun,
        span_name=None,
        service_name=None,
        component=None,
        links=None,
    ) -> EmptySpan:
        """Start a span from dagrun."""
        return EMPTY_SPAN

    @classmethod
    def start_span_from_taskinstance(
        cls,
        ti,
        span_name=None,
        component=None,
        child=False,
        links=None,
    ) -> EmptySpan:
        """Start a span from taskinstance."""
        return EMPTY_SPAN


class _TraceMeta(type):
    factory: Callable[[], Tracer] | None = None
    instance: Tracer | EmptyTrace | None = None

    def __getattr__(cls, name: str):
        if not cls.factory:
            # Lazy initialization of the factory
            cls.configure_factory()
        if not cls.instance:
            cls._initialize_instance()
        return getattr(cls.instance, name)

    def _initialize_instance(cls):
        """Initialize the trace instance."""
        try:
            cls.instance = cls.factory()
        except (socket.gaierror, ImportError) as e:
            log.error("Could not configure Trace: %s. Using EmptyTrace instead.", e)
            cls.instance = EmptyTrace()

    def __call__(cls, *args, **kwargs):
        """Ensure the class behaves as a singleton."""
        if not cls.instance:
            cls._initialize_instance()
        return cls.instance

    @classmethod
    def configure_factory(cls):
        """Configure the trace factory based on settings."""
        if conf.has_option("traces", "otel_on") and conf.getboolean("traces", "otel_on"):
            from airflow.traces import otel_tracer

            cls.factory = otel_tracer.get_otel_tracer
        else:
            # EmptyTrace is a class and not inherently callable.
            # Using a lambda ensures it can be invoked as a callable factory.
            # staticmethod ensures the lambda is treated as a standalone function
            # and avoids passing `cls` as an implicit argument.
            cls.factory = staticmethod(lambda: EmptyTrace())

    @classmethod
    def get_constant_tags(cls) -> str | None:
        """Get constant tags to add to all traces."""
        return conf.get("traces", "tags", fallback=None)


if TYPE_CHECKING:
    Trace: EmptyTrace
else:

    class Trace(metaclass=_TraceMeta):
        """Empty class for Trace - we use metaclass to inject the right one."""
