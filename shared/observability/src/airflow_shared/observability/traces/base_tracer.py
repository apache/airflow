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

from typing import TYPE_CHECKING, Any, Protocol

import structlog

if TYPE_CHECKING:
    from airflow.typing_compat import Self

log = structlog.getLogger(__name__)


def gen_context(trace_id, span_id):
    """Generate span context from trace_id and span_id."""
    from .otel_tracer import gen_context as otel_gen_context

    return otel_gen_context(trace_id, span_id)


def gen_links_from_kv_list(list):
    """Generate links from kv list of {trace_id:int, span_id:int}."""
    from .otel_tracer import gen_links_from_kv_list

    return gen_links_from_kv_list(list)


class EmptyContext:
    """If no Tracer is configured, EmptyContext is used as a fallback."""

    trace_id = 1
    span_id = 1


class EmptySpan:
    """If no Tracer is configured, EmptySpan is used as a fallback."""

    def __enter__(self) -> Self:
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
    def start_root_span(cls, span_name=None, component=None, start_time=None, start_as_current=True):
        """Start a root span."""
        raise NotImplementedError()

    @classmethod
    def start_child_span(
        cls,
        span_name=None,
        parent_context=None,
        component=None,
        links=None,
        start_time=None,
        start_as_current=True,
    ):
        """Start a child span."""
        raise NotImplementedError()

    @classmethod
    def inject(cls) -> dict:
        """Inject the current span context into a carrier and return it."""
        raise NotImplementedError()

    @classmethod
    def extract(cls, carrier) -> EmptyContext:
        """Extract the span context from a provided carrier."""
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
    def start_root_span(
        cls, span_name=None, component=None, start_time=None, start_as_current=True
    ) -> EmptySpan:
        """Start a root span."""
        return EMPTY_SPAN

    @classmethod
    def start_child_span(
        cls,
        span_name=None,
        parent_context=None,
        component=None,
        links=None,
        start_time=None,
        start_as_current=True,
    ) -> EmptySpan:
        """Start a child span."""
        return EMPTY_SPAN

    @classmethod
    def inject(cls):
        """Inject the current span context into a carrier and return it."""
        return {}

    @classmethod
    def extract(cls, carrier) -> EmptyContext:
        """Extract the span context from a provided carrier."""
        return EMPTY_CTX
