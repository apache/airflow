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

import socket
import logging
import types
import typing
from typing import (
    Optional,
)

from typing import TYPE_CHECKING, Callable

from airflow.configuration import conf
from airflow.typing_compat import Protocol

log = logging.getLogger(__name__)


def span(func):
    """decorator that can be used to generate trace spans"""
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        qual_name = func.__qualname__
        module_name = func.__module__
        if '.' in qual_name:
            component = f"{qual_name.rsplit('.', 1)[0]}"
        else:
            component = module_name
        with Trace.start_span(span_name=func_name, component=component):
            func(*args, **kwargs)
    return wrapper


class Tracer(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, etc)"""
    instance: Tracer | DummyTrace | None = None

    @classmethod
    def get_tracer(cls, component):
        """get tracer"""
        raise NotImplementedError()

    @classmethod
    def start_span(cls, span_name:str, component:str=None):
        """start span"""
        raise NotImplementedError()
    
    @classmethod
    def use_span(cls, span):
        """use span"""
        raise NotImplementedError()

    @classmethod
    def get_current_span(self):
        raise NotImplementedError()

    @classmethod
    def start_span_from_dagrun(cls, dagrun, span_name, service_name, component):
        """start span from dagrun"""
        raise NotImplementedError()

    @classmethod
    def start_span_from_taskinstance(cls, ti, span_name, component, child=False):
        """start span from taskinstance"""
        raise NotImplementedError()


class DummySpan(object):
    """If no Tracer is configured, DummySpan is used as a fallback"""
    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self):
        pass

    def __call__(self, obj):
        return obj

    def set_attribute(self, key, value) -> None:
        """setting a attribute to the span"""
        pass

    def set_attributes(self, attributes) -> None:
        """setting multiple attributes at once"""
        pass

    def add_event(
        self,
        name: str,
        attributes: types.Attributes = None,
        timestamp: Optional[int] = None,
    ) -> None:
        """adding event to span"""
        pass

    def add_link(
        self,
        context: typing.Any,
        attributes: types.Attributes = None,
    ) -> None:
        """adding link to the span"""
        pass

    def end(self, end_time = None) -> None:
        pass


class DummyTrace:
    """If no Tracer is configured, DummyTracer is used as a fallback"""

    @classmethod
    def get_tracer(cls, component):
        """get tracer"""

    @classmethod
    def start_span(cls, span_name:str, component:str=None) -> DummySpan:
        """start span"""
        return DummySpan()
    
    @classmethod
    def use_span(cls, span) -> DummySpan:
        """use span"""
        return DummySpan()
    
    @classmethod
    def get_current_span(self) -> DummySpan:
        """get current span"""
        return DummySpan()

    @classmethod
    def start_span_from_dagrun(cls, dagrun, span_name, service_name, component) -> DummySpan:
        """start span from dagrun"""
        return DummySpan()

    @classmethod
    def start_span_from_taskinstance(cls, ti, span_name, component, child=False) -> DummySpan:
        """start span from taskinstance"""
        return DummySpan()


class _Trace(type):
    factory: Callable
    instance: Tracer | DummyTrace | None = None

    def __getattr__(cls, name: str) -> str:
        if not cls.instance:
            try:
                cls.instance = cls.factory()
            except (socket.gaierror, ImportError) as e:
                log.error("Could not configure Trace: %s, using DummyTrace instead.", e)
                cls.instance = DummyTrace()
        return getattr(cls.instance, name)

    def __init__(cls, *args, **kwargs) -> None:
        super().__init__(cls)
        if not hasattr(cls.__class__, "factory"):
            if conf.has_option('traces', 'otel_on') and conf.getboolean('traces', 'otel_on'):
                from airflow.traces import otel_tracer
                cls.__class__.factory = otel_tracer.get_otel_tracer
            else:
                cls.__class__.factory = DummyTrace
    
    @classmethod
    def get_constant_tags(cls) -> str:
        """Get constant tags to add to all traces."""
        tags_in_string = conf.get("traces", "tags", fallback=None)
        if not tags_in_string:
            return None
        return tags_in_string


if TYPE_CHECKING:
    Trace: DummyTrace
else:
    class Trace(metaclass=_Trace):
        """Empty class for Trace - we use metaclass to inject the right one"""