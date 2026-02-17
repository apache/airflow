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
from collections.abc import Callable
from functools import wraps
from socket import socket
from typing import TYPE_CHECKING

from airflow._shared.observability.traces.base_tracer import EmptyTrace, Tracer
from airflow.configuration import conf

log = logging.getLogger(__name__)


def add_debug_span(func):
    """Decorate a function with span."""
    func_name = func.__name__
    qual_name = func.__qualname__
    module_name = func.__module__
    component = qual_name.rsplit(".", 1)[0] if "." in qual_name else module_name

    @wraps(func)
    def wrapper(*args, **kwargs):
        with DebugTrace.start_span(span_name=func_name, component=component):
            return func(*args, **kwargs)

    return wrapper


class _TraceMeta(type):
    factory: Callable[[], Tracer] | None = None
    instance: Tracer | EmptyTrace | None = None

    def __new__(cls, name, bases, attrs):
        # Read the debug flag from the class body.
        if "check_debug_traces_flag" not in attrs:
            raise TypeError(f"Class '{name}' must define 'check_debug_traces_flag'.")

        return super().__new__(cls, name, bases, attrs)

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

    def configure_factory(cls):
        """Configure the trace factory based on settings."""
        otel_on = conf.getboolean("traces", "otel_on")

        if cls.check_debug_traces_flag:
            debug_traces_on = conf.getboolean("traces", "otel_debug_traces_on")
        else:
            # Set to true so that it will be ignored during the evaluation for the factory instance.
            # If this is true, then (otel_on and debug_traces_on) will always evaluate to
            # whatever value 'otel_on' has and therefore it will be ignored.
            debug_traces_on = True

        if otel_on and debug_traces_on:
            from airflow.observability.traces import otel_tracer

            cls.factory = staticmethod(
                lambda use_simple_processor=False: otel_tracer.get_otel_tracer(cls, use_simple_processor)
            )
        else:
            # EmptyTrace is a class and not inherently callable.
            # Using a lambda ensures it can be invoked as a callable factory.
            # staticmethod ensures the lambda is treated as a standalone function
            # and avoids passing `cls` as an implicit argument.
            cls.factory = staticmethod(lambda: EmptyTrace())

    def get_constant_tags(cls) -> str | None:
        """Get constant tags to add to all traces."""
        return conf.get("traces", "tags", fallback=None)


if TYPE_CHECKING:
    Trace: EmptyTrace
    DebugTrace: EmptyTrace
else:

    class Trace(metaclass=_TraceMeta):
        """Empty class for Trace - we use metaclass to inject the right one."""

        check_debug_traces_flag = False

    class DebugTrace(metaclass=_TraceMeta):
        """Empty class for Trace and in case the debug traces flag is enabled."""

        check_debug_traces_flag = True
