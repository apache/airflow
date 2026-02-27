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
from socket import socket
from typing import TYPE_CHECKING
import warnings
from functools import wraps

from opentelemetry import trace

from airflow.exceptions import RemovedInAirflow4Warning

log = logging.getLogger(__name__)

tracer = trace.get_tracer(__name__)

warnings.warn(
    "This module is deprecated.  Do not use it.",
    category=RemovedInAirflow4Warning,
    stacklevel=1,
)

def add_debug_span(func):
    """Decorate a function with span."""
    warnings.warn(
        "The `add_debug_span` class is deprecated.  Do not use it.",
        category=RemovedInAirflow4Warning,
        stacklevel=1,
    )

    @wraps(func)
    def wrapper(*args, **kwargs):
        with tracer.start_as_current_span(func.__name__):
            return func(*args, **kwargs)

    return wrapper


def __getattr__(name: str):
    if name == "add_debug_span":
        return add_debug_span
    if name in ("Trace", "DebugTrace"):
        from airflow._shared.observability.traces.otel_tracer import OtelTrace

        warnings.warn(
            "You are trying to import Trace or DebugTrace from airflow.sdk.observability.trace."
            "These classes are deprecated. Do not use them! Instead, create traces traces with "
            "`from opentelemetry import trace; tracer = trace.get_tracer(__name__)`.",
            category=RemovedInAirflow4Warning,
            stacklevel=1,
        )
        return OtelTrace()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
>>>>>>> 6c017317de (Remove the unnecessary OTEL tracing abstractions)
