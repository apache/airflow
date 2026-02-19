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
from functools import wraps

from airflow.sdk._shared.observability.traces.base_tracer import EmptyTrace
from airflow.sdk._shared.observability.traces.otel_tracer import get_otel_tracer
from airflow.sdk.configuration import conf

log = logging.getLogger(__name__)

otel_on = conf.getboolean("traces", "otel_on")
debug_on = conf.getboolean("traces", "otel_debugging_on", fallback=False)
port = conf.getint("traces", "otel_port", fallback=4318)

if otel_on and debug_on:
    debug_tracer = get_otel_tracer(
        use_simple_processor=True,
        host=conf.get("traces", "otel_host", fallback=None),
        port=port,
        ssl_active=conf.getboolean("traces", "otel_ssl_active", fallback=False),
        otel_service=conf.get("traces", "otel_service", fallback=None),
        tag_string=conf.get("traces", "tags", fallback=None),
        debug=debug_on,
    )
else:
    debug_tracer = EmptyTrace()

if otel_on:
    tracer = get_otel_tracer(
        use_simple_processor=True,
        host=conf.get("traces", "otel_host", fallback=None),
        port=port,
        ssl_active=conf.getboolean("traces", "otel_ssl_active", fallback=False),
        otel_service=conf.get("traces", "otel_service", fallback=None),
        tag_string=conf.get("traces", "tags", fallback=None),
        debug=debug_on,
    )
else:
    tracer = EmptyTrace()


def add_debug_span(func):
    """Decorate a function with span."""
    func_name = func.__name__
    qual_name = func.__qualname__
    module_name = func.__module__
    component = qual_name.rsplit(".", 1)[0] if "." in qual_name else module_name

    @wraps(func)
    def wrapper(*args, **kwargs):
        with debug_tracer.start_span(span_name=func_name, component=component):
            return func(*args, **kwargs)

    return wrapper
