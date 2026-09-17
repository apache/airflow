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
"""Span helpers shared by the modules that make up a task run."""

from __future__ import annotations

import functools
import inspect

from opentelemetry import trace

from airflow.sdk._shared.observability.traces import get_task_span_detail_level

tracer = trace.get_tracer(__name__)


class detail_span:
    """Context manager and decorator that creates a child span when detail level > 1."""

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._ctx = None

    def _make_ctx(self):
        parent_span = trace.get_current_span()
        config_level = get_task_span_detail_level(span=parent_span)
        if config_level > 1:
            return tracer.start_as_current_span(*self._args, **self._kwargs)
        return trace.INVALID_SPAN

    def __enter__(self):
        self._ctx = self._make_ctx()
        return self._ctx.__enter__()

    def __exit__(self, *exc_info):
        return self._ctx.__exit__(*exc_info)

    def __call__(self, f):
        @functools.wraps(f)
        def wrapper(*inner_args, **inner_kwargs):
            with self._make_ctx():
                return f(*inner_args, **inner_kwargs)

        wrapper.__signature__ = inspect.signature(f)
        return wrapper
