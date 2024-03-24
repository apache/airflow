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

import typing

from airflow.metrics.protocols import Timer


class EmptyContext:
    """If no Tracer is configured, EmptyContext is used as a fallback."""

    def __init__(self):
        self.trace_id = 1


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

    def add_event(
        self,
        name: str,
        attributes=None,
        timestamp: int | None = None,
    ) -> None:
        """Add event to span."""
        pass

    def add_link(
        self,
        context: typing.Any,
        attributes=None,
    ) -> None:
        """Add link to the span."""
        pass

    def end(self, end_time=None, *args, **kwargs) -> None:
        """End."""
        pass


EMPTY_SPAN = EmptySpan()
EMPTY_CTX = EmptyContext()
EMPTY_TIMER = Timer()
