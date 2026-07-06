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
"""Per-route request metrics middleware for the API server."""

from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING

from airflow.observability.stats import Stats

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

REQUEST_COUNT_METRIC = "api_server.request.count"
REQUEST_DURATION_METRIC = "api_server.request.duration"

# Emitted when a request does not resolve to a known route. Using a constant keeps the
# "route" tag cardinality bounded — the raw path would let unmatched URLs explode it.
_UNMATCHED_ROUTE = "__unmatched__"


class RequestMetricsMiddleware:
    """
    Emit per-route request count and latency metrics through the "Stats" backend.

    Records one counter increment and one timing observation per completed request,
    tagged with the templated route, HTTP method, and response status. The templated
    route (e.g. "/api/v2/dags/{dag_id}/dagRuns") is used rather than the concrete
    path so that per-"dag_id" requests do not each become a distinct metric series.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.monotonic_ns()
        response: Message | None = None

        async def capture_send(message: Message) -> None:
            nonlocal response
            if message["type"] == "http.response.start":
                response = message
            await send(message)

        try:
            await self.app(scope, receive, capture_send)
        except Exception:
            if response is None:
                response = {"status": 500}
            raise
        finally:
            duration_ms = (time.monotonic_ns() - start) / 1_000_000
            status = response["status"] if response is not None else 0
            route = scope.get("route")
            tags = {
                "method": scope.get("method", ""),
                "status": str(status),
                "route": getattr(route, "path", _UNMATCHED_ROUTE),
            }

            # Never let a metrics failure replace an application exception propagating
            # through this "finally" (see HttpAccessLogMiddleware for the rationale).
            with contextlib.suppress(Exception):
                Stats.incr(REQUEST_COUNT_METRIC, tags=tags)
                Stats.timing(REQUEST_DURATION_METRIC, duration_ms, tags=tags)
