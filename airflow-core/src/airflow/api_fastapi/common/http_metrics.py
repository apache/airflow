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
"""HTTP API metrics middleware."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import structlog

from airflow._shared.observability.metrics.stats import Stats

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = structlog.get_logger(logger_name="http.metrics")

_HEALTH_PATHS = frozenset(["/api/v2/monitor/health"])
_API_PATH_PREFIX_TO_SURFACE = (
    ("/api/v2", "public"),
    ("/ui", "ui"),
)
_ROUTE_PATHS_BY_ROUTER_ID: dict[int, dict[object, str]] = {}


def _get_api_surface(path: str) -> str | None:
    for prefix, surface in _API_PATH_PREFIX_TO_SURFACE:
        if path == prefix or path.startswith(f"{prefix}/"):
            return surface
    return None


def _get_status_family(status_code: int) -> str:
    return f"{status_code // 100}xx"


def _get_route_tag(scope: Scope) -> str:
    route = scope.get("route")
    route_path = getattr(route, "path", None)
    if isinstance(route_path, str) and route_path:
        return route_path

    router = scope.get("router")
    endpoint = scope.get("endpoint")
    if router is not None and endpoint is not None:
        route_paths = _ROUTE_PATHS_BY_ROUTER_ID.get(id(router))
        if route_paths is None:
            route_paths = {
                candidate_endpoint: candidate_route_path
                for candidate_route in getattr(router, "routes", ())
                for candidate_endpoint, candidate_route_path in [
                    (
                        getattr(candidate_route, "endpoint", None),
                        getattr(candidate_route, "path", None),
                    )
                ]
                if candidate_endpoint is not None
                and isinstance(candidate_route_path, str)
                and candidate_route_path
            }
            _ROUTE_PATHS_BY_ROUTER_ID[id(router)] = route_paths

        endpoint_route_path = route_paths.get(endpoint)
        if isinstance(endpoint_route_path, str) and endpoint_route_path:
            return endpoint_route_path

    return "unmatched"


def _emit_api_metrics(
    *,
    scope: Scope,
    path: str,
    method: str,
    status_code: int,
    duration_us: int,
) -> None:
    api_surface = _get_api_surface(path)
    if api_surface is None:
        return

    # Keep tags bounded so API metrics remain usable across supported backends.
    base_tags = {
        "api_surface": api_surface,
        "method": method or "UNKNOWN",
        "route": _get_route_tag(scope),
    }
    request_tags = {
        **base_tags,
        "status_family": _get_status_family(status_code),
    }
    duration_ms = duration_us / 1000.0

    Stats.incr("api.requests", tags=request_tags)
    Stats.timing("api.request.duration", duration_ms, tags=base_tags)
    if status_code >= 500:
        Stats.incr("api.request.errors", tags=base_tags)


class HttpMetricsMiddleware:
    """
    Emit REST API metrics for completed HTTP requests.

    Health-check paths are excluded to avoid metric noise.
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
            path = scope["path"]
            if path not in _HEALTH_PATHS:
                duration_us = (time.monotonic_ns() - start) // 1000
                status = response["status"] if response is not None else 0
                method = scope.get("method", "")

                # Observability failures must never affect serving the request.
                try:
                    _emit_api_metrics(
                        scope=scope,
                        path=path,
                        method=method,
                        status_code=status,
                        duration_us=duration_us,
                    )
                except Exception:
                    logger.exception(
                        "failed to emit API metrics",
                        method=method,
                        path=path,
                        status_code=status,
                    )
