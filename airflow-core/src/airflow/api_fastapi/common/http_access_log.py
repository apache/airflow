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
"""HTTP access log middleware using structlog."""

from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = structlog.get_logger(logger_name="http.access")

_HEALTH_PATHS = frozenset(["/api/v2/monitor/health"])


class HttpAccessLogMiddleware:
    """
    Log completed HTTP requests as structured log events.

    This middleware replaces uvicorn's built-in access logger. It measures the
    full round-trip duration, binds any ``x-request-id`` header value to the
    structlog context for the duration of the request, and emits one log event
    per completed request.

    Health-check paths are excluded to avoid log noise.
    """

    def __init__(
        self,
        app: ASGIApp,
        request_id_header: str = "x-request-id",
    ) -> None:
        self.app = app
        self.request_id_header = request_id_header.lower().encode("ascii")

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

        request_id: str | None = None
        for name, value in scope["headers"]:
            if name == self.request_id_header:
                request_id = value.decode("ascii", errors="replace")
                break

        ctx = (
            structlog.contextvars.bound_contextvars(request_id=request_id)
            if request_id is not None
            else contextlib.nullcontext()
        )

        with ctx:
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
                    query = scope["query_string"].decode("ascii", errors="replace")
                    client = scope.get("client")
                    client_addr = f"{client[0]}:{client[1]}" if client else None

                    logger.info(
                        "request finished",
                        method=method,
                        path=path,
                        query=query,
                        status_code=status,
                        duration_us=duration_us,
                        client_addr=client_addr,
                    )
