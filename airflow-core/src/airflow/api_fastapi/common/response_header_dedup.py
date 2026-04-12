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
"""Middleware to remove duplicate response headers."""

from __future__ import annotations

from starlette.datastructures import MutableHeaders
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


class ResponseHeaderDedupMiddleware(BaseHTTPMiddleware):
    """
    Middleware to remove duplicate response headers.

    When Flask is mounted via WSGIMiddleware, both uvicorn and Flask may add
    the same headers (e.g., 'Date'), resulting in duplicate header entries.
    This middleware removes duplicates, keeping only the first occurrence.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)

        # Track seen headers (case-insensitive)
        seen_headers = set()
        deduped_headers = MutableHeaders()

        for name, value in response.headers.raw:
            name_lower = name.decode("latin1").lower()
            if name_lower not in seen_headers:
                seen_headers.add(name_lower)
                deduped_headers.append(name.decode("latin1"), value.decode("latin1"))

        # Replace headers with deduplicated version
        response.headers.clear()
        for key, value in deduped_headers.items():
            response.headers[key] = value

        return response
