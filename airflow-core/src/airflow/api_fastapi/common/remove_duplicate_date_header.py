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

from typing import Awaitable, Callable

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class RemoveDuplicateDateHeaderMiddleware:
    """
    Remove duplicate Date headers that can occur when Flask WSGI middleware is mounted.

    When Flask WSGI plugins are loaded, responses can contain duplicate Date headers -
    one from uvicorn and one from Flask. This middleware keeps only the first Date header
    and removes any subsequent duplicates.

    This works at the ASGI level, intercepting http.response.start messages before headers
    are sent to the client.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_deduplicated_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                # Extract headers and look for duplicates
                headers = MutableHeaders(raw=message["headers"])

                # Find all Date headers (case-insensitive)
                date_headers = []
                for header_name, header_value in message["headers"]:
                    if header_name.lower() == b"date":
                        date_headers.append(header_value)

                # If there are multiple Date headers, keep only the first
                if len(date_headers) > 1:
                    # Remove all Date headers
                    del headers["date"]

                    # Re-add the first one
                    headers["date"] = (
                        date_headers[0].decode("utf-8")
                        if isinstance(date_headers[0], bytes)
                        else date_headers[0]
                    )

                    # Update message with deduplicated headers
                    message["headers"] = list(headers.raw)

            await send(message)

        await self.app(scope, receive, send_with_deduplicated_headers)
