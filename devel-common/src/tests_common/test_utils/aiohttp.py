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

import json
from http import HTTPStatus
from typing import Any

from aiohttp import ClientResponseError, RequestInfo
from yarl import URL


class MockAiohttpClientResponse:
    """Minimal aiohttp-like response for unit tests.

    This avoids constructing ``aiohttp.ClientResponse`` directly, which can change
    required constructor parameters between aiohttp versions.
    """

    def __init__(
        self,
        *,
        status: int = 200,
        payload: Any = None,
        text_payload: str | None = None,
        method: str = "GET",
        url: str = "http://example.com",
        reason: str | None = None,
    ) -> None:
        self.status = status
        self._payload = payload
        self._text_payload = text_payload
        self._method = method
        self._url = url
        self._reason = reason

    @property
    def reason(self) -> str:
        if self._reason is not None:
            return self._reason
        try:
            return HTTPStatus(self.status).phrase
        except ValueError:
            return ""

    def raise_for_status(self) -> None:
        if self.status >= HTTPStatus.BAD_REQUEST:
            raise ClientResponseError(
                request_info=RequestInfo(url=URL(self._url), method=self._method, headers=None),  # type: ignore[arg-type]
                history=(),
                status=self.status,
                message=self.reason,
            )

    async def json(self, content_type: str | None = None) -> Any:
        del content_type
        if self._payload is None:
            return {}
        if isinstance(self._payload, (dict, list, int, float, bool)):
            return self._payload
        if isinstance(self._payload, bytes):
            return json.loads(self._payload.decode())
        if isinstance(self._payload, str):
            return json.loads(self._payload)
        return self._payload

    async def text(self) -> str:
        if self._text_payload is not None:
            return self._text_payload
        if self._payload is None:
            return ""
        if isinstance(self._payload, bytes):
            return self._payload.decode()
        if isinstance(self._payload, str):
            return self._payload
        return json.dumps(self._payload)
