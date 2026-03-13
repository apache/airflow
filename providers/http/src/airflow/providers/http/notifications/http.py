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

from functools import cached_property
from typing import TYPE_CHECKING, Any

import aiohttp

from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.http.hooks.http import HttpAsyncHook, HttpHook

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


class HttpNotifier(BaseNotifier):
    """
    HTTP Notifier.

    Sends HTTP requests to notify external systems.

    :param http_conn_id: HTTP connection id that has the base URL and optional authentication credentials.
    :param endpoint: The endpoint to be called i.e. resource/v1/query?
    :param method: The HTTP method to use. Defaults to POST.
    :param data: Payload to be uploaded or request parameters
    :param json: JSON payload to be uploaded
    :param headers: Additional headers to be passed through as a dictionary
    :param extra_options: Additional options to be used when executing the request
    """

    template_fields = ("http_conn_id", "endpoint", "data", "json", "headers", "extra_options")

    def __init__(
        self,
        *,
        http_conn_id: str = HttpHook.default_conn_name,
        endpoint: str | None = None,
        method: str = "POST",
        data: dict[str, Any] | str | None = None,
        json: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.json = json
        self.headers = headers
        self.extra_options = extra_options or {}

    @cached_property
    def hook(self) -> HttpHook:
        """HTTP Hook."""
        return HttpHook(method=self.method, http_conn_id=self.http_conn_id)

    @cached_property
    def async_hook(self) -> HttpAsyncHook:
        """HTTP Async Hook."""
        return HttpAsyncHook(method=self.method, http_conn_id=self.http_conn_id)

    def notify(self, context: Context) -> None:
        """Send HTTP notification (sync)."""
        resp = self.hook.run(
            endpoint=self.endpoint,
            data=self.data,
            headers=self.headers,
            extra_options=self.extra_options,
            json=self.json,
        )
        self.log.debug("HTTP notification sent: %s %s", resp.status_code, resp.url)

    async def async_notify(self, context: Context) -> None:
        """Send HTTP notification (async)."""
        async with aiohttp.ClientSession() as session:
            resp = await self.async_hook.run(
                session=session,
                endpoint=self.endpoint,
                data=self.data,
                json=self.json,
                headers=self.headers,
                extra_options=self.extra_options,
            )
            self.log.debug("HTTP notification sent (async): %s %s", resp.status, resp.url)


send_http_notification = HttpNotifier
