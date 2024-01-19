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

import asyncio
from typing import Any, AsyncIterator

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class HttpSensorTrigger(BaseTrigger):
    """
    A trigger that fires when the request to a URL returns a non-404 status code.

    :param endpoint: The relative part of the full url
    :param http_conn_id: The HTTP Connection ID to run the sensor against
    :param method: The HTTP request method to use
    :param data: payload to be uploaded or aiohttp parameters
    :param headers: The HTTP headers to be added to the GET request
    :param extra_options: Additional kwargs to pass when creating a request.
        For example, ``run(json=obj)`` is passed as ``aiohttp.ClientSession().get(json=obj)``
    :param poke_interval: Time to sleep using asyncio
    """

    def __init__(
        self,
        endpoint: str | None = None,
        http_conn_id: str = "http_default",
        method: str = "GET",
        data: dict[str, Any] | str | None = None,
        headers: dict[str, str] | None = None,
        extra_options: dict[str, Any] | None = None,
        poke_interval: float = 5.0,
    ):
        super().__init__()
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.headers = headers
        self.extra_options = extra_options or {}
        self.http_conn_id = http_conn_id
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes HttpTrigger arguments and classpath."""
        return (
            "airflow.providers.http.triggers.http_sensor.HttpSensorTrigger",
            {
                "endpoint": self.endpoint,
                "data": self.data,
                "headers": self.headers,
                "extra_options": self.extra_options,
                "http_conn_id": self.http_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Makes a series of asynchronous http calls via an http hook."""
        hook = self._get_async_hook()
        while True:
            try:
                await hook.run(
                    endpoint=self.endpoint,
                    data=self.data,
                    headers=self.headers,
                    extra_options=self.extra_options,
                )
                yield TriggerEvent(True)
            except AirflowException as exc:
                if str(exc).startswith("404"):
                    await asyncio.sleep(self.poke_interval)

    def _get_async_hook(self) -> HttpAsyncHook:
        return HttpAsyncHook(
            method=self.method,
            http_conn_id=self.http_conn_id,
        )
