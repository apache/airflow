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

import base64
import pickle
from typing import TYPE_CHECKING, Any, AsyncIterator

import requests
from requests.cookies import RequestsCookieJar
from requests.structures import CaseInsensitiveDict

from airflow.providers.http.hooks.http import HttpAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse


class HttpTrigger(BaseTrigger):
    """
    HttpTrigger run on the trigger worker.

    :param http_conn_id: http connection id that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    :param method: the API method to be called
    :param endpoint: Endpoint to be called, i.e. ``resource/v1/query?``.
    :param headers: Additional headers to be passed through as a dict.
    :param data: Payload to be uploaded or request parameters.
    :param extra_options: Additional kwargs to pass when creating a request.
        For example, ``run(json=obj)`` is passed as
        ``aiohttp.ClientSession().get(json=obj)``.
        2XX or 3XX status codes
    """

    def __init__(
        self,
        http_conn_id: str = "http_default",
        auth_type: Any = None,
        method: str = "POST",
        endpoint: str | None = None,
        headers: dict[str, str] | None = None,
        data: Any = None,
        extra_options: dict[str, Any] | None = None,
    ):
        super().__init__()
        self.http_conn_id = http_conn_id
        self.method = method
        self.auth_type = auth_type
        self.endpoint = endpoint
        self.headers = headers
        self.data = data
        self.extra_options = extra_options

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes HttpTrigger arguments and classpath."""
        return (
            "airflow.providers.http.triggers.http.HttpTrigger",
            {
                "http_conn_id": self.http_conn_id,
                "method": self.method,
                "auth_type": self.auth_type,
                "endpoint": self.endpoint,
                "headers": self.headers,
                "data": self.data,
                "extra_options": self.extra_options,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Makes a series of asynchronous http calls via an http hook."""
        hook = HttpAsyncHook(
            method=self.method,
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
        )
        try:
            client_response = await hook.run(
                endpoint=self.endpoint,
                data=self.data,
                headers=self.headers,
                extra_options=self.extra_options,
            )
            response = await self._convert_response(client_response)
            yield TriggerEvent(
                {
                    "status": "success",
                    "response": base64.standard_b64encode(pickle.dumps(response)).decode("ascii"),
                }
            )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            # yield TriggerEvent({"status": "error", "message": str(traceback.format_exc())})

    @staticmethod
    async def _convert_response(client_response: ClientResponse) -> requests.Response:
        """Convert aiohttp.client_reqrep.ClientResponse to requests.Response."""
        response = requests.Response()
        response._content = await client_response.read()
        response.status_code = client_response.status
        response.headers = CaseInsensitiveDict(client_response.headers)
        response.url = str(client_response.url)
        response.history = [await HttpTrigger._convert_response(h) for h in client_response.history]
        response.encoding = client_response.get_encoding()
        response.reason = str(client_response.reason)
        cookies = RequestsCookieJar()
        for (k, v) in client_response.cookies.items():
            cookies.set(k, v)
        response.cookies = cookies
        return response
