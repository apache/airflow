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
import base64
import importlib
import inspect
import pickle
import sys
from collections.abc import AsyncIterator
from importlib import import_module
from typing import TYPE_CHECKING, Any

import aiohttp
import requests
from asgiref.sync import sync_to_async
from requests.cookies import RequestsCookieJar
from requests.structures import CaseInsensitiveDict

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.providers.http.hooks.http import HttpAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger
else:
    from airflow.triggers.base import BaseTrigger as BaseEventTrigger  # type: ignore

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse


def serialize_auth_type(auth: str | type | None) -> str | None:
    if auth is None:
        return None
    if isinstance(auth, str):
        return auth
    return f"{auth.__module__}.{auth.__qualname__}"


def deserialize_auth_type(path: str | None) -> type | None:
    if path is None:
        return None
    module_path, cls_name = path.rsplit(".", 1)
    return getattr(import_module(module_path), cls_name)


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
        auth_type: str | None = None,
        method: str = "POST",
        endpoint: str | None = None,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | str | None = None,
        extra_options: dict[str, Any] | None = None,
    ):
        super().__init__()
        self.http_conn_id = http_conn_id
        self.method = method
        self.auth_type = deserialize_auth_type(auth_type)
        self.endpoint = endpoint
        self.headers = headers
        self.data = data
        self.extra_options = extra_options

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize HttpTrigger arguments and classpath."""
        return (
            "airflow.providers.http.triggers.http.HttpTrigger",
            {
                "http_conn_id": self.http_conn_id,
                "method": self.method,
                "auth_type": serialize_auth_type(self.auth_type),
                "endpoint": self.endpoint,
                "headers": self.headers,
                "data": self.data,
                "extra_options": self.extra_options,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make a series of asynchronous http calls via a http hook."""
        hook = self._get_async_hook()
        try:
            response = await self._get_response(hook)
            yield TriggerEvent(
                {
                    "status": "success",
                    "response": base64.standard_b64encode(pickle.dumps(response)).decode("ascii"),
                }
            )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> HttpAsyncHook:
        return HttpAsyncHook(
            method=self.method,
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
        )

    async def _get_response(self, hook):
        async with aiohttp.ClientSession() as session:
            client_response = await hook.run(
                session=session,
                endpoint=self.endpoint,
                data=self.data,
                headers=self.headers,
                extra_options=self.extra_options,
            )
            response = await self._convert_response(client_response)
            return response

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
        for k, v in client_response.cookies.items():
            cookies.set(k, str(v))  # Convert Morsel to string
        response.cookies = cookies
        return response


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
        """Serialize HttpTrigger arguments and classpath."""
        return (
            "airflow.providers.http.triggers.http.HttpSensorTrigger",
            {
                "endpoint": self.endpoint,
                "data": self.data,
                "method": self.method,
                "headers": self.headers,
                "extra_options": self.extra_options,
                "http_conn_id": self.http_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make a series of asynchronous http calls via an http hook."""
        hook = self._get_async_hook()
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    await hook.run(
                        session=session,
                        endpoint=self.endpoint,
                        data=self.data,
                        headers=self.headers,
                        extra_options=self.extra_options,
                    )
                yield TriggerEvent(True)
                return
            except AirflowException as exc:
                if str(exc).startswith("404"):
                    await asyncio.sleep(self.poke_interval)

    def _get_async_hook(self) -> HttpAsyncHook:
        return HttpAsyncHook(
            method=self.method,
            http_conn_id=self.http_conn_id,
        )


class HttpEventTrigger(HttpTrigger, BaseEventTrigger):
    """
    HttpEventTrigger for event-based DAG scheduling when the API response satisfies the response check.

    :param response_check_path: Path to the function that evaluates whether the API response
        passes the conditions set by the user to fire the trigger. The method must be asynchronous.
    :param http_conn_id: http connection id that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    :param method: The API method to be called
    :param endpoint: Endpoint to be called, i.e. ``resource/v1/query?``.
    :param headers: Additional headers to be passed through as a dict.
    :param data: Payload to be uploaded or request parameters.
    :param extra_options: Additional kwargs to pass when creating a request.
    :parama poll_interval: How often, in seconds, the trigger should send a request to the API.
    """

    def __init__(
        self,
        response_check_path: str,
        http_conn_id: str = "http_default",
        auth_type: Any = None,
        method: str = "GET",
        endpoint: str | None = None,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | str | None = None,
        extra_options: dict[str, Any] | None = None,
        poll_interval: float = 60.0,
    ):
        super().__init__(http_conn_id, auth_type, method, endpoint, headers, data, extra_options)
        self.response_check_path = response_check_path
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize HttpEventTrigger arguments and classpath."""
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "http_conn_id": self.http_conn_id,
                "method": self.method,
                "auth_type": serialize_auth_type(self.auth_type),
                "endpoint": self.endpoint,
                "headers": self.headers,
                "data": self.data,
                "extra_options": self.extra_options,
                "response_check_path": self.response_check_path,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make a series of asynchronous http calls via a http hook until the response passes the response check."""
        hook = super()._get_async_hook()
        try:
            while True:
                response = await super()._get_response(hook)
                if await self._run_response_check(response):
                    break
                await asyncio.sleep(self.poll_interval)
            yield TriggerEvent(
                {
                    "status": "success",
                    "response": base64.standard_b64encode(pickle.dumps(response)).decode("ascii"),
                }
            )
        except Exception as e:
            self.log.error("status: error, message: %s", str(e))

    async def _import_from_response_check_path(self):
        """Import the response check callable from the path provided by the user."""
        module_path, func_name = self.response_check_path.rsplit(".", 1)
        if module_path in sys.modules:
            module = await sync_to_async(importlib.reload)(sys.modules[module_path])
        module = await sync_to_async(importlib.import_module)(module_path)
        return getattr(module, func_name)

    async def _run_response_check(self, response) -> bool:
        """Run the response_check callable provided by the user."""
        response_check = await self._import_from_response_check_path()
        if not inspect.iscoroutinefunction(response_check):
            raise AirflowException("The response_check callable is not asynchronous.")
        check = await response_check(response)
        return check
