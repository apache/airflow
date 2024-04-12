#
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
import locale
from base64 import b64encode
from contextlib import suppress
from datetime import datetime
from io import BytesIO
from json import JSONDecodeError
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    Sequence,
)
from urllib.parse import quote
from uuid import UUID

import pendulum
from kiota_abstractions.api_error import APIError
from kiota_abstractions.method import Method
from kiota_abstractions.request_information import RequestInformation
from kiota_abstractions.response_handler import ResponseHandler
from kiota_http.middleware.options import ResponseHandlerOption

from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from kiota_abstractions.request_adapter import RequestAdapter
    from kiota_abstractions.request_information import QueryParams
    from kiota_abstractions.response_handler import NativeResponseType
    from kiota_abstractions.serialization import ParsableFactory
    from kiota_http.httpx_request_adapter import ResponseType
    from msgraph_core import APIVersion


class ResponseSerializer:
    """ResponseSerializer serializes the response as a string."""

    def __init__(self, encoding: str | None = None):
        self.encoding = encoding or locale.getpreferredencoding()

    def serialize(self, response) -> str | None:
        def convert(value) -> str | None:
            if value is not None:
                if isinstance(value, UUID):
                    return str(value)
                if isinstance(value, datetime):
                    return value.isoformat()
                if isinstance(value, pendulum.DateTime):
                    return value.to_iso8601_string()  # Adjust the format as needed
                raise TypeError(f"Object of type {type(value)} is not JSON serializable!")
            return None

        if response is not None:
            if isinstance(response, bytes):
                return b64encode(response).decode(self.encoding)
            with suppress(JSONDecodeError):
                return json.dumps(response, default=convert)
            return response
        return None

    def deserialize(self, response) -> Any:
        if isinstance(response, str):
            with suppress(JSONDecodeError):
                response = json.loads(response)
        return response


class CallableResponseHandler(ResponseHandler):
    """
    CallableResponseHandler executes the passed callable_function with response as parameter.

    param callable_function: Function that is applied to the response.
    """

    def __init__(
        self,
        callable_function: Callable[[NativeResponseType, dict[str, ParsableFactory | None] | None], Any],
    ):
        self.callable_function = callable_function

    async def handle_response_async(
        self, response: NativeResponseType, error_map: dict[str, ParsableFactory | None] | None = None
    ) -> Any:
        """
        Invoke this callback method when a response is received.

        param response: The type of the native response object.
        param error_map: The error dict to use in case of a failed request.
        """
        return self.callable_function(response, error_map)


class MSGraphTrigger(BaseTrigger):
    """
    A Microsoft Graph API trigger which allows you to execute an async REST call to the Microsoft Graph API.

    :param url: The url being executed on the Microsoft Graph API (templated).
    :param response_type: The expected return type of the response as a string. Possible value are: `bytes`,
        `str`, `int`, `float`, `bool` and `datetime` (default is None).
    :param response_handler: Function to convert the native HTTPX response returned by the hook (default is
        lambda response, error_map: response.json()).  The default expression will convert the native response
        to JSON.  If response_type parameter is specified, then the response_handler will be ignored.
    :param method: The HTTP method being used to do the REST call (default is GET).
    :param conn_id: The HTTP Connection ID to run the operator against (templated).
    :param timeout: The HTTP timeout being used by the `KiotaRequestAdapter` (default is None).
        When no timeout is specified or set to None then there is no HTTP timeout on each request.
    :param proxies: A dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as `v1.0` or `beta`.
    :param serializer: Class which handles response serialization (default is ResponseSerializer).
        Bytes will be base64 encoded into a string, so it can be stored as an XCom.
    """

    DEFAULT_HEADERS = {"Accept": "application/json;q=1"}
    template_fields: Sequence[str] = (
        "url",
        "response_type",
        "path_parameters",
        "url_template",
        "query_parameters",
        "headers",
        "data",
        "conn_id",
    )

    def __init__(
        self,
        url: str,
        response_type: ResponseType | None = None,
        response_handler: Callable[
            [NativeResponseType, dict[str, ParsableFactory | None] | None], Any
        ] = lambda response, error_map: response.json(),
        path_parameters: dict[str, Any] | None = None,
        url_template: str | None = None,
        method: str = "GET",
        query_parameters: dict[str, QueryParams] | None = None,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | str | BytesIO | None = None,
        conn_id: str = KiotaRequestAdapterHook.default_conn_name,
        timeout: float | None = None,
        proxies: dict | None = None,
        api_version: APIVersion | None = None,
        serializer: type[ResponseSerializer] = ResponseSerializer,
    ):
        super().__init__()
        self.hook = KiotaRequestAdapterHook(
            conn_id=conn_id,
            timeout=timeout,
            proxies=proxies,
            api_version=api_version,
        )
        self.url = url
        self.response_type = response_type
        self.response_handler = response_handler
        self.path_parameters = path_parameters
        self.url_template = url_template
        self.method = method
        self.query_parameters = query_parameters
        self.headers = headers
        self.data = data
        self.serializer: ResponseSerializer = self.resolve_type(serializer, default=ResponseSerializer)()

    @classmethod
    def resolve_type(cls, value: str | type, default) -> type:
        if isinstance(value, str):
            with suppress(ImportError):
                return import_string(value)
            return default
        return value or default

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the HttpTrigger arguments and classpath."""
        api_version = self.api_version.value if self.api_version else None
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "conn_id": self.conn_id,
                "timeout": self.timeout,
                "proxies": self.proxies,
                "api_version": api_version,
                "serializer": f"{self.serializer.__class__.__module__}.{self.serializer.__class__.__name__}",
                "url": self.url,
                "path_parameters": self.path_parameters,
                "url_template": self.url_template,
                "method": self.method,
                "query_parameters": self.query_parameters,
                "headers": self.headers,
                "data": self.data,
                "response_type": self.response_type,
            },
        )

    def get_conn(self) -> RequestAdapter:
        return self.hook.get_conn()

    @property
    def conn_id(self) -> str:
        return self.hook.conn_id

    @property
    def timeout(self) -> float | None:
        return self.hook.timeout

    @property
    def proxies(self) -> dict | None:
        return self.hook.proxies

    @property
    def api_version(self) -> APIVersion:
        return self.hook.api_version

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make a series of asynchronous HTTP calls via a KiotaRequestAdapterHook."""
        try:
            response = await self.execute()

            self.log.debug("response: %s", response)

            if response:
                response_type = type(response)

                self.log.debug("response type: %s", response_type)

                yield TriggerEvent(
                    {
                        "status": "success",
                        "type": f"{response_type.__module__}.{response_type.__name__}",
                        "response": self.serializer.serialize(response),
                    }
                )
            else:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "type": None,
                        "response": None,
                    }
                )
        except Exception as e:
            self.log.exception("An error occurred: %s", e)
            yield TriggerEvent({"status": "failure", "message": str(e)})

    def normalize_url(self) -> str | None:
        if self.url.startswith("/"):
            return self.url.replace("/", "", 1)
        return self.url

    def encoded_query_parameters(self) -> dict:
        if self.query_parameters:
            return {quote(key): quote(str(value)) for key, value in self.query_parameters.items()}
        return {}

    def request_information(self) -> RequestInformation:
        request_information = RequestInformation()
        request_information.path_parameters = self.path_parameters or {}
        request_information.http_method = Method(self.method.strip().upper())
        request_information.query_parameters = self.encoded_query_parameters()
        if self.url.startswith("http"):
            request_information.url = self.url
        elif request_information.query_parameters.keys():
            query = ",".join(request_information.query_parameters.keys())
            request_information.url_template = f"{{+baseurl}}/{self.normalize_url()}{{?{query}}}"
        else:
            request_information.url_template = f"{{+baseurl}}/{self.normalize_url()}"
        if not self.response_type:
            request_information.request_options[ResponseHandlerOption.get_key()] = ResponseHandlerOption(
                response_handler=CallableResponseHandler(self.response_handler)
            )
        headers = {**self.DEFAULT_HEADERS, **self.headers} if self.headers else self.DEFAULT_HEADERS
        for header_name, header_value in headers.items():
            request_information.headers.try_add(header_name=header_name, header_value=header_value)
        if isinstance(self.data, BytesIO) or isinstance(self.data, bytes) or isinstance(self.data, str):
            request_information.content = self.data
        elif self.data:
            request_information.headers.try_add(
                header_name=RequestInformation.CONTENT_TYPE_HEADER, header_value="application/json"
            )
            request_information.content = json.dumps(self.data).encode("utf-8")
        return request_information

    @staticmethod
    def error_mapping() -> dict[str, ParsableFactory | None]:
        return {
            "4XX": APIError,
            "5XX": APIError,
        }

    async def execute(self) -> AsyncIterator[TriggerEvent]:
        return await self.get_conn().send_primitive_async(
            request_info=self.request_information(),
            response_type=self.response_type,
            error_map=self.error_mapping(),
        )
