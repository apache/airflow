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

from types import NoneType
from typing import (
    Any,
    AsyncIterator,
    Sequence,
    TYPE_CHECKING,
    Callable,
)

from kiota_abstractions.api_error import APIError
from kiota_abstractions.method import Method
from kiota_abstractions.request_information import RequestInformation
from kiota_http.middleware.options import ResponseHandlerOption

from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.providers.microsoft.azure.serialization.response_handler import (
    CallableResponseHandler,
)
from airflow.providers.microsoft.azure.serialization.serializer import (
    ResponseSerializer,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from io import BytesIO
    from kiota_abstractions.request_adapter import RequestAdapter
    from kiota_abstractions.request_information import QueryParams
    from kiota_abstractions.response_handler import NativeResponseType
    from kiota_abstractions.serialization import ParsableFactory
    from kiota_http.httpx_request_adapter import ResponseType
    from msgraph_core import APIVersion


class MSGraphTrigger(BaseTrigger):
    """
    A Microsoft Graph API trigger which allows you to execute an async REST call to the Microsoft Graph API.

    https://github.com/microsoftgraph/msgraph-sdk-python

    :param url: The url being executed on the Microsoft Graph API (templated).
    :param response_type: The expected return type of the response as a string. Possible value are: "bytes",
        "str", "int", "float", "bool" and "datetime" (default is None).
    :param method: The HTTP method being used to do the REST call (default is GET).
    :param conn_id: The HTTP Connection ID to run the trigger against (templated).
    :param timeout: The HTTP timeout being used by the KiotaRequestAdapter (default is None).
        When no timeout is specified or set to None then no HTTP timeout is applied on each request.
    :param proxies: A Dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as "v1.0" or "beta".
    """

    DEFAULT_HEADERS = {"Accept": "application/json;q=1"}
    template_fields: Sequence[str] = (
        "url",
        "response_type",
        "path_parameters",
        "url_template",
        "query_parameters",
        "headers",
        "content",
        "conn_id",
    )

    def __init__(
        self,
        url: str | None = None,
        response_type: ResponseType | NoneType = None,
        response_handler: Callable[
            [NativeResponseType, dict[str, ParsableFactory | NoneType] | None], Any
        ] = lambda response, error_map: response.json(),
        path_parameters: dict[str, Any] | None = None,
        url_template: str | None = None,
        method: str = "GET",
        query_parameters: dict[str, QueryParams] | None = None,
        headers: dict[str, str] | None = None,
        content: BytesIO | NoneType = None,
        conn_id: str = KiotaRequestAdapterHook.default_conn_name,
        timeout: float | None = None,
        proxies: dict | None = None,
        api_version: APIVersion | NoneType = None,
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
        self.content = content
        self.serializer: ResponseSerializer = self.resolve_type(
            serializer, default=ResponseSerializer
        )()

    @classmethod
    def resolve_type(cls, value: str | type, default) -> type:
        if isinstance(value, str):
            try:
                return import_string(value)
            except ImportError:
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
                "content": self.content,
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
        """ Make a series of asynchronous HTTP calls via a KiotaRequestAdapterHook."""
        try:
            response = await self.execute()

            self.log.debug("response: %s", response)

            if response:
                response_type = type(response)

                self.log.debug("response type: %s", type(response))

                response = self.serializer.serialize(response)

                self.log.debug("serialized response type: %s", type(response))

                yield TriggerEvent(
                    {
                        "status": "success",
                        "type": f"{response_type.__module__}.{response_type.__name__}",
                        "response": response,
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

    def normalize_url(self) -> str:
        if self.url.startswith("/"):
            return self.url.replace("/", "", 1)
        return self.url

    def request_information(self) -> RequestInformation:
        request_information = RequestInformation()
        if self.url.startswith("http"):
            request_information.url = self.url
        else:
            request_information.url_template = f"{{+baseurl}}/{self.normalize_url()}"
        request_information.path_parameters = self.path_parameters or {}
        request_information.http_method = Method(self.method.strip().upper())
        request_information.query_parameters = self.query_parameters or {}
        if not self.response_type:
            request_information.request_options[
                ResponseHandlerOption.get_key()
            ] = ResponseHandlerOption(
                response_handler=CallableResponseHandler(self.response_handler)
            )
        request_information.content = self.content
        headers = (
            {**self.DEFAULT_HEADERS, **self.headers}
            if self.headers
            else self.DEFAULT_HEADERS
        )
        for header_name, header_value in headers.items():
            request_information.headers.try_add(
                header_name=header_name, header_value=header_value
            )
        return request_information

    @staticmethod
    def error_mapping() -> dict[str, ParsableFactory | NoneType]:
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
