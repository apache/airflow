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
from json import JSONDecodeError
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Sequence,
)
from uuid import UUID

import pendulum

from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from io import BytesIO

    from kiota_abstractions.request_adapter import RequestAdapter
    from kiota_abstractions.request_information import QueryParams
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


class MSGraphTrigger(BaseTrigger):
    """
    A Microsoft Graph API trigger which allows you to execute an async REST call to the Microsoft Graph API.

    :param url: The url being executed on the Microsoft Graph API (templated).
    :param response_type: The expected return type of the response as a string. Possible value are: `bytes`,
        `str`, `int`, `float`, `bool` and `datetime` (default is None).
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
        path_parameters: dict[str, Any] | None = None,
        url_template: str | None = None,
        method: str = "GET",
        query_parameters: dict[str, QueryParams] | None = None,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | str | BytesIO | None = None,
        conn_id: str = KiotaRequestAdapterHook.default_conn_name,
        timeout: float | None = None,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
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
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "conn_id": self.conn_id,
                "timeout": self.timeout,
                "proxies": self.proxies,
                "api_version": self.api_version,
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
    def api_version(self) -> APIVersion | str:
        return self.hook.api_version

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make a series of asynchronous HTTP calls via a KiotaRequestAdapterHook."""
        try:
            response = await self.hook.run(
                url=self.url,
                response_type=self.response_type,
                path_parameters=self.path_parameters,
                method=self.method,
                query_parameters=self.query_parameters,
                headers=self.headers,
                data=self.data,
            )

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
