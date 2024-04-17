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

import asyncio
import json
from typing import TYPE_CHECKING, Any, Callable, Sequence

from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.providers.microsoft.azure.triggers.msgraph import MSGraphTrigger, ResponseSerializer
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue

if TYPE_CHECKING:
    from io import BytesIO

    from kiota_abstractions.request_information import QueryParams
    from kiota_abstractions.response_handler import NativeResponseType
    from kiota_abstractions.serialization import ParsableFactory
    from kiota_http.httpx_request_adapter import ResponseType
    from msgraph_core import APIVersion

    from airflow.triggers.base import TriggerEvent
    from airflow.utils.context import Context


def default_event_processor(context: Context, event: TriggerEvent) -> bool:
    if event.payload["status"] == "success":
        return json.loads(event.payload["response"])["status"] == "Succeeded"
    return False


class MSGraphSensor(BaseSensorOperator):
    """
    A Microsoft Graph API sensor which allows you to poll an async REST call to the Microsoft Graph API.

    :param url: The url being executed on the Microsoft Graph API (templated).
    :param response_type: The expected return type of the response as a string. Possible value are: `bytes`,
        `str`, `int`, `float`, `bool` and `datetime` (default is None).
    :param response_handler: Function to convert the native HTTPX response returned by the hook (default is
        lambda response, error_map: response.json()).  The default expression will convert the native response
        to JSON.  If response_type parameter is specified, then the response_handler will be ignored.
    :param method: The HTTP method being used to do the REST call (default is GET).
    :param conn_id: The HTTP Connection ID to run the operator against (templated).
    :param proxies: A dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as `v1.0` or `beta`.
    :param event_processor: Function which checks the response from MS Graph API (default is the
        `default_event_processor` method) and returns a boolean.  When the result is True, the sensor
        will stop poking, otherwise it will continue until it's True or times out.
    :param result_processor: Function to further process the response from MS Graph API
        (default is lambda: context, response: response).  When the response returned by the
        `KiotaRequestAdapterHook` are bytes, then those will be base64 encoded into a string.
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
        proxies: dict | None = None,
        api_version: APIVersion | None = None,
        event_processor: Callable[[Context, TriggerEvent], bool] = default_event_processor,
        result_processor: Callable[[Context, Any], Any] = lambda context, result: result,
        serializer: type[ResponseSerializer] = ResponseSerializer,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.url = url
        self.response_type = response_type
        self.response_handler = response_handler
        self.path_parameters = path_parameters
        self.url_template = url_template
        self.method = method
        self.query_parameters = query_parameters
        self.headers = headers
        self.data = data
        self.conn_id = conn_id
        self.proxies = proxies
        self.api_version = api_version
        self.event_processor = event_processor
        self.result_processor = result_processor
        self.serializer = serializer()

    @property
    def trigger(self):
        return MSGraphTrigger(
            url=self.url,
            response_type=self.response_type,
            response_handler=self.response_handler,
            path_parameters=self.path_parameters,
            url_template=self.url_template,
            method=self.method,
            query_parameters=self.query_parameters,
            headers=self.headers,
            data=self.data,
            conn_id=self.conn_id,
            timeout=self.timeout,
            proxies=self.proxies,
            api_version=self.api_version,
            serializer=type(self.serializer),
        )

    async def async_poke(self, context: Context) -> bool | PokeReturnValue:
        self.log.info("Sensor triggered")

        async for event in self.trigger.run():
            self.log.debug("event: %s", event)

            is_done = self.event_processor(context, event)

            self.log.debug("is_done: %s", is_done)

            response = self.serializer.deserialize(event.payload["response"])

            self.log.debug("deserialize event: %s", response)

            result = self.result_processor(context, response)

            self.log.debug("result: %s", result)

            return PokeReturnValue(is_done=is_done, xcom_value=result)
        return PokeReturnValue(is_done=True)

    def poke(self, context) -> bool | PokeReturnValue:
        return asyncio.run(self.async_poke(context))
