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
from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable

from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.providers.microsoft.azure.triggers.msgraph import MSGraphTrigger, ResponseSerializer
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context

if TYPE_CHECKING:
    from airflow.triggers.base import TriggerEvent

    from kiota_abstractions.request_information import QueryParams
    from kiota_abstractions.response_handler import NativeResponseType
    from kiota_abstractions.serialization import ParsableFactory
    from kiota_http.httpx_request_adapter import ResponseType
    from msgraph_core import APIVersion


def default_event_processor(context: Context, event: TriggerEvent) -> bool:
    if event.payload["status"] == "success":
        return json.loads(event.payload["response"])["status"] == "Succeeded"
    return False


class MSGraphSensor(BaseSensorOperator):
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
        event_processor: Callable[[Context, TriggerEvent], bool] = default_event_processor,
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
        self.timeout = timeout
        self.proxies = proxies
        self.api_version = api_version
        self.serializer = serializer
        self.event_processor = event_processor

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
            serializer=self.serializer,
        )

    async def async_poke(self, context: Context) -> bool | PokeReturnValue:
        self.log.info("Sensor triggered")

        async for response in self.trigger.run():
            self.log.debug("response: %s", response)

            is_done = self.event_processor(context, response)

            self.log.debug("is_done: %s", is_done)

            return PokeReturnValue(is_done=is_done, xcom_value=response)

    def poke(self, context) -> bool | PokeReturnValue:
        return asyncio.run(self.async_poke(context))
