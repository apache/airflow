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

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseSensorOperator
from airflow.providers.common.compat.standard.triggers import TimeDeltaTrigger
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.providers.microsoft.azure.operators.msgraph import execute_callable
from airflow.providers.microsoft.azure.triggers.msgraph import MSGraphTrigger, ResponseSerializer

if TYPE_CHECKING:
    from datetime import timedelta
    from io import BytesIO

    from msgraph_core import APIVersion

    from airflow.utils.context import Context


class MSGraphSensor(BaseSensorOperator):
    """
    A Microsoft Graph API sensor which allows you to poll an async REST call to the Microsoft Graph API.

    :param url: The url being executed on the Microsoft Graph API (templated).
    :param response_type: The expected return type of the response as a string. Possible value are: `bytes`,
        `str`, `int`, `float`, `bool` and `datetime` (default is None).
    :param method: The HTTP method being used to do the REST call (default is GET).
    :param conn_id: The HTTP Connection ID to run the operator against (templated).
    :param proxies: A dict defining the HTTP proxies to be used (default is None).
    :param scopes: The scopes to be used (default is ["https://graph.microsoft.com/.default"]).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as `v1.0` or `beta`.
    :param event_processor: Function which checks the response from MS Graph API (default is the
        `default_event_processor` method) and returns a boolean.  When the result is True, the sensor
        will stop poking, otherwise it will continue until it's True or times out.
    :param result_processor: Function to further process the response from MS Graph API
        (default is lambda: response, context: response).  When the response returned by the
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
        response_type: str | None = None,
        path_parameters: dict[str, Any] | None = None,
        url_template: str | None = None,
        method: str = "GET",
        query_parameters: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | str | BytesIO | None = None,
        conn_id: str = KiotaRequestAdapterHook.default_conn_name,
        proxies: dict | None = None,
        scopes: str | list[str] | None = None,
        api_version: APIVersion | str | None = None,
        event_processor: Callable[[Any, Context], bool] = lambda e, **context: e.get("status") == "Succeeded",
        result_processor: Callable[[Any, Context], Any] = lambda result, **context: result,
        serializer: type[ResponseSerializer] = ResponseSerializer,
        retry_delay: timedelta | float = 60,
        **kwargs,
    ):
        super().__init__(retry_delay=retry_delay, **kwargs)
        self.url = url
        self.response_type = response_type
        self.path_parameters = path_parameters
        self.url_template = url_template
        self.method = method
        self.query_parameters = query_parameters
        self.headers = headers
        self.data = data
        self.conn_id = conn_id
        self.proxies = proxies
        self.scopes = scopes
        self.api_version = api_version
        self.event_processor = event_processor
        self.result_processor = result_processor
        self.serializer = serializer()

    def execute(self, context: Context):
        self.defer(
            trigger=MSGraphTrigger(
                url=self.url,
                response_type=self.response_type,
                path_parameters=self.path_parameters,
                url_template=self.url_template,
                method=self.method,
                query_parameters=self.query_parameters,
                headers=self.headers,
                data=self.data,
                conn_id=self.conn_id,
                timeout=self.timeout,
                proxies=self.proxies,
                scopes=self.scopes,
                api_version=self.api_version,
                serializer=type(self.serializer),
            ),
            method_name=self.execute_complete.__name__,
        )

    def retry_execute(
        self,
        context: Context,
        **kwargs,
    ) -> Any:
        self.execute(context=context)

    def execute_complete(
        self,
        context: Context,
        event: dict[Any, Any] | None = None,
    ) -> Any:
        """
        Execute callback when MSGraphSensor finishes execution.

        This method gets executed automatically when MSGraphTrigger completes its execution.
        """
        self.log.debug("context: %s", context)

        if event:
            self.log.debug("%s completed with %s: %s", self.task_id, event.get("status"), event)

            if event.get("status") == "failure":
                raise AirflowException(event.get("message"))

            response = event.get("response")

            self.log.debug("response: %s", response)

            if response:
                response = self.serializer.deserialize(response)

                self.log.debug("deserialize response: %s", response)

                is_done = execute_callable(
                    self.event_processor,
                    response,
                    context,
                    "event_processor signature has changed, event parameter should be defined before context!",
                )

                self.log.debug("is_done: %s", is_done)

                if is_done:
                    result = execute_callable(
                        self.result_processor,
                        response,
                        context,
                        "result_processor signature has changed, result parameter should be defined before context!",
                    )

                    self.log.debug("processed response: %s", result)

                    return result

                self.defer(
                    trigger=TimeDeltaTrigger(self.retry_delay),
                    method_name=self.retry_execute.__name__,
                )

        return None
