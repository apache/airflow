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
    TYPE_CHECKING,
    Sequence,
    Callable, Optional,
)

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.providers.microsoft.azure.serialization.serializer import (
    ResponseSerializer,
)
from airflow.providers.microsoft.azure.triggers.msgraph import MSGraphTrigger
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    from msgraph_core import APIVersion
    from io import BytesIO
    from airflow.utils.context import Context
    from kiota_abstractions.request_adapter import ResponseType
    from kiota_abstractions.request_information import QueryParams
    from kiota_abstractions.response_handler import NativeResponseType
    from kiota_abstractions.serialization import ParsableFactory


class MSGraphAsyncOperator(BaseOperator):
    """
    A Microsoft Graph API operator which allows you to execute REST call to the Microsoft Graph API.

    https://learn.microsoft.com/en-us/graph/use-the-api

    :param conn_id: The HTTP Connection ID to run the operator against (templated).
    :param key: The key that will be used to store XCOM's ("return_value" is default).
    :param url: The url being executed on the Microsoft Graph API (templated).
    :param timeout: The HTTP timeout being used by the KiotaRequestAdapter (default is None).
        When no timeout is specified or set to None then no HTTP timeout is applied on each request.
    :param proxies: A Dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as "v1.0" or "beta".
    :param result_processor: Function to further process the response from MS Graph API
        (default is lambda: context, response: response).  When the response returned by the
        GraphServiceClientHook are bytes, then those will be base64 encoded into a string.
    :param response_type: The expected return type of the response as a string. Possible value are: "bytes",
        "str", "int", "float", "bool" and "datetime" (default is None).
    :param method: The HTTP method being used to do the REST call (default is GET).
    :param response_handler: Function to convert the native HTTPX response returned by the hook (default is
        lambda response, error_map: response.json()).  The default expression will convert the native response
        to JSON.  If response_type parameter is specified, then the response_handler will be ignored.
    :param serializer: Class which handles response serialization (default is ResponseSerializer).
        Bytes will be base64 encoded into a string, so it can be stored as an XCom.
    """

    template_fields: Sequence[str] = ("url", "conn_id")

    def __init__(
        self,
        *,
        url: str | None = None,
        response_type: ResponseType | NoneType = None,
        response_handler: Callable[
            [NativeResponseType, Optional[dict[str, Optional[ParsableFactory]]]], Any
        ] = lambda response, error_map: response.json(),
        path_parameters: Optional[dict[str, Any]] = None,
        url_template: str | None = None,
        method: str = "GET",
        query_parameters: dict[str, QueryParams] | None = None,
        headers: dict[str, str] | None = None,
        content: BytesIO | NoneType = None,
        conn_id: str = KiotaRequestAdapterHook.default_conn_name,
        key: str = XCOM_RETURN_KEY,
        timeout: float | None = None,
        proxies: dict | None = None,
        api_version: APIVersion | NoneType = None,
        result_processor: Callable[
            [Context, Any], Any
        ] = lambda context, result: result,
        serializer: type[ResponseSerializer] = ResponseSerializer,
        **kwargs: Any,
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
        self.content = content
        self.conn_id = conn_id
        self.key = key
        self.timeout = timeout
        self.proxies = proxies
        self.api_version = api_version
        self.result_processor = result_processor
        self.serializer: ResponseSerializer = serializer()
        self.results = None

    def execute(self, context: Context) -> None:
        self.log.info("Executing url '%s' as '%s'", self.url, self.method)
        self.defer(
            trigger=MSGraphTrigger(
                url=self.url,
                response_type=self.response_type,
                path_parameters=self.path_parameters,
                url_template=self.url_template,
                method=self.method,
                query_parameters=self.query_parameters,
                headers=self.headers,
                content=self.content,
                conn_id=self.conn_id,
                timeout=self.timeout,
                proxies=self.proxies,
                api_version=self.api_version,
                serializer=type(self.serializer),
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self,
        context: Context,
        event: dict[Any, Any] | None = None,
    ) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.debug("context: %s", context)

        if event:
            self.log.info(
                "%s completed with %s: %s", self.task_id, event.get("status"), event
            )

            if event.get("status") == "failure":
                raise AirflowException(event.get("message"))

            response = event.get("response")

            self.log.info("response: %s", response)

            if response:
                self.log.debug("response type: %s", type(response))

                response = self.serializer.deserialize(response)

                self.log.debug("deserialized response type: %s", type(response))

                result = self.result_processor(context, response)

                self.log.debug("processed response: %s", result)

                event["response"] = result

                self.log.debug("parsed response type: %s", type(response))

                try:
                    self.trigger_next_link(
                        response, method_name="pull_execute_complete"
                    )
                except TaskDeferred as exception:
                    self.append_result(
                        result=result,
                        append_result_as_list_if_absent=True,
                    )
                    self.push_xcom(context=context, value=self.results)
                    raise exception

                self.append_result(result=result)
                self.log.debug("results: %s", self.results)

                return self.results
        return None

    def append_result(
        self,
        result: Any,
        append_result_as_list_if_absent: bool = False,
    ):
        self.log.debug("value: %s", result)

        if isinstance(self.results, list):
            if isinstance(result, list):
                self.results.extend(result)
            else:
                self.results.append(result)
        else:
            if append_result_as_list_if_absent:
                if isinstance(result, list):
                    self.results = result
                else:
                    self.results = [result]
            else:
                self.results = result

    def push_xcom(self, context: Context, value) -> None:
        self.log.debug("do_xcom_push: %s", self.do_xcom_push)
        if self.do_xcom_push:
            self.log.info("Pushing XCom with key '%s': %s", self.key, value)
            self.xcom_push(context=context, key=self.key, value=value)

    def pull_execute_complete(
        self, context: Context, event: dict[Any, Any] | None = None
    ) -> Any:
        self.results = list(
            self.xcom_pull(
                context=context,
                task_ids=self.task_id,
                dag_id=self.dag_id,
                key=self.key,
            )
            or []  # noqa: W503
        )
        self.log.info(
            "Pulled XCom with task_id '%s' and dag_id '%s' and key '%s': %s",
            self.task_id,
            self.dag_id,
            self.key,
            self.results,
        )
        return self.execute_complete(context, event)

    def trigger_next_link(
        self, response, method_name="execute_complete"
    ) -> None:
        if isinstance(response, dict):
            odata_next_link = response.get("@odata.nextLink")

            self.log.debug("odata_next_link: %s", odata_next_link)

            if odata_next_link:
                self.defer(
                    trigger=MSGraphTrigger(
                        url=odata_next_link,
                        response_type=self.response_type,
                        response_handler=self.response_handler,
                        conn_id=self.conn_id,
                        timeout=self.timeout,
                        proxies=self.proxies,
                        api_version=self.api_version,
                        serializer=type(self.serializer),
                    ),
                    method_name=method_name,
                )
