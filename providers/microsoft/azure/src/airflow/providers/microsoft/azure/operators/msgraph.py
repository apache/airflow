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

import warnings
from collections.abc import Callable, Sequence
from contextlib import suppress
from typing import (
    TYPE_CHECKING,
    Any,
)

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import XCOM_RETURN_KEY, AirflowException, BaseOperator, TaskDeferred
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.providers.microsoft.azure.triggers.msgraph import (
    MSGraphTrigger,
    ResponseSerializer,
)

if TYPE_CHECKING:
    from io import BytesIO

    from msgraph_core import APIVersion

    from airflow.utils.context import Context


def default_event_handler(event: dict[Any, Any] | None = None, **context) -> Any:
    if event:
        if event.get("status") == "failure":
            raise AirflowException(event.get("message"))

        return event.get("response")


def execute_callable(
    func: Callable[[dict[Any, Any] | None, Context], Any] | Callable[[dict[Any, Any] | None, Any], Any],
    value: Any,
    context: Context,
    message: str,
) -> Any:
    try:
        with warnings.catch_warnings():
            with suppress(AttributeError, ImportError):
                from airflow.utils.context import (  # type: ignore[attr-defined]
                    AirflowContextDeprecationWarning,
                )

                warnings.filterwarnings("ignore", category=AirflowContextDeprecationWarning)
            warnings.simplefilter("ignore", category=DeprecationWarning)
            warnings.simplefilter("ignore", category=UserWarning)
            return func(value, **context)  # type: ignore
    except TypeError:
        warnings.warn(
            message,
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        return func(context, value)  # type: ignore


class MSGraphAsyncOperator(BaseOperator):
    """
    A Microsoft Graph API operator which allows you to execute REST call to the Microsoft Graph API.

    https://learn.microsoft.com/en-us/graph/use-the-api

        .. seealso::
            For more information on how to use this operator, take a look at the guide:
            :ref:`howto/operator:MSGraphAsyncOperator`

    :param url: The url being executed on the Microsoft Graph API (templated).
    :param response_type: The expected return type of the response as a string. Possible value are: `bytes`,
        `str`, `int`, `float`, `bool` and `datetime` (default is None).
    :param method: The HTTP method being used to do the REST call (default is GET).
    :param conn_id: The HTTP Connection ID to run the operator against (templated).
    :param key: The key that will be used to store `XCom's` ("return_value" is default).
    :param timeout: The HTTP timeout being used by the `KiotaRequestAdapter` (default is None).
        When no timeout is specified or set to None then there is no HTTP timeout on each request.
    :param proxies: A dict defining the HTTP proxies to be used (default is None).
    :param scopes: The scopes to be used (default is ["https://graph.microsoft.com/.default"]).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as `v1.0` or `beta`.
    :param result_processor: Function to further process the response from MS Graph API
        (default is lambda: response, context: response).  When the response returned by the
        `KiotaRequestAdapterHook` are bytes, then those will be base64 encoded into a string.
    :param event_handler: Function to process the event returned from `MSGraphTrigger`.  By default, when the
        event returned by the `MSGraphTrigger` has a failed status, an AirflowException is being raised with
        the message from the event, otherwise the response from the event payload is returned.
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
        *,
        url: str,
        response_type: str | None = None,
        path_parameters: dict[str, Any] | None = None,
        url_template: str | None = None,
        method: str = "GET",
        query_parameters: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | str | BytesIO | None = None,
        conn_id: str = KiotaRequestAdapterHook.default_conn_name,
        key: str = XCOM_RETURN_KEY,
        timeout: float | None = None,
        proxies: dict | None = None,
        scopes: str | list[str] | None = None,
        api_version: APIVersion | str | None = None,
        pagination_function: Callable[[MSGraphAsyncOperator, dict, Context], tuple[str, dict]] | None = None,
        result_processor: Callable[[Any, Context], Any] = lambda result, **context: result,
        event_handler: Callable[[dict[Any, Any] | None, Context], Any] | None = None,
        serializer: type[ResponseSerializer] = ResponseSerializer,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.url = url
        self.response_type = response_type
        self.path_parameters = path_parameters
        self.url_template = url_template
        self.method = method
        self.query_parameters = query_parameters
        self.headers = headers
        self.data = data
        self.conn_id = conn_id
        self.key = key
        self.timeout = timeout
        self.proxies = proxies
        self.scopes = scopes
        self.api_version = api_version
        self.pagination_function = pagination_function or self.paginate
        self.result_processor = result_processor
        self.event_handler = event_handler or default_event_handler
        self.serializer: ResponseSerializer = serializer()

    def execute(self, context: Context) -> None:
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

    def execute_complete(
        self,
        context: Context,
        event: dict[Any, Any] | None = None,
    ) -> Any:
        """
        Execute callback when MSGraphTrigger finishes execution.

        This method gets executed automatically when MSGraphTrigger completes its execution.
        """
        self.log.debug("context: %s", context)

        if event:
            self.log.debug("%s completed with %s: %s", self.task_id, event.get("status"), event)

            response = execute_callable(
                self.event_handler,  # type: ignore
                event,
                context,
                "event_handler signature has changed, event parameter should be defined before context!",
            )

            self.log.debug("response: %s", response)

            results = self.pull_xcom(context=context)

            if response:
                response = self.serializer.deserialize(response)

                self.log.debug("deserialize response: %s", response)

                result = execute_callable(
                    self.result_processor,
                    response,
                    context,
                    "result_processor signature has changed, result parameter should be defined before context!",
                )

                self.log.debug("processed response: %s", result)

                try:
                    self.trigger_next_link(
                        response=response, method_name=self.execute_complete.__name__, context=context
                    )
                except TaskDeferred as exception:
                    self.append_result(
                        results=results,
                        result=result,
                        append_result_as_list_if_absent=True,
                    )
                    self.push_xcom(context=context, value=results)
                    raise exception

                if not results:
                    return result

                self.append_result(results=results, result=result)
            return results
        return None

    @classmethod
    def append_result(
        cls,
        results: list[Any],
        result: Any,
        append_result_as_list_if_absent: bool = False,
    ) -> list[Any]:
        if isinstance(results, list):
            if isinstance(result, list):
                results.extend(result)
            else:
                results.append(result)
        else:
            if append_result_as_list_if_absent:
                if isinstance(result, list):
                    return result
                return [result]
            return result
        return results

    def pull_xcom(self, context: Context | dict[str, Any]) -> list:
        map_index = context["ti"].map_index
        value = list(
            context["ti"].xcom_pull(
                key=self.key,
                task_ids=self.task_id,
                dag_id=self.dag_id,
                map_indexes=map_index,
            )
            or []
        )

        if map_index:
            self.log.info(
                "Pulled XCom with task_id '%s' and dag_id '%s' and key '%s' and map_index %s: %s",
                self.task_id,
                self.dag_id,
                self.key,
                map_index,
                value,
            )
        else:
            self.log.info(
                "Pulled XCom with task_id '%s' and dag_id '%s' and key '%s': %s",
                self.task_id,
                self.dag_id,
                self.key,
                value,
            )

        return value

    def push_xcom(self, context: Any, value) -> None:
        self.log.debug("do_xcom_push: %s", self.do_xcom_push)
        if self.do_xcom_push:
            self.log.info(
                "Pushing XCom with task_id '%s' and dag_id '%s' and key '%s': %s",
                self.task_id,
                self.dag_id,
                self.key,
                value,
            )
            context["ti"].xcom_push(key=self.key, value=value)

    @staticmethod
    def paginate(
        operator: MSGraphAsyncOperator, response: dict, **context
    ) -> tuple[Any, dict[str, Any] | None]:
        return KiotaRequestAdapterHook.default_pagination(
            response=response,
            url=operator.url,
            query_parameters=operator.query_parameters,
            responses=lambda: operator.pull_xcom(context),
        )

    def trigger_next_link(self, response, method_name: str, context: Context) -> None:
        if isinstance(response, dict):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=DeprecationWarning)
                    warnings.filterwarnings("ignore", category=UserWarning)
                    url, query_parameters = self.pagination_function(self, response, **context)  # type: ignore
            except TypeError:
                warnings.warn(
                    "pagination_function signature has changed, context parameter should be a kwargs argument!",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
                url, query_parameters = self.pagination_function(self, response, context)  # type: ignore

            self.log.debug("url: %s", url)
            self.log.debug("query_parameters: %s", query_parameters)

            if url:
                self.defer(
                    trigger=MSGraphTrigger(
                        url=url,
                        method=self.method,
                        query_parameters=query_parameters,
                        response_type=self.response_type,
                        conn_id=self.conn_id,
                        timeout=self.timeout,
                        proxies=self.proxies,
                        api_version=self.api_version,
                        serializer=type(self.serializer),
                    ),
                    method_name=method_name,
                )
