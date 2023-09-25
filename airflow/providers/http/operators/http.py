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

import base64
import pickle
from typing import TYPE_CHECKING, Any, Callable, Sequence, List, Union

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.triggers.http import HttpTrigger
from airflow.utils.helpers import merge_dicts

if TYPE_CHECKING:
    from requests import Response
    from requests.auth import AuthBase

    from airflow.utils.context import Context


class SimpleHttpOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SimpleHttpOperator`

    :param http_conn_id: The :ref:`http connection<howto/connection:http>` to run
        the operator against
    :param endpoint: The relative part of the full url. (templated)
    :param method: The HTTP method to use, default = "POST"
    :param data: The data to pass. POST-data in POST/PUT and params
        in the URL for a GET request. (templated)
    :param headers: The HTTP headers to be added to the GET request
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.
    :param response_filter: A function allowing you to manipulate the response
        text. e.g response_filter=lambda response: json.loads(response.text).
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param log_response: Log the response (default: False)
    :param auth_type: The auth type for the service
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    :param deferrable: Run operator in the deferrable mode
    """

    template_fields: Sequence[str] = (
        "endpoint",
        "data",
        "headers",
    )
    template_fields_renderers = {"headers": "json", "data": "py"}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        method: str = "POST",
        data: Any = None,
        headers: dict[str, str] | None = None,
        pagination_function: Callable[..., Any] | None = None,
        response_check: Callable[..., bool] | None = None,
        response_filter: Callable[..., Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        http_conn_id: str = "http_default",
        log_response: bool = False,
        auth_type: type[AuthBase] | None = None,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data or {}
        self.pagination_function = pagination_function
        self.response_check = response_check
        self.response_filter = response_filter
        self.extra_options = extra_options or {}
        self.log_response = log_response
        self.auth_type = auth_type
        self.tcp_keep_alive = tcp_keep_alive
        self.tcp_keep_alive_idle = tcp_keep_alive_idle
        self.tcp_keep_alive_count = tcp_keep_alive_count
        self.tcp_keep_alive_interval = tcp_keep_alive_interval
        self.deferrable = deferrable
        self._deferrable_paginated_responses: List[Response] = []

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=HttpTrigger(
                    http_conn_id=self.http_conn_id,
                    auth_type=self.auth_type,
                    method=self.method,
                    endpoint=self.endpoint,
                    headers=self.headers,
                    data=self.data,
                    extra_options=self.extra_options,
                ),
                method_name="execute_complete",
            )
        else:
            http = HttpHook(
                self.method,
                http_conn_id=self.http_conn_id,
                auth_type=self.auth_type,
                tcp_keep_alive=self.tcp_keep_alive,
                tcp_keep_alive_idle=self.tcp_keep_alive_idle,
                tcp_keep_alive_count=self.tcp_keep_alive_count,
                tcp_keep_alive_interval=self.tcp_keep_alive_interval,
            )

            self.log.info("Calling HTTP method")
            response = http.run(self.endpoint, self.data, self.headers, self.extra_options)

            if self.pagination_function:
                all_responses: List[Response] = [response]
                while True:
                    next_page_params = self.pagination_function(response)
                    if not next_page_params:
                        break
                    response = http.run(**self._merge_next_page_parameters(next_page_params))
                    all_responses.append(response)
                response = all_responses

            return self.process_response(context=context, response=response)

    def process_response(
        self, context: Context, response: Union[Response, List[Response]]
    ) -> Union[str, List[str]]:
        """Process the response."""
        from airflow.utils.operator_helpers import determine_kwargs

        make_default_response: Callable = self._default_response_maker(response=response)

        if self.log_response:
            self.log.info(make_default_response())
        if self.response_check:
            kwargs = determine_kwargs(self.response_check, [response], context)
            if not self.response_check(response, **kwargs):
                raise AirflowException("Response check returned False.")
        if self.response_filter:
            kwargs = determine_kwargs(self.response_filter, [response], context)
            return self.response_filter(response, **kwargs)
        return make_default_response()

    @staticmethod
    def _default_response_maker(response: Union[Response, List[Response]]) -> Callable:
        """
        Create a default response maker function based on the type of response.

        :param response: The response object or list of response objects.
        :return: A function that returns response text(s).
        """
        if isinstance(response, Response):
            return lambda: response.text
        return lambda: [entry.text for entry in response]

    def execute_complete(self, context: Context, event: dict):
        """
        Callback for when the trigger fires. When no pagination, this method
        returns immediately. Otherwise, it creates a new deferrable.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event["status"] == "success":
            response = pickle.loads(base64.standard_b64decode(event["response"]))

            if self.pagination_function:
                self._deferrable_paginated_responses.append(response)

                next_page_params = self.pagination_function(response)
                if not next_page_params:
                    return self.process_response(
                        context=context, response=self._deferrable_paginated_responses
                    )
                self.defer(
                    trigger=HttpTrigger(
                        http_conn_id=self.http_conn_id,
                        auth_type=self.auth_type,
                        method=self.method,
                        **self._merge_next_page_parameters(next_page_params),
                    ),
                    method_name="execute_complete",
                )
            else:
                return self.process_response(context=context, response=response)
        else:
            raise AirflowException(f"Unexpected error in the operation: {event['message']}")

    def _merge_next_page_parameters(self, next_page_params: dict) -> dict:
        """
        Merge current requests parameters with the ones for the next page,
        generated by the pagination function. Items in the 'next_page_params'
        overrides those in the current request.

        :param next_page_params: A dictionary containing the parameters for the next page.
        :return: A dictionary containing the merged parameters.
        """
        return dict(
            endpoint=next_page_params.get("endpoint") or self.endpoint,
            data=merge_dicts(self.data, next_page_params.get("data", {})),
            headers=merge_dicts(self.headers, next_page_params.get("headers", {})),
            extra_options=merge_dicts(self.extra_options, next_page_params.get("extra_options", {})),
        )
