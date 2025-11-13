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
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from aiohttp import BasicAuth
from requests import Response

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseHook, BaseOperator
from airflow.providers.http.triggers.http import HttpTrigger, serialize_auth_type
from airflow.utils.helpers import merge_dicts

if TYPE_CHECKING:
    from requests.auth import AuthBase

    from airflow.providers.http.hooks.http import HttpHook

    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context


class HttpOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:HttpOperator`

    :param http_conn_id: The :ref:`http connection<howto/connection:http>` to run
        the operator against
    :param endpoint: The relative part of the full url. (templated)
    :param method: The HTTP method to use, default = "POST"
    :param data: The data to pass. POST-data in POST/PUT and params
        in the URL for a GET request. (templated)
    :param headers: The HTTP headers to be added to the GET request
    :param pagination_function: A callable that generates the parameters used to call the API again,
        based on the previous response. Typically used when the API is paginated and returns for e.g a
        cursor, a 'next page id', or a 'next page URL'. When provided, the Operator will call the API
        repeatedly until this callable returns None. The result of the Operator will become by default a
        list of Response.text objects (instead of a single response object). Same with the other injected
        functions (like response_check, response_filter, ...) which will also receive a list of Response
        objects. This function receives a Response object form previous call, and should return a nested
        dictionary with the following optional keys: `endpoint`, `data`, `headers` and `extra_options.
        Those keys will be merged and/or override the parameters provided into the HttpOperator declaration.
        Parameters are merged when they are both a dictionary (e.g.: HttpOperator.headers will be merged
        with the `headers` dict provided by this function). When merging, dict items returned by this
        function will override initial ones (e.g: if both HttpOperator.headers and `headers` have a 'cookie'
        item, the one provided by `headers` is kept). Parameters are simply overridden when any of them are
        string (e.g.: HttpOperator.endpoint is overridden by `endpoint`).
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise. If a pagination_function
        is provided, this function will receive a list of response objects instead of a
        single response object.
    :param response_filter: A function allowing you to manipulate the response
        text. e.g response_filter=lambda response: json.loads(response.text).
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        If a pagination_function is provided, this function will receive a list of response
        object instead of a single response object.
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
    :param retry_args: Arguments which define the retry behaviour.
        See Tenacity documentation at https://github.com/jd/tenacity
    """

    conn_id_field = "http_conn_id"
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
        data: dict[str, Any] | str | None = None,
        headers: dict[str, str] | None = None,
        pagination_function: Callable[..., Any] | None = None,
        response_check: Callable[..., bool] | None = None,
        response_filter: Callable[..., Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        request_kwargs: dict[str, Any] | None = None,
        http_conn_id: str = "http_default",
        log_response: bool = False,
        auth_type: type[AuthBase] | type[BasicAuth] | None = None,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        retry_args: dict[str, Any] | None = None,
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
        self.retry_args = retry_args
        self.request_kwargs = request_kwargs or {}

    @property
    def hook(self) -> HttpHook:
        """Get Http Hook based on connection type."""
        conn_id = getattr(self, self.conn_id_field)
        self.log.debug("Get connection for %s", conn_id)
        conn = BaseHook.get_connection(conn_id)

        hook = conn.get_hook(
            hook_params=dict(
                method=self.method,
                auth_type=self.auth_type,
                tcp_keep_alive=self.tcp_keep_alive,
                tcp_keep_alive_idle=self.tcp_keep_alive_idle,
                tcp_keep_alive_count=self.tcp_keep_alive_count,
                tcp_keep_alive_interval=self.tcp_keep_alive_interval,
            )
        )
        return hook

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.execute_async(context=context)
        else:
            return self.execute_sync(context=context)

    def execute_sync(self, context: Context) -> Any:
        self.log.info("Calling HTTP method")
        if self.retry_args:
            response = self.hook.run_with_advanced_retry(
                self.retry_args,
                self.endpoint,
                self.data,
                self.headers,
                self.extra_options,
                **self.request_kwargs,
            )
        else:
            response = self.hook.run(
                self.endpoint,
                self.data,
                self.headers,
                self.extra_options,
                **self.request_kwargs,
            )
        response = self.paginate_sync(response=response)
        return self.process_response(context=context, response=response)

    def paginate_sync(self, response: Response) -> Response | list[Response]:
        if not self.pagination_function:
            return response

        all_responses = [response]
        while True:
            next_page_params = self.pagination_function(response)
            if not next_page_params:
                break
            if self.retry_args:
                response = self.hook.run_with_advanced_retry(
                    self.retry_args,
                    **self._merge_next_page_parameters(next_page_params),
                )
            else:
                response = self.hook.run(**self._merge_next_page_parameters(next_page_params))
            all_responses.append(response)
        return all_responses

    def execute_async(self, context: Context) -> None:
        self.defer(
            trigger=HttpTrigger(
                http_conn_id=self.http_conn_id,
                auth_type=serialize_auth_type(self._resolve_auth_type()),
                method=self.method,
                endpoint=self.endpoint,
                headers=self.headers,
                data=self.data,
                extra_options=self.extra_options,
            ),
            method_name="execute_complete",
        )

    def _resolve_auth_type(self) -> type[AuthBase] | type[BasicAuth] | None:
        """
        Resolve the authentication type for the HTTP request.

        If auth_type is not explicitly set, attempt to infer it from the connection configuration.
        For connections with login/password, default to BasicAuth.

        :return: The resolved authentication type class, or None if no auth is needed.
        """
        if self.auth_type is not None:
            return self.auth_type

        try:
            conn = BaseHook.get_connection(self.http_conn_id)
            if conn.login or conn.password:
                return BasicAuth
        except Exception as e:
            self.log.warning("Failed to resolve auth type from connection: %s", e)

        return None

    def process_response(self, context: Context, response: Response | list[Response]) -> Any:
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
    def _default_response_maker(response: Response | list[Response]) -> Callable:
        """
        Create a default response maker function based on the type of response.

        :param response: The response object or list of response objects.
        :return: A function that returns response text(s).
        """
        if isinstance(response, Response):
            response_object = response  # Makes mypy happy
            return lambda: response_object.text

        response_list: list[Response] = response  # Makes mypy happy
        return lambda: [entry.text for entry in response_list]

    def execute_complete(
        self, context: Context, event: dict, paginated_responses: None | list[Response] = None
    ):
        """
        Execute callback when the trigger fires; returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event["status"] == "success":
            response = pickle.loads(base64.standard_b64decode(event["response"]))

            self.paginate_async(context=context, response=response, previous_responses=paginated_responses)
            return self.process_response(context=context, response=response)
        raise AirflowException(f"Unexpected error in the operation: {event['message']}")

    def paginate_async(
        self, context: Context, response: Response, previous_responses: None | list[Response] = None
    ):
        if self.pagination_function:
            all_responses = previous_responses or []
            all_responses.append(response)

            next_page_params = self.pagination_function(response)
            if not next_page_params:
                return self.process_response(context=context, response=all_responses)
            self.defer(
                trigger=HttpTrigger(
                    http_conn_id=self.http_conn_id,
                    auth_type=serialize_auth_type(self._resolve_auth_type()),
                    method=self.method,
                    **self._merge_next_page_parameters(next_page_params),
                ),
                method_name="execute_complete",
                kwargs={"paginated_responses": all_responses},
            )

    def _merge_next_page_parameters(self, next_page_params: dict) -> dict:
        """
        Merge initial request parameters with next page parameters.

        Merge initial requests parameters with the ones for the next page, generated by
        the pagination function. Items in the 'next_page_params' overrides those defined
        in the previous request.

        :param next_page_params: A dictionary containing the parameters for the next page.
        :return: A dictionary containing the merged parameters.
        """
        data: str | dict | None = None  # makes mypy happy
        next_page_data_param = next_page_params.get("data")
        if isinstance(self.data, dict) and isinstance(next_page_data_param, dict):
            data = merge_dicts(self.data, next_page_data_param)
        else:
            data = next_page_data_param or self.data

        return dict(
            endpoint=next_page_params.get("endpoint") or self.endpoint,
            data=data,
            headers=merge_dicts(self.headers, next_page_params.get("headers", {})),
            extra_options=merge_dicts(self.extra_options, next_page_params.get("extra_options", {})),
            **self.request_kwargs,
        )
