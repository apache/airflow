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

from typing import TYPE_CHECKING, Any, Callable, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HttpSensor(BaseSensorOperator):
    """
    Executes a HTTP GET statement and returns False on failure caused by
    404 Not Found or `response_check` returning False.

    HTTP Error codes other than 404 (like 403) or Connection Refused Error
    would raise an exception and fail the sensor itself directly (no more poking).
    To avoid failing the task for other codes than 404, the argument ``extra_option``
    can be passed with the value ``{'check_response': False}``. It will make the ``response_check``
    be execute for any http status code.

    The response check can access the template context to the operator:

    .. code-block:: python

        def response_check(response, task_instance):
            # The task_instance is injected, so you can pull data form xcom
            # Other context variables such as dag, ds, execution_date are also available.
            xcom_data = task_instance.xcom_pull(task_ids="pushing_task")
            # In practice you would do something more sensible with this data..
            print(xcom_data)
            return True


        HttpSensor(task_id="my_http_sensor", ..., response_check=response_check)

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:HttpSensor`

    :param http_conn_id: The :ref:`http connection<howto/connection:http>` to run the
        sensor against
    :param method: The HTTP request method to use
    :param endpoint: The relative part of the full url
    :param request_params: The parameters to be added to the GET url
    :param headers: The HTTP headers to be added to the GET request
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    """

    template_fields: Sequence[str] = ("endpoint", "request_params", "headers")

    def __init__(
        self,
        *,
        endpoint: str,
        http_conn_id: str = "http_default",
        method: str = "GET",
        request_params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        response_check: Callable[..., bool] | None = None,
        extra_options: dict[str, Any] | None = None,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.method = method
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.tcp_keep_alive = tcp_keep_alive
        self.tcp_keep_alive_idle = tcp_keep_alive_idle
        self.tcp_keep_alive_count = tcp_keep_alive_count
        self.tcp_keep_alive_interval = tcp_keep_alive_interval

    def poke(self, context: Context) -> bool:
        from airflow.utils.operator_helpers import determine_kwargs

        hook = HttpHook(
            method=self.method,
            http_conn_id=self.http_conn_id,
            tcp_keep_alive=self.tcp_keep_alive,
            tcp_keep_alive_idle=self.tcp_keep_alive_idle,
            tcp_keep_alive_count=self.tcp_keep_alive_count,
            tcp_keep_alive_interval=self.tcp_keep_alive_interval,
        )

        self.log.info("Poking: %s", self.endpoint)
        try:
            response = hook.run(
                self.endpoint,
                data=self.request_params,
                headers=self.headers,
                extra_options=self.extra_options,
            )
            if self.response_check:
                kwargs = determine_kwargs(self.response_check, [response], context)
                return self.response_check(response, **kwargs)
        except AirflowException as exc:
            if str(exc).startswith("404"):
                return False

            raise exc

        return True
