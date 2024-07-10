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
from typing import TYPE_CHECKING, Any, Callable

import aiohttp
import requests
import tenacity
from aiohttp import ClientResponseError
from asgiref.sync import sync_to_async
from requests.auth import HTTPBasicAuth
from requests.models import DEFAULT_REDIRECT_LIMIT
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse

    from airflow.models import Connection


def _url_from_endpoint(base_url: str | None, endpoint: str | None) -> str:
    """Combine base url with endpoint."""
    if base_url and not base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
        return f"{base_url}/{endpoint}"
    return (base_url or "") + (endpoint or "")


class HttpHook(BaseHook):
    """
    Interact with HTTP servers.

    :param method: the API method to be called
    :param http_conn_id: :ref:`http connection<howto/connection:http>` that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    :param auth_args: extra arguments used to initialize the auth_type if different than default HTTPBasicAuth
    """

    conn_name_attr = "http_conn_id"
    default_conn_name = "http_default"
    conn_type = "http"
    hook_name = "HTTP"

    def __init__(
        self,
        method: str = "POST",
        http_conn_id: str = default_conn_name,
        auth_type: Any = None,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
    ) -> None:
        super().__init__()
        self.http_conn_id = http_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self._retry_obj: Callable[..., Any]
        self._auth_type: Any = auth_type
        self.tcp_keep_alive = tcp_keep_alive
        self.keep_alive_idle = tcp_keep_alive_idle
        self.keep_alive_count = tcp_keep_alive_count
        self.keep_alive_interval = tcp_keep_alive_interval

    @property
    def auth_type(self):
        return self._auth_type or HTTPBasicAuth

    @auth_type.setter
    def auth_type(self, v):
        self._auth_type = v

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_conn(self, headers: dict[Any, Any] | None = None) -> requests.Session:
        """
        Create a Requests HTTP session.

        :param headers: additional headers to be passed through as a dictionary
        """
        session = requests.Session()

        if self.http_conn_id:
            conn = self.get_connection(self.http_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = f"{schema}://{host}"

            if conn.port:
                self.base_url += f":{conn.port}"
            if conn.login:
                session.auth = self.auth_type(conn.login, conn.password)
            elif self._auth_type:
                session.auth = self.auth_type()
            if conn.extra:
                extra = conn.extra_dejson
                extra.pop(
                    "timeout", None
                )  # ignore this as timeout is only accepted in request method of Session
                extra.pop("allow_redirects", None)  # ignore this as only max_redirects is accepted in Session
                session.proxies = extra.pop("proxies", extra.pop("proxy", {}))
                session.stream = extra.pop("stream", False)
                session.verify = extra.pop("verify", extra.pop("verify_ssl", True))
                session.cert = extra.pop("cert", None)
                session.max_redirects = extra.pop("max_redirects", DEFAULT_REDIRECT_LIMIT)
                session.trust_env = extra.pop("trust_env", True)

                try:
                    session.headers.update(extra)
                except TypeError:
                    self.log.warning("Connection to %s has invalid extra field.", conn.host)
        if headers:
            session.headers.update(headers)

        return session

    def run(
        self,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        **request_kwargs: Any,
    ) -> Any:
        r"""
        Perform the request.

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :param data: payload to be uploaded or request parameters
        :param headers: additional headers to be passed through as a dictionary
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :param request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        extra_options = extra_options or {}

        session = self.get_conn(headers)

        url = _url_from_endpoint(self.base_url, endpoint)

        if self.tcp_keep_alive:
            keep_alive_adapter = TCPKeepAliveAdapter(
                idle=self.keep_alive_idle, count=self.keep_alive_count, interval=self.keep_alive_interval
            )
            session.mount(url, keep_alive_adapter)
        if self.method == "GET":
            # GET uses params
            req = requests.Request(self.method, url, params=data, headers=headers, **request_kwargs)
        elif self.method == "HEAD":
            # HEAD doesn't use params
            req = requests.Request(self.method, url, headers=headers, **request_kwargs)
        else:
            # Others use data
            req = requests.Request(self.method, url, data=data, headers=headers, **request_kwargs)

        prepped_request = session.prepare_request(req)
        self.log.debug("Sending '%s' to url: %s", self.method, url)
        return self.run_and_check(session, prepped_request, extra_options)

    def check_response(self, response: requests.Response) -> None:
        """
        Check the status code and raise on failure.

        :param response: A requests response object.
        :raise AirflowException: If the response contains a status code not
            in the 2xx and 3xx range.
        """
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)

    def run_and_check(
        self,
        session: requests.Session,
        prepped_request: requests.PreparedRequest,
        extra_options: dict[Any, Any],
    ) -> Any:
        """
        Grab extra options, actually run the request, and check the result.

        :param session: the session to be used to execute the request
        :param prepped_request: the prepared request generated in run()
        :param extra_options: additional options to be used when executing the request
            i.e. ``{'check_response': False}`` to avoid checking raising exceptions on non 2XX
            or 3XX status codes
        """
        extra_options = extra_options or {}

        settings = session.merge_environment_settings(
            prepped_request.url,
            proxies=extra_options.get("proxies", {}),
            stream=extra_options.get("stream", False),
            verify=extra_options.get("verify"),
            cert=extra_options.get("cert"),
        )

        # Send the request.
        send_kwargs: dict[str, Any] = {
            "timeout": extra_options.get("timeout"),
            "allow_redirects": extra_options.get("allow_redirects", True),
        }
        send_kwargs.update(settings)

        try:
            response = session.send(prepped_request, **send_kwargs)

            if extra_options.get("check_response", True):
                self.check_response(response)
            return response

        except requests.exceptions.ConnectionError as ex:
            self.log.warning("%s Tenacity will retry to execute the operation", ex)
            raise ex

    def run_with_advanced_retry(self, _retry_args: dict[Any, Any], *args: Any, **kwargs: Any) -> Any:
        """
        Run the hook with retry.

        This is useful for connectors which might be disturbed by intermittent
        issues and should not instantly fail.

        :param _retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity


        .. code-block:: python

            hook = HttpHook(http_conn_id="my_conn", method="GET")
            retry_args = dict(
                wait=tenacity.wait_exponential(),
                stop=tenacity.stop_after_attempt(10),
                retry=tenacity.retry_if_exception_type(Exception),
            )
            hook.run_with_advanced_retry(endpoint="v1/test", _retry_args=retry_args)

        """
        self._retry_obj = tenacity.Retrying(**_retry_args)

        # TODO: remove ignore type when https://github.com/jd/tenacity/issues/428 is resolved
        return self._retry_obj(self.run, *args, **kwargs)  # type: ignore

    def url_from_endpoint(self, endpoint: str | None) -> str:
        """Combine base url with endpoint."""
        return _url_from_endpoint(base_url=self.base_url, endpoint=endpoint)

    def test_connection(self):
        """Test HTTP Connection."""
        try:
            self.run()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)


class HttpAsyncHook(BaseHook):
    """
    Interact with HTTP servers asynchronously.

    :param method: the API method to be called
    :param http_conn_id: http connection id that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    """

    conn_name_attr = "http_conn_id"
    default_conn_name = "http_default"
    conn_type = "http"
    hook_name = "HTTP"

    def __init__(
        self,
        method: str = "POST",
        http_conn_id: str = default_conn_name,
        auth_type: Any = aiohttp.BasicAuth,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        self.http_conn_id = http_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self._retry_obj: Callable[..., Any]
        self.auth_type: Any = auth_type
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    async def run(
        self,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        json: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
    ) -> ClientResponse:
        """
        Perform an asynchronous HTTP request call.

        :param endpoint: Endpoint to be called, i.e. ``resource/v1/query?``.
        :param data: Payload to be uploaded or request parameters.
        :param json: Payload to be uploaded as JSON.
        :param headers: Additional headers to be passed through as a dict.
        :param extra_options: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as
            ``aiohttp.ClientSession().get(json=obj)``.
        """
        extra_options = extra_options or {}

        # headers may be passed through directly or in the "extra" field in the connection
        # definition
        _headers = {}
        auth = None

        if self.http_conn_id:
            conn = await sync_to_async(self.get_connection)(self.http_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url += f":{conn.port}"
            if conn.login:
                auth = self.auth_type(conn.login, conn.password)
            if conn.extra:
                extra = self._process_extra_options_from_connection(conn=conn, extra_options=extra_options)

                try:
                    _headers.update(extra)
                except TypeError:
                    self.log.warning("Connection to %s has invalid extra field.", conn.host)
        if headers:
            _headers.update(headers)

        url = _url_from_endpoint(self.base_url, endpoint)

        async with aiohttp.ClientSession() as session:
            if self.method == "GET":
                request_func = session.get
            elif self.method == "POST":
                request_func = session.post
            elif self.method == "PATCH":
                request_func = session.patch
            elif self.method == "HEAD":
                request_func = session.head
            elif self.method == "PUT":
                request_func = session.put
            elif self.method == "DELETE":
                request_func = session.delete
            elif self.method == "OPTIONS":
                request_func = session.options
            else:
                raise AirflowException(f"Unexpected HTTP Method: {self.method}")

            for attempt in range(1, 1 + self.retry_limit):
                response = await request_func(
                    url,
                    params=data if self.method == "GET" else None,
                    data=data if self.method in ("POST", "PUT", "PATCH") else None,
                    json=json,
                    headers=_headers,
                    auth=auth,
                    **extra_options,
                )
                try:
                    response.raise_for_status()
                except ClientResponseError as e:
                    self.log.warning(
                        "[Try %d of %d] Request to %s failed.",
                        attempt,
                        self.retry_limit,
                        url,
                    )
                    if not self._retryable_error_async(e) or attempt == self.retry_limit:
                        self.log.exception("HTTP error with status: %s", e.status)
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        raise AirflowException(f"{e.status}:{e.message}")
                    else:
                        await asyncio.sleep(self.retry_delay)
                else:
                    return response
            else:
                raise NotImplementedError  # should not reach this, but makes mypy happy

    @classmethod
    def _process_extra_options_from_connection(cls, conn: Connection, extra_options: dict) -> dict:
        extra = conn.extra_dejson
        extra.pop("stream", None)
        extra.pop("cert", None)
        proxies = extra.pop("proxies", extra.pop("proxy", None))
        timeout = extra.pop("timeout", None)
        verify_ssl = extra.pop("verify", extra.pop("verify_ssl", None))
        allow_redirects = extra.pop("allow_redirects", None)
        max_redirects = extra.pop("max_redirects", None)
        trust_env = extra.pop("trust_env", None)

        if proxies is not None and "proxy" not in extra_options:
            extra_options["proxy"] = proxies
        if timeout is not None and "timeout" not in extra_options:
            extra_options["timeout"] = timeout
        if verify_ssl is not None and "verify_ssl" not in extra_options:
            extra_options["verify_ssl"] = verify_ssl
        if allow_redirects is not None and "allow_redirects" not in extra_options:
            extra_options["allow_redirects"] = allow_redirects
        if max_redirects is not None and "max_redirects" not in extra_options:
            extra_options["max_redirects"] = max_redirects
        if trust_env is not None and "trust_env" not in extra_options:
            extra_options["trust_env"] = trust_env
        return extra

    def _retryable_error_async(self, exception: ClientResponseError) -> bool:
        """
        Determine whether an exception may successful on a subsequent attempt.

        It considers the following to be retryable:
            - requests_exceptions.ConnectionError
            - requests_exceptions.Timeout
            - anything with a status code >= 500

        Most retryable errors are covered by status code >= 500.
        """
        if exception.status == 429:
            # don't retry for too Many Requests
            return False
        if exception.status == 413:
            # don't retry for payload Too Large
            return False

        return exception.status >= 500
