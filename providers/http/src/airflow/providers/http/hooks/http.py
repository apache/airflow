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

import copy
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

import aiohttp
import tenacity
from aiohttp import ClientResponseError
from asgiref.sync import sync_to_async
from requests import PreparedRequest, Request, Response, Session
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, HTTPError
from requests.models import DEFAULT_REDIRECT_LIMIT
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.providers.http.exceptions import HttpErrorException, HttpMethodException

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse
    from requests.adapters import HTTPAdapter

    from airflow.models import Connection


def _url_from_endpoint(base_url: str | None, endpoint: str | None) -> str:
    """Combine base url with endpoint."""
    if base_url and not base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
        return f"{base_url}/{endpoint}"
    return (base_url or "") + (endpoint or "")


def _process_extra_options_from_connection(
    conn, extra_options: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Return the updated extra options from the connection, as well as those passed.

    :param conn: The HTTP Connection object passed to the Hook
    :param extra_options: Use-defined extra options
    :return: (tuple)
    """
    # Copy, to prevent changing conn.extra_dejson and extra_options
    conn_extra_options: dict = copy.copy(conn.extra_dejson)
    passed_extra_options: dict = copy.copy(extra_options)

    stream = conn_extra_options.pop("stream", None)
    cert = conn_extra_options.pop("cert", None)
    proxies = conn_extra_options.pop("proxies", conn_extra_options.pop("proxy", None))
    timeout = conn_extra_options.pop("timeout", None)
    verify_ssl = conn_extra_options.pop("verify", conn_extra_options.pop("verify_ssl", None))
    allow_redirects = conn_extra_options.pop("allow_redirects", None)
    max_redirects = conn_extra_options.pop("max_redirects", None)
    trust_env = conn_extra_options.pop("trust_env", None)
    check_response = conn_extra_options.pop("check_response", None)

    if stream is not None and "stream" not in passed_extra_options:
        passed_extra_options["stream"] = stream
    if cert is not None and "cert" not in passed_extra_options:
        passed_extra_options["cert"] = cert
    if proxies is not None and "proxy" not in passed_extra_options:
        passed_extra_options["proxy"] = proxies
    if timeout is not None and "timeout" not in passed_extra_options:
        passed_extra_options["timeout"] = timeout
    if verify_ssl is not None and "verify_ssl" not in passed_extra_options:
        passed_extra_options["verify_ssl"] = verify_ssl
    if allow_redirects is not None and "allow_redirects" not in passed_extra_options:
        passed_extra_options["allow_redirects"] = allow_redirects
    if max_redirects is not None and "max_redirects" not in passed_extra_options:
        passed_extra_options["max_redirects"] = max_redirects
    if trust_env is not None and "trust_env" not in passed_extra_options:
        passed_extra_options["trust_env"] = trust_env
    if check_response is not None and "check_response" not in passed_extra_options:
        passed_extra_options["check_response"] = check_response

    return conn_extra_options, passed_extra_options


class HttpHook(BaseHook):
    """
    Interact with HTTP servers.

    :param method: the API method to be called
    :param http_conn_id: :ref:`http connection<howto/connection:http>` that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    :param adapter: An optional instance of `requests.adapters.HTTPAdapter` to mount for the session.
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    """

    conn_name_attr = "http_conn_id"
    default_conn_name = "http_default"
    conn_type = "http"
    hook_name = "HTTP"
    default_host = ""
    default_headers: dict[str, str] = {}

    def __init__(
        self,
        method: str = "POST",
        http_conn_id: str = default_conn_name,
        auth_type: Any = None,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
        adapter: HTTPAdapter | None = None,
    ) -> None:
        super().__init__()
        self.http_conn_id = http_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self._base_url_initialized: bool = False
        self._retry_obj: Callable[..., Any]
        self._auth_type: Any = auth_type

        # If no adapter is provided, use TCPKeepAliveAdapter (default behavior)
        self.adapter = adapter
        if tcp_keep_alive and adapter is None:
            self.keep_alive_adapter = TCPKeepAliveAdapter(
                idle=tcp_keep_alive_idle,
                count=tcp_keep_alive_count,
                interval=tcp_keep_alive_interval,
            )
        else:
            self.keep_alive_adapter = None

        self.merged_extra: dict = {}

    @property
    def auth_type(self):
        return self._auth_type or HTTPBasicAuth

    @auth_type.setter
    def auth_type(self, v):
        self._auth_type = v

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_conn(
        self, headers: dict[Any, Any] | None = None, extra_options: dict[str, Any] | None = None
    ) -> Session:
        """
        Create a Requests HTTP session.

        :param headers: Additional headers to be passed through as a dictionary.
        :param extra_options: additional options to be used when executing the request
        :return: A configured requests.Session object.
        """
        session = Session()
        connection = self.get_connection(self.http_conn_id)
        self._set_base_url(connection)
        session = self._configure_session_from_auth(session, connection)  # type: ignore[arg-type]

        # Since get_conn can be called outside of run, we'll check this again
        extra_options = extra_options or {}

        if connection.extra or extra_options:
            # These are being passed from to _configure_session_from_extra, no manipulation has been done yet
            session = self._configure_session_from_extra(session, connection, extra_options)

        session = self._configure_session_from_mount_adapters(session)
        if self.default_headers:
            session.headers.update(self.default_headers)
        if headers:
            session.headers.update(headers)
        return session

    def _set_base_url(self, connection) -> None:
        host = connection.host or self.default_host
        schema = connection.schema or "http"
        # RFC 3986 (https://www.rfc-editor.org/rfc/rfc3986.html#page-16)
        if "://" in host:
            self.base_url = host
        else:
            self.base_url = f"{schema}://{host}" if host else f"{schema}://"
        if connection.port:
            self.base_url = f"{self.base_url}:{connection.port}"
        parsed = urlparse(self.base_url)
        if not parsed.scheme:
            raise ValueError(f"Invalid base URL: Missing scheme in {self.base_url}")
        self._base_url_initialized = True

    def _configure_session_from_auth(self, session: Session, connection: Connection) -> Session:
        session.auth = self._extract_auth(connection)
        return session

    def _extract_auth(self, connection: Connection) -> Any | None:
        if connection.login:
            return self.auth_type(connection.login, connection.password)
        if self._auth_type:
            return self.auth_type()
        return None

    def _configure_session_from_extra(
        self, session: Session, connection, extra_options: dict[str, Any]
    ) -> Session:
        """
        Configure the session using both the extra field from the Connection and passed in extra_options.

        :param session: (Session)
        :param connection: HTTP Connection passed into Hook
        :param extra_options: (dict)
        :return: (Session)
        """
        # This is going to update self.merged_extra, which will be used below
        conn_extra_options, self.merged_extra = _process_extra_options_from_connection(
            connection, extra_options
        )

        session.proxies = self.merged_extra.get("proxies", self.merged_extra.get("proxy", {}))
        session.stream = self.merged_extra.get("stream", False)
        session.verify = self.merged_extra.get("verify", self.merged_extra.get("verify_ssl", True))
        session.cert = self.merged_extra.get("cert", None)
        session.max_redirects = cast("int", self.merged_extra.get("max_redirects", DEFAULT_REDIRECT_LIMIT))
        session.trust_env = self.merged_extra.get("trust_env", True)

        try:
            session.headers.update(conn_extra_options)
        except TypeError:
            self.log.warning("Connection to %s has invalid extra field.", connection.host)

        return session

    def _configure_session_from_mount_adapters(self, session: Session) -> Session:
        scheme = urlparse(self.base_url).scheme
        if not scheme:
            raise ValueError(
                f"Cannot mount adapters: {self.base_url} does not include a valid scheme (http or https)."
            )
        if self.adapter:
            session.mount(f"{scheme}://", self.adapter)
        elif self.keep_alive_adapter:
            session.mount("http://", self.keep_alive_adapter)
            session.mount("https://", self.keep_alive_adapter)
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
        session = self.get_conn(headers, extra_options)  # This sets self.merged_extra, which is used later
        url = self.url_from_endpoint(endpoint)

        if self.method == "GET":
            # GET uses params
            req = Request(self.method, url, params=data, headers=headers, **request_kwargs)
        elif self.method == "HEAD":
            # HEAD doesn't use params
            req = Request(self.method, url, headers=headers, **request_kwargs)
        else:
            # Others use data
            req = Request(self.method, url, data=data, headers=headers, **request_kwargs)

        prepped_request = session.prepare_request(req)
        self.log.debug("Sending '%s' to url: %s", self.method, url)

        # This is referencing self.merged_extra, which is update by _process ...
        return self.run_and_check(session, prepped_request, self.merged_extra)

    def check_response(self, response: Response) -> None:
        """
        Check the status code and raise on failure.

        :param response: A requests response object.
        :raise AirflowException: If the response contains a status code not
            in the 2xx and 3xx range.
        """
        try:
            response.raise_for_status()
        except HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)

    def run_and_check(
        self,
        session: Session,
        prepped_request: PreparedRequest,
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
        settings = session.merge_environment_settings(
            prepped_request.url,
            proxies=session.proxies,
            stream=session.stream,
            verify=session.verify,
            cert=session.cert,
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

        except ConnectionError as ex:
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
        return self._retry_obj(self.run, *args, **kwargs)

    def url_from_endpoint(self, endpoint: str | None) -> str:
        """Combine base url with endpoint."""
        # Ensure base_url is set by initializing it if it hasn't been initialized yet
        if not self._base_url_initialized and not self.base_url:
            connection = self.get_connection(self.http_conn_id)
            self._set_base_url(connection)
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
        session: aiohttp.ClientSession,
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
                conn_extra_options, extra_options = _process_extra_options_from_connection(
                    conn=conn, extra_options=extra_options
                )

                try:
                    _headers.update(conn_extra_options)
                except TypeError:
                    self.log.warning("Connection to %s has invalid extra field.", conn.host)
        if headers:
            _headers.update(headers)

        url = _url_from_endpoint(self.base_url, endpoint)

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
            raise HttpMethodException(f"Unexpected HTTP Method: {self.method}")

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
                    raise HttpErrorException(f"{e.status}:{e.message}")
            else:
                return response

        raise NotImplementedError  # should not reach this, but makes mypy happy

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
