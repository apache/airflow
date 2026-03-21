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
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

import aiohttp
import tenacity
from aiohttp import ClientResponseError
from pydantic import BaseModel
from requests import PreparedRequest, Request, Response, Session
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, HTTPError
from requests.models import DEFAULT_REDIRECT_LIMIT
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter
from tenacity import retry_if_exception

from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.providers.http.exceptions import HttpErrorException, HttpMethodException
from airflow.utils.log.logging_mixin import LoggingMixin

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


def _retryable_error_async(exception: BaseException) -> bool:
    """
    Determine whether an exception may successful on a subsequent attempt.

    It considers the following to be retryable:
    - requests_exceptions.ConnectionError
    - requests_exceptions.Timeout
    - anything with a status code >= 500

    Most retryable errors are covered by status code >= 500.
    """
    if not isinstance(exception, ClientResponseError):
        return False
    if exception.status == 429:
        # don't retry for too Many Requests
        return False
    if exception.status == 413:
        # don't retry for payload Too Large
        return False
    return exception.status >= 500


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


class SessionConfig(BaseModel):
    """Configuration container for an asynchronous HTTP session."""

    base_url: str
    headers: dict[str, Any] | None = None
    auth: aiohttp.BasicAuth | None = None
    extra_options: dict[str, Any] | None = None


class AsyncHttpSession(LoggingMixin):
    """
    Wrapper around an ``aiohttp.ClientSession`` providing a session bound ``HttpAsyncHook``.

    This class binds an asynchronous HTTP client session to an ``HttpAsyncHook`` and applies connection
    configuration, authentication, headers, and retry logic consistently across requests. A single
    ``AsyncHttpSession`` instance is intended to be used for multiple HTTP calls within the same logical session.

    :param hook: The ``HttpAsyncHook`` instance that owns this session and provides connection-level behavior
        such as retries and logging.
    :param request: A callable used to perform the underlying HTTP request. This is typically a bound
        ``aiohttp.ClientSession`` request method.
    :param config: Resolved session configuration containing base URL, headers, and authentication settings.
    """

    def __init__(
        self,
        hook: HttpAsyncHook,
        request: Callable[..., Awaitable[ClientResponse]],
        config: SessionConfig,
    ) -> None:
        super().__init__()
        self._hook = hook
        self._request = request
        self.config = config

    @property
    def http_conn_id(self) -> str:
        return self._hook.http_conn_id

    @property
    def base_url(self) -> str:
        return self.config.base_url

    @property
    def method(self) -> str:
        return self._hook.method

    @property
    def retry_limit(self) -> int:
        return self._hook.retry_limit

    @property
    def retry_delay(self) -> float:
        return self._hook.retry_delay

    @property
    def headers(self) -> dict[str, Any] | None:
        return self.config.headers

    @property
    def extra_options(self) -> dict[str, Any] | None:
        return self.config.extra_options

    @property
    def auth(self) -> aiohttp.BasicAuth | None:
        return self.config.auth

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
        from tenacity import AsyncRetrying, stop_after_attempt, wait_fixed

        url = _url_from_endpoint(self.base_url, endpoint)
        merged_headers = {**(self.headers or {}), **(headers or {})}
        extra_options = {**(self.extra_options or {}), **(extra_options or {})}

        async def request_func() -> ClientResponse:
            response = await self._request(
                url,
                params=data if self.method == "GET" else None,
                data=data if self.method in {"POST", "PUT", "PATCH"} else None,
                json=json,
                headers=merged_headers,
                auth=self.auth,
                **extra_options,
            )
            response.raise_for_status()
            return response

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.retry_limit),
            wait=wait_fixed(self.retry_delay),
            retry=retry_if_exception(_retryable_error_async),
            reraise=True,
        ):
            with attempt:
                try:
                    return await request_func()
                except ClientResponseError as e:
                    self.log.warning(
                        "[Try %d of %d] Request to %s failed.",
                        attempt.retry_state.attempt_number,
                        self.retry_limit,
                        url,
                    )
                    raise e

        raise NotImplementedError  # should not reach this, but makes mypy happy


class HttpAsyncHook(BaseHook):
    """
    Interact with HTTP servers asynchronously.

    :param method: the API method to be called
    :param http_conn_id: http connection id that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    :param retry_limit: Maximum number of times to retry this job if it fails  (default is 3)
    :param retry_delay: Delay between retry attempts (default is 1.0)
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
            raise ValueError("Retry limit must be greater or equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self._config: SessionConfig | None = None

    def _get_request_func(self, session: aiohttp.ClientSession) -> Callable[..., Any]:
        method = self.method
        if method == "GET":
            return session.get
        if method == "POST":
            return session.post
        if method == "PATCH":
            return session.patch
        if method == "HEAD":
            return session.head
        if method == "PUT":
            return session.put
        if method == "DELETE":
            return session.delete
        if method == "OPTIONS":
            return session.options
        raise HttpMethodException(f"Unexpected HTTP Method: {method}")

    async def config(self) -> SessionConfig:
        if not self._config:
            from airflow.providers.common.compat.connection import get_async_connection

            base_url: str = self.base_url
            auth: aiohttp.BasicAuth | None = None
            headers: dict[str, Any] = {}
            extra_options: dict[str, Any] = {}

            if self.http_conn_id:
                conn = await get_async_connection(conn_id=self.http_conn_id)

                if conn.host and "://" in conn.host:
                    base_url = conn.host
                else:
                    schema = conn.schema or "http"
                    base_url = f"{schema}://{conn.host or ''}"

                if conn.port:
                    base_url += f":{conn.port}"

                if conn.login:
                    auth = self.auth_type(conn.login, conn.password)

                if conn.extra:
                    conn_extra_options, extra_options = _process_extra_options_from_connection(
                        conn=conn, extra_options={}
                    )
                    headers.update(conn_extra_options)

            self._config = SessionConfig(
                base_url=base_url,
                headers=headers,
                auth=auth,
                extra_options=extra_options,
            )
        return self._config

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncHttpSession, None]:
        """
        Create an ``AsyncHttpSession`` bound to a single ``aiohttp.ClientSession``.

        Airflow connection resolution happens exactly once here.
        """
        async with aiohttp.ClientSession() as session:
            request = self._get_request_func(session=session)
            config = await self.config()
            yield AsyncHttpSession(hook=self, request=request, config=config)

    async def run(
        self,
        session: aiohttp.ClientSession | None = None,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        json: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
    ) -> ClientResponse:
        """
        Perform an asynchronous HTTP request call.

        :param session: ``aiohttp.ClientSession``
        :param endpoint: Endpoint to be called, i.e. ``resource/v1/query?``.
        :param data: Payload to be uploaded or request parameters.
        :param json: Payload to be uploaded as JSON.
        :param headers: Additional headers to be passed through as a dict.
        :param extra_options: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as
            ``aiohttp.ClientSession().get(json=obj)``.
        """
        try:
            if session is not None:
                request = self._get_request_func(session=session)
                config = await self.config()
                return await AsyncHttpSession(hook=self, request=request, config=config).run(
                    endpoint=endpoint, data=data, json=json, headers=headers, extra_options=extra_options
                )

            async with self.session() as http:
                return await http.run(
                    endpoint=endpoint, data=data, json=json, headers=headers, extra_options=extra_options
                )
        except ClientResponseError as e:
            raise HttpErrorException(f"{e.status}:{e.message}")
