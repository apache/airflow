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
import warnings
from contextlib import suppress
from functools import cached_property
from json import JSONDecodeError
from logging import Logger
from typing import TYPE_CHECKING, Any, Callable

import aiohttp
import requests
import tenacity
from aiohttp import ClientResponseError
from asgiref.sync import sync_to_async
from requests.auth import HTTPBasicAuth
from requests.models import DEFAULT_REDIRECT_LIMIT
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

from airflow.compat.functools import cache
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse


DEFAULT_AUTH_TYPES = frozenset(
    {
        "requests.auth.HTTPBasicAuth",
        "requests.auth.HTTPProxyAuth",
        "requests.auth.HTTPDigestAuth",
        "requests_kerberos.HTTPKerberosAuth",
        "aiohttp.BasicAuth",
    }
)


@cache
def get_auth_types() -> frozenset[str]:
    """
    Get comma-separated extra auth_types from airflow config.

    Those auth_types can then be used in Connection configuration.
    """
    from airflow.configuration import conf

    auth_types = DEFAULT_AUTH_TYPES.copy()
    extra_auth_types = conf.get("http", "extra_auth_types", fallback=None)
    if extra_auth_types:
        auth_types |= frozenset({field.strip() for field in extra_auth_types.split(",")})
    return auth_types


class ConnectionWithExtra(Connection):
    """
    Patched Connection class added for backward compatibility.

    Implements the `get_extra_dejson` method which was added in the Connection class in Airflow 2.10.0.
    This patched class must be removed once the http provider depends on Airflow 2.10.0 or higher.
    """

    def __init__(
        self,
        conn_id: str | None = None,
        conn_type: str | None = None,
        description: str | None = None,
        host: str | None = None,
        login: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        port: int | None = None,
        extra: str | dict | None = None,
        uri: str | None = None,
    ):
        super().__init__(conn_id, conn_type, description, host, login, password, schema, port, extra, uri)

    def get_extra_dejson(self, nested: bool = False) -> dict:
        """
        Deserialize extra property to JSON.

        :param nested: Determines whether nested structures are also deserialized into JSON (default False).
        """
        extra = {}

        if self.extra:
            try:
                if nested:
                    for key, value in json.loads(self.extra).items():
                        extra[key] = value
                        if isinstance(value, str):
                            with suppress(JSONDecodeError):
                                extra[key] = json.loads(value)
                else:
                    extra = json.loads(self.extra)
            except JSONDecodeError:
                self.log.exception("Failed parsing the json for conn_id %s", self.conn_id)

            # Mask sensitive keys from this list
            mask_secret(extra)

        return extra


class HttpHookMixin:
    """
    Common superclass for the HttpHook and HttpAsyncHook.

    Implements methods to create a Connection.
    """

    http_conn_id: str
    conn_type: str
    base_url: str
    auth_type: Any
    default_auth_type = HTTPBasicAuth
    get_connection: Callable
    log: Logger

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to the connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextAreaFieldWidget, BS3TextFieldWidget, Select2Widget
        from flask_babel import lazy_gettext
        from wtforms.fields import BooleanField, SelectField, StringField, TextAreaField

        default_auth_type: str = ""
        auth_types_choices = frozenset({default_auth_type}) | get_auth_types()

        return {
            "timeout": StringField(lazy_gettext("Timeout"), widget=BS3TextFieldWidget()),
            "allow_redirects": BooleanField(lazy_gettext("Allow redirects"), default=True),
            "proxies": TextAreaField(lazy_gettext("Proxies"), widget=BS3TextAreaFieldWidget()),
            "stream": BooleanField(lazy_gettext("Stream"), default=False),
            "verify": BooleanField(lazy_gettext("Verify"), default=True),
            "trust_env": BooleanField(lazy_gettext("Trust env"), default=True),
            "cert": StringField(lazy_gettext("Cert"), widget=BS3TextFieldWidget()),
            "max_redirects": StringField(
                lazy_gettext("Max redirects"), widget=BS3TextFieldWidget(), default=DEFAULT_REDIRECT_LIMIT
            ),
            "auth_type": SelectField(
                lazy_gettext("Auth type"),
                choices=[(clazz, clazz) for clazz in auth_types_choices],
                widget=Select2Widget(),
                default=default_auth_type,
            ),
            "auth_kwargs": TextAreaField(lazy_gettext("Auth kwargs"), widget=BS3TextAreaFieldWidget()),
            "headers": TextAreaField(
                lazy_gettext("Headers"),
                widget=BS3TextAreaFieldWidget(),
                description=(
                    "Warning: Passing headers parameters directly in 'Extra' field is deprecated, and "
                    "will be removed in a future version of the Http provider. Use the 'Headers' "
                    "field instead."
                ),
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Hive Client Wrapper connection."""
        return {
            "hidden_fields": ["extra"],
            "relabeling": {},
        }

    @cached_property
    def connection(self) -> Connection:
        """
        Return a cached connection property.

        This method calls the original get_connection method from the BaseHook and returns a patched version
        of the Connection class which also has the `get_extra_dejson` method that has been added in Apache
        Airflow since 2.10.0. Once the provider depends on Airflow version 2.10.0 or higher, this method and
        the ConnectionWithExtra class can be removed.
        """
        conn = BaseHook.get_connection(conn_id=self.http_conn_id)

        return ConnectionWithExtra(
            conn_id=self.http_conn_id,
            conn_type=conn.conn_type,
            description=conn.description,
            host=conn.host,
            login=conn.login,
            password=conn.password,
            schema=conn.schema,
            port=conn.port,
            extra=conn.extra,
        )

    def load_connection_settings(self, *, headers: dict[Any, Any] | None = None) -> tuple[dict, Any, dict]:
        """
        Load and update the class with Connection Settings.

        Load the settings from the Connection and update the class.
        Returns the headers and auth which are later passed into a request.Session
        (for the HttpHook) or an aiohttp.Session (for the HttpAsyncHook).
        """
        _headers = {}
        _auth = None
        _session_conf = {}

        if self.http_conn_id:
            conn = self.connection

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = f"{schema}://{host}"

            if conn.port:
                self.base_url += f":{conn.port}"

            conn_extra: dict = self._parse_extra(conn_extra=conn.get_extra_dejson(nested=True))
            _session_conf = conn_extra["session_conf"]
            auth_args: list[str | None] = [conn.login, conn.password]
            auth_kwargs: dict[str, Any] = conn_extra["auth_kwargs"]
            auth_type: Any = self.auth_type or self._load_conn_auth_type(module_name=conn_extra["auth_type"])

            self.log.debug("auth_type: %s", auth_type)

            if auth_type:
                if any(auth_args) or auth_kwargs:
                    _auth = auth_type(*auth_args, **auth_kwargs)
                elif conn.login:
                    _auth = auth_type(conn.login, conn.password)
                else:
                    _auth = auth_type()

            extra_headers = conn_extra["headers"]
            if extra_headers:
                try:
                    _headers.update(extra_headers)
                except TypeError:
                    self.log.warning("Connection to %s has invalid extra field.", conn.host)
        if headers:
            _headers.update(headers)

        self.log.debug("headers: %s", _headers)
        self.log.debug("auth: %s", _auth)
        self.log.debug("session_conf: %s", _session_conf)

        return _headers, _auth, _session_conf

    def _parse_extra(self, conn_extra: dict) -> dict:
        """
        Parse the settings from 'extra' into dict.

        The "auth_kwargs" and "headers" data from TextAreaField are returned as
        string via the 'extra' field. This method converts the data to dict.
        """
        extra = conn_extra.copy()
        session_conf = {}
        timeout = extra.pop(
            "timeout", None
        )  # ignore this as timeout is only accepted in request method of Session
        if timeout is not None:
            session_conf["timeout"] = timeout
        allow_redirects = extra.pop(
            "allow_redirects", None
        )  # ignore this as only max_redirects is accepted in Session
        if allow_redirects is not None:
            session_conf["allow_redirects"] = allow_redirects
        session_conf["proxies"] = extra.pop("proxies", extra.pop("proxy", {}))
        session_conf["stream"] = extra.pop("stream", False)
        session_conf["verify"] = extra.pop("verify", extra.pop("verify_ssl", True))
        session_conf["trust_env"] = extra.pop("trust_env", True)
        cert = extra.pop("cert", None)
        if cert is not None:
            session_conf["cert"] = cert
        session_conf["max_redirects"] = extra.pop("max_redirects", DEFAULT_REDIRECT_LIMIT)
        auth_type: str | None = extra.pop("auth_type", None)
        auth_kwargs = extra.pop("auth_kwargs", {})
        headers = extra.pop("headers", {})

        if extra:
            warnings.warn(
                "Passing headers parameters directly in 'Extra' field is deprecated, and "
                "will be removed in a future version of the Http provider. Use the 'Headers' "
                "field instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            headers = {**extra, **headers}

        return {
            "auth_type": auth_type,
            "auth_kwargs": auth_kwargs,
            "session_conf": session_conf,
            "headers": headers,
        }

    def _load_conn_auth_type(self, module_name: str | None) -> Any:
        """
        Load auth_type module from extra Connection parameters.

        Check if the auth_type module is listed in 'extra_auth_types' and load it.
        This method protects against the execution of random modules.
        """
        if module_name:
            if module_name in get_auth_types():
                try:
                    module = import_string(module_name)
                    self._is_auth_type_setup = True
                    self.log.info("Loaded auth_type: %s", module_name)
                    return module
                except ImportError as error:
                    self.log.error("Cannot import auth_type '%s' due to: %s", module_name, error)
                    raise AirflowException(error)
            self.log.warning(
                "Skipping import of auth_type '%s'. The class should be listed in "
                "'extra_auth_types' config of the http provider.",
                module_name,
            )
        return None

    def url_from_endpoint(self, endpoint: str | None) -> str:
        """Combine base url with endpoint."""
        if self.base_url and not self.base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
            return self.base_url + "/" + endpoint
        return (self.base_url or "") + (endpoint or "")


class HttpHook(HttpHookMixin, BaseHook):
    """
    Interact with HTTP servers.

    To configure the auth_type, in addition to the `auth_type` parameter, you can also:
        * set the `auth_type` parameter in the Connection settings.
        * define extra parameters passed to the `auth_type` class via the `auth_kwargs`, in the Connection
            settings. The class will be instantiated with those parameters.

    See :doc:`/connections/http` for full documentation.

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

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        return super().get_connection_form_widgets()

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return super().get_ui_field_behaviour()

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
        self.auth_type: Any = auth_type
        self.tcp_keep_alive = tcp_keep_alive
        self.keep_alive_idle = tcp_keep_alive_idle
        self.keep_alive_count = tcp_keep_alive_count
        self.keep_alive_interval = tcp_keep_alive_interval

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_conn(self, headers: dict[Any, Any] | None = None) -> requests.Session:
        """
        Create a Requests HTTP session.

        :param headers: additional headers to be passed through as a dictionary.
                        Note: Headers may also be passed in the "Headers" field in the Connection definition
        """
        headers, auth, session_conf = self.load_connection_settings(headers=headers)

        session_conf.pop(
            "timeout", None
        )  # ignore this as timeout is only accepted in request method of Session
        session_conf.pop("allow_redirects", None)  # ignore this as only max_redirects is accepted in Session

        session = requests.Session()
        session.auth = auth
        session.proxies = session_conf["proxies"]
        session.stream = session_conf["stream"]
        session.verify = session_conf["verify"]
        session.cert = session_conf.get("cert")
        session.max_redirects = session_conf["max_redirects"]
        session.trust_env = session_conf["trust_env"]

        try:
            session.headers.update(headers)
        except TypeError:
            self.log.warning("Connection to %s has invalid extra field.", self.http_conn_id)

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

        url = self.url_from_endpoint(endpoint)

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

    def test_connection(self):
        """Test HTTP Connection."""
        try:
            self.run()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)


class HttpAsyncHook(HttpHookMixin, BaseHook):
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
        auth_type: Any = None,
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
        _headers, auth, session_conf = await sync_to_async(self.load_connection_settings)(headers=headers)
        session_conf = self._process_session_conf(session_conf)
        session_conf.update(extra_options)

        url = self.url_from_endpoint(endpoint)

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
    def _process_session_conf(cls, session_conf: dict) -> dict:
        session_conf.pop("stream", None)
        session_conf.pop("cert", None)
        proxies = session_conf.pop("proxies")
        if proxies is not None:
            session_conf["proxy"] = proxies
        verify = session_conf.pop("verify")
        if verify is not None:
            session_conf["verify_ssl"] = verify
        session_conf.pop("trust_env")
        return session_conf

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
