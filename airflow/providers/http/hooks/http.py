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
import http
import os
from typing import Any, Callable, Dict, Optional, Type, Union

import httpx
import tenacity
from httpx import Auth, BasicAuth

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class HttpHook(BaseHook):
    """
    Interact with HTTP servers.

    :param method: the API method to be called
    :type method: str
    :param http_conn_id: :ref:`http connection<howto/connection:http>` that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :type http_conn_id: str
    :param auth_type: The auth type for the service
    :type auth_type: AuthBase of python requests lib
    """

    conn_name_attr = 'http_conn_id'
    default_conn_name = 'http_default'
    conn_type = 'http'
    hook_name = 'HTTP'

    def __init__(
        self,
        method: str = 'POST',
        http_conn_id: str = default_conn_name,
        auth_type: Type[Auth] = BasicAuth,
    ) -> None:
        super().__init__()
        self.http_conn_id = http_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self._retry_obj: Callable[..., Any]
        self.auth_type: Type[Auth] = auth_type
        self.extra_headers: Dict = {}
        self.conn = None

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_conn(
        self, headers: Optional[Dict[Any, Any]] = None, verify: bool = True, proxies=None, cert=None
    ) -> httpx.Client:
        """
        Returns httpx client for use with requests

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        :param verify: whether to verify SSL during the connection (only use for testing)
        :param proxies: A dictionary mapping proxy keys to proxy
        :param cert: client An SSL certificate used by the requested host
            to authenticate the client. Either a path to an SSL certificate file, or
            two-tuple of (certificate file, key file), or a three-tuple of (certificate
            file, key file, password).
        """
        client = httpx.Client(verify=verify, proxies=proxies, cert=cert)

        if self.http_conn_id:
            conn = self.get_connection(self.http_conn_id)
            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)
            if conn.login:
                # Note! This handles Basic Auth and DigestAuth and any other authentication that
                # supports login/password in the constructor.
                client.auth = self.auth_type(conn.login, conn.password)  # noqa
            if conn.extra:
                try:
                    client.headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning('Connection to %s has invalid extra field.', conn.host)
            # Hooks deriving from HTTP Hook might use it to query connection details
            self.conn = conn
        if headers:
            client.headers.update(headers)

        return client

    def run(
        self,
        endpoint: Optional[str],
        data: Optional[Union[Dict[str, Any], str]] = None,
        headers: Optional[Dict[str, Any]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
        **request_kwargs: Any,
    ) -> Any:
        r"""
        Performs the request

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :type endpoint: str
        :param data: payload to be uploaded or request parameters
        :type data: dict
        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        :param extra_options: additional options to be used when executing the request
            i.e. ``{'check_response': False}`` to avoid checking raising exceptions on non
            2XX or 3XX status codes. The extra options can take the following keys:
            verify, proxies, cert, stream, allow_redirects, timeout, check_response.
            See ``httpx.Client`` for description of those parameters.
        :type extra_options: dict
        :param request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``httpx.Request(json=obj)``
        """
        extra_options = extra_options or {}

        verify = extra_options.get("verify", True)
        if verify is True:
            # Only use REQUESTS_CA_BUNDLE content if verify is set to True,
            # otherwise use passed value as it can be string, or SSLcontext
            verify = os.environ.get('REQUESTS_CA_BUNDLE', verify)
        proxies = extra_options.get("proxies", None)
        cert = extra_options.get("cert", None)
        client = self.get_conn(headers, verify=verify, proxies=proxies, cert=cert)
        if self.base_url and not self.base_url.endswith('/') and endpoint and not endpoint.startswith('/'):
            url = self.base_url + '/' + endpoint
        else:
            url = (self.base_url or '') + (endpoint or '')

        if self.method == 'GET':
            # GET uses params
            req = httpx.Request(self.method, url, params=data, headers=client.headers, **request_kwargs)
        elif self.method == 'HEAD':
            # HEAD doesn't use params
            req = httpx.Request(self.method, url, headers=client.headers, **request_kwargs)
        else:
            # Others use data
            req = httpx.Request(self.method, url, headers=client.headers, data=data, **request_kwargs)

        # Send the request
        send_kwargs = {
            "stream": extra_options.get("stream", False),
            "allow_redirects": extra_options.get("allow_redirects", True),
            "timeout": extra_options.get("timeout"),
        }
        self.log.info("Sending '%s' to url: %s", self.method, url)

        try:
            response = client.send(req, **send_kwargs)
            if extra_options.get('check_response', True):
                self.check_response(response)
            return response

        except httpx.NetworkError as ex:
            self.log.warning('%s Tenacity will retry to execute the operation', ex)
            raise ex

    def check_response(self, response: httpx.Response) -> None:
        """
        Checks the status code and raise an AirflowException exception on non 2XX or 3XX
        status codes

        :param response: A requests response object
        :type response: requests.response
        """
        try:
            response.raise_for_status()
        except httpx.HTTPError:
            phrase = 'Unknown'
            try:
                phrase = http.HTTPStatus(response.status_code).phrase
            except ValueError:
                pass
            self.log.error("HTTP error: %s", phrase)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.text)

    def run_with_advanced_retry(self, _retry_args: Dict[Any, Any], *args: Any, **kwargs: Any) -> Any:
        """
        Runs Hook.run() with a Tenacity decorator attached to it. This is useful for
        connectors which might be disturbed by intermittent issues and should not
        instantly fail.

        :param _retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity
        :type _retry_args: dict


        .. code-block:: python

            hook = HttpHook(http_conn_id='my_conn',method='GET')
            retry_args = dict(
                 wait=tenacity.wait_exponential(),
                 stop=tenacity.stop_after_attempt(10),
                 retry=requests.exceptions.ConnectionError
             )
             hook.run_with_advanced_retry(
                     endpoint='v1/test',
                     _retry_args=retry_args
                 )

        """
        self._retry_obj = tenacity.Retrying(**_retry_args)

        return self._retry_obj(self.run, *args, **kwargs)
