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
"""This module contains operator to move data from HTTP endpoint to S3."""
from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook

if TYPE_CHECKING:
    from collections.abc import Sequence

    from requests.auth import AuthBase

    from airflow.utils.context import Context


class HttpToS3Operator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action and store the result in S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:HttpToS3Operator`

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
    :param s3_bucket: Name of the S3 bucket where to save the object. (templated)
        It should be omitted when ``s3_key`` is provided as a full s3:// url.
    :param s3_key: The key of the object to be created. (templated)
        It can be either full s3:// style url or relative path from root level.
        When it's specified as a full s3:// url, please omit ``s3_bucket``.
    :param replace: If True, it will overwrite the key if it already exists
    :param encrypt: If True, the file will be encrypted on the server-side
        by S3 and will be stored in an encrypted form while at rest in S3.
    :param acl_policy: String specifying the canned ACL policy for the file being
        uploaded to the S3 bucket.
    :param aws_conn_id: Connection id of the S3 connection to use
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.

        You can provide the following values:

        - False: do not validate SSL certificates. SSL will still be used,
                 but SSL certificates will not be
                 verified.
        - path/to/cert/bundle.pem: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    """

    template_fields: Sequence[str] = ("endpoint", "data", "headers", "s3_bucket", "s3_key")
    template_fields_renderers = {"headers": "json", "data": "py"}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        method: str = "GET",
        data: Any = None,
        headers: dict[str, str] | None = None,
        extra_options: dict[str, Any] | None = None,
        http_conn_id: str = "http_default",
        log_response: bool = False,
        auth_type: type[AuthBase] | None = None,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
        s3_bucket: str | None = None,
        s3_key: str,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: str | None = None,
        aws_conn_id: str = "aws_default",
        verify: str | bool | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data or {}
        self.extra_options = extra_options or {}
        self.log_response = log_response
        self.auth_type = auth_type
        self.tcp_keep_alive = tcp_keep_alive
        self.tcp_keep_alive_idle = tcp_keep_alive_idle
        self.tcp_keep_alive_count = tcp_keep_alive_count
        self.tcp_keep_alive_interval = tcp_keep_alive_interval
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.replace = replace
        self.encrypt = encrypt
        self.acl_policy = acl_policy
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    @cached_property
    def http_hook(self) -> HttpHook:
        """Create and return an HttpHook."""
        return HttpHook(
            self.method,
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
            tcp_keep_alive=self.tcp_keep_alive,
            tcp_keep_alive_idle=self.tcp_keep_alive_idle,
            tcp_keep_alive_count=self.tcp_keep_alive_count,
            tcp_keep_alive_interval=self.tcp_keep_alive_interval,
        )

    @cached_property
    def s3_hook(self) -> S3Hook:
        """Create and return an S3Hook."""
        return S3Hook(
            aws_conn_id=self.aws_conn_id,
            verify=self.verify,
        )

    def execute(self, context: Context):
        self.log.info("Calling HTTP method")
        response = self.http_hook.run(self.endpoint, self.data, self.headers, self.extra_options)

        self.s3_hook.load_bytes(
            response.content,
            self.s3_key,
            self.s3_bucket,
            self.replace,
            self.encrypt,
            self.acl_policy,
        )
