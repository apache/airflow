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
"""This module contains operator to move data from HTTP endpoint to GCS."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.version_compat import BaseOperator
from airflow.providers.http.hooks.http import HttpHook

if TYPE_CHECKING:
    from collections.abc import Sequence

    from requests.auth import AuthBase

    from airflow.providers.common.compat.sdk import Context


class HttpToGCSOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action and store the result in GCS.

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
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term credentials,
        or chained list of accounts required to get the access_token of the last account in the list,
        which will be impersonated in the request. If set as a string,
        the account must grant the originating account the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant Service Account Token Creator IAM role to the directly preceding identity,
        with first account from the list granting this role to the originating account.
    :param bucket_name: The bucket to upload to.
    :param object_name: The object name to set when uploading the file.
    :param mime_type: The file mime type set when uploading the file.
    :param gzip: Option to compress local file or file data for upload
    :param encoding: bytes encoding for file data if provided as string
    :param chunk_size: Blob chunk size.
    :param timeout: Request timeout in seconds.
    :param num_max_attempts: Number of attempts to try to upload the file.
    :param metadata: The metadata to be uploaded with the file.
    :param cache_contro: Cache-Control metadata field.
    :param user_project: The identifier of the Google Cloud project to bill for the request. Required for Requester Pays buckets.
    """

    template_fields: Sequence[str] = (
        "http_conn_id",
        "endpoint",
        "data",
        "headers",
        "gcp_conn_id",
        "bucket_name",
        "object_name",
    )
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
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        bucket_name: str,
        object_name: str,
        mime_type: str | None = None,
        gzip: bool = False,
        encoding: str | None = None,
        chunk_size: int | None = None,
        timeout: int | None = None,
        num_max_attempts: int = 3,
        metadata: dict | None = None,
        cache_control: str | None = None,
        user_project: str | None = None,
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
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.mime_type = mime_type
        self.gzip = gzip
        self.encoding = encoding
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.num_max_attempts = num_max_attempts
        self.metadata = metadata
        self.cache_control = cache_control
        self.user_project = user_project

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
    def gcs_hook(self) -> GCSHook:
        """Create and return an GCSHook."""
        return GCSHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

    def execute(self, context: Context):
        self.log.info("Calling HTTP method")
        response = self.http_hook.run(
            endpoint=self.endpoint, data=self.data, headers=self.headers, extra_options=self.extra_options
        )

        self.log.info("Uploading to GCS")
        self.gcs_hook.upload(
            data=response.content,
            bucket_name=self.bucket_name,
            object_name=self.object_name,
            mime_type=self.mime_type,
            gzip=self.gzip,
            encoding=self.encoding or response.encoding,
            chunk_size=self.chunk_size,
            timeout=self.timeout,
            num_max_attempts=self.num_max_attempts,
            metadata=self.metadata,
            cache_control=self.cache_control,
            user_project=self.user_project,
        )
