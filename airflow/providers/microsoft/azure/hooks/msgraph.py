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

import json
from http import HTTPStatus
from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import quote, urljoin, urlparse

import httpx
from azure.identity import ClientSecretCredential
from httpx import Timeout
from kiota_abstractions.api_error import APIError
from kiota_abstractions.method import Method
from kiota_abstractions.request_information import RequestInformation
from kiota_abstractions.response_handler import ResponseHandler
from kiota_authentication_azure.azure_identity_authentication_provider import (
    AzureIdentityAuthenticationProvider,
)
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from kiota_http.middleware.options import ResponseHandlerOption
from msgraph_core import APIVersion, GraphClientFactory
from msgraph_core._enums import NationalClouds

from airflow.exceptions import AirflowBadRequest, AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from kiota_abstractions.request_adapter import RequestAdapter
    from kiota_abstractions.request_information import QueryParams
    from kiota_abstractions.response_handler import NativeResponseType
    from kiota_abstractions.serialization import ParsableFactory
    from kiota_http.httpx_request_adapter import ResponseType

    from airflow.models import Connection


class CallableResponseHandler(ResponseHandler):
    """
    CallableResponseHandler executes the passed callable_function with response as parameter.

    param callable_function: Function that is applied to the response.
    """

    def __init__(
        self,
        callable_function: Callable[[NativeResponseType, dict[str, ParsableFactory | None] | None], Any],
    ):
        self.callable_function = callable_function

    async def handle_response_async(
        self, response: NativeResponseType, error_map: dict[str, ParsableFactory | None] | None = None
    ) -> Any:
        """
        Invoke this callback method when a response is received.

        param response: The type of the native response object.
        param error_map: The error dict to use in case of a failed request.
        """
        value = self.callable_function(response, error_map)
        if response.status_code not in {200, 201, 202, 204, 302}:
            message = value or response.reason_phrase
            status_code = HTTPStatus(response.status_code)
            if status_code == HTTPStatus.BAD_REQUEST:
                raise AirflowBadRequest(message)
            elif status_code == HTTPStatus.NOT_FOUND:
                raise AirflowNotFoundException(message)
            raise AirflowException(message)
        return value


class KiotaRequestAdapterHook(BaseHook):
    """
    A Microsoft Graph API interaction hook, a Wrapper around KiotaRequestAdapter.

    https://github.com/microsoftgraph/msgraph-sdk-python-core

    :param conn_id: The HTTP Connection ID to run the trigger against.
    :param timeout: The HTTP timeout being used by the KiotaRequestAdapter (default is None).
        When no timeout is specified or set to None then no HTTP timeout is applied on each request.
    :param proxies: A Dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as "v1.0" or "beta".
    """

    DEFAULT_HEADERS = {"Accept": "application/json;q=1"}
    cached_request_adapters: dict[str, tuple[APIVersion, RequestAdapter]] = {}
    default_conn_name: str = "msgraph_default"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        timeout: float | None = None,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.timeout = timeout
        self.proxies = proxies
        self._api_version = self.resolve_api_version_from_value(api_version)

    @property
    def api_version(self) -> APIVersion:
        self.get_conn()  # Make sure config has been loaded through get_conn to have correct api version!
        return self._api_version

    @staticmethod
    def resolve_api_version_from_value(
        api_version: APIVersion | str, default: APIVersion | None = None
    ) -> APIVersion:
        if isinstance(api_version, APIVersion):
            return api_version
        return next(
            filter(lambda version: version.value == api_version, APIVersion),
            default,
        )

    def get_api_version(self, config: dict) -> APIVersion:
        if self._api_version is None:
            return self.resolve_api_version_from_value(
                api_version=config.get("api_version"), default=APIVersion.v1
            )
        return self._api_version

    @staticmethod
    def get_host(connection: Connection) -> str:
        if connection.schema and connection.host:
            return f"{connection.schema}://{connection.host}"
        return NationalClouds.Global.value

    @staticmethod
    def format_no_proxy_url(url: str) -> str:
        if "://" not in url:
            url = f"all://{url}"
        return url

    @classmethod
    def to_httpx_proxies(cls, proxies: dict) -> dict:
        proxies = proxies.copy()
        if proxies.get("http"):
            proxies["http://"] = proxies.pop("http")
        if proxies.get("https"):
            proxies["https://"] = proxies.pop("https")
        if proxies.get("no"):
            for url in proxies.pop("no", "").split(","):
                proxies[cls.format_no_proxy_url(url.strip())] = None
        return proxies

    def to_msal_proxies(self, authority: str | None, proxies: dict):
        self.log.info("authority: %s", authority)
        if authority:
            no_proxies = proxies.get("no")
            self.log.info("no_proxies: %s", no_proxies)
            if no_proxies:
                for url in no_proxies.split(","):
                    self.log.info("url: %s", url)
                    domain_name = urlparse(url).path.replace("*", "")
                    self.log.info("domain_name: %s", domain_name)
                    if authority.endswith(domain_name):
                        return None
        return proxies

    def get_conn(self) -> RequestAdapter:
        if not self.conn_id:
            raise AirflowException("Failed to create the KiotaRequestAdapterHook. No conn_id provided!")

        api_version, request_adapter = self.cached_request_adapters.get(self.conn_id, (None, None))

        if not request_adapter:
            connection = self.get_connection(conn_id=self.conn_id)
            client_id = connection.login
            client_secret = connection.password
            config = connection.extra_dejson if connection.extra else {}
            tenant_id = config.get("tenant_id")
            api_version = self.get_api_version(config)
            host = self.get_host(connection)
            base_url = config.get("base_url", urljoin(host, api_version.value))
            authority = config.get("authority")
            proxies = self.proxies or config.get("proxies", {})
            msal_proxies = self.to_msal_proxies(authority=authority, proxies=proxies)
            httpx_proxies = self.to_httpx_proxies(proxies=proxies)
            scopes = config.get("scopes", ["https://graph.microsoft.com/.default"])
            verify = config.get("verify", True)
            trust_env = config.get("trust_env", False)
            disable_instance_discovery = config.get("disable_instance_discovery", False)
            allowed_hosts = (config.get("allowed_hosts", authority) or "").split(",")

            self.log.info(
                "Creating Microsoft Graph SDK client %s for conn_id: %s",
                api_version.value,
                self.conn_id,
            )
            self.log.info("Host: %s", host)
            self.log.info("Base URL: %s", base_url)
            self.log.info("Tenant id: %s", tenant_id)
            self.log.info("Client id: %s", client_id)
            self.log.info("Client secret: %s", client_secret)
            self.log.info("API version: %s", api_version.value)
            self.log.info("Scope: %s", scopes)
            self.log.info("Verify: %s", verify)
            self.log.info("Timeout: %s", self.timeout)
            self.log.info("Trust env: %s", trust_env)
            self.log.info("Authority: %s", authority)
            self.log.info("Disable instance discovery: %s", disable_instance_discovery)
            self.log.info("Allowed hosts: %s", allowed_hosts)
            self.log.info("Proxies: %s", proxies)
            self.log.info("MSAL Proxies: %s", msal_proxies)
            self.log.info("HTTPX Proxies: %s", httpx_proxies)
            credentials = ClientSecretCredential(
                tenant_id=tenant_id,  # type: ignore
                client_id=connection.login,
                client_secret=connection.password,
                authority=authority,
                proxies=msal_proxies,
                disable_instance_discovery=disable_instance_discovery,
                connection_verify=verify,
            )
            http_client = GraphClientFactory.create_with_default_middleware(
                api_version=api_version,
                client=httpx.AsyncClient(
                    proxies=httpx_proxies,
                    timeout=Timeout(timeout=self.timeout),
                    verify=verify,
                    trust_env=trust_env,
                ),
                host=host,
            )
            auth_provider = AzureIdentityAuthenticationProvider(
                credentials=credentials,
                scopes=scopes,
                allowed_hosts=allowed_hosts,
            )
            request_adapter = HttpxRequestAdapter(
                authentication_provider=auth_provider,
                http_client=http_client,
                base_url=base_url,
            )
            self.cached_request_adapters[self.conn_id] = (api_version, request_adapter)
        self._api_version = api_version
        return request_adapter

    def test_connection(self):
        """Test HTTP Connection."""
        try:
            self.run()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)

    async def run(
        self,
        url: str = "",
        response_type: ResponseType | None = None,
        response_handler: Callable[
            [NativeResponseType, dict[str, ParsableFactory | None] | None], Any
        ] = lambda response, error_map: response.json(),
        path_parameters: dict[str, Any] | None = None,
        method: str = "GET",
        query_parameters: dict[str, QueryParams] | None = None,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | str | BytesIO | None = None,
    ):
        response = await self.get_conn().send_primitive_async(
            request_info=self.request_information(
                url=url,
                response_type=response_type,
                response_handler=response_handler,
                path_parameters=path_parameters,
                method=method,
                query_parameters=query_parameters,
                headers=headers,
                data=data,
            ),
            response_type=response_type,
            error_map=self.error_mapping(),
        )

        self.log.debug("response: %s", response)

        return response

    def request_information(
        self,
        url: str,
        response_type: ResponseType | None = None,
        response_handler: Callable[
            [NativeResponseType, dict[str, ParsableFactory | None] | None], Any
        ] = lambda response, error_map: response.json(),
        path_parameters: dict[str, Any] | None = None,
        method: str = "GET",
        query_parameters: dict[str, QueryParams] | None = None,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | str | BytesIO | None = None,
    ) -> RequestInformation:
        request_information = RequestInformation()
        request_information.path_parameters = path_parameters or {}
        request_information.http_method = Method(method.strip().upper())
        request_information.query_parameters = self.encoded_query_parameters(query_parameters)
        if url.startswith("http"):
            request_information.url = url
        elif request_information.query_parameters.keys():
            query = ",".join(request_information.query_parameters.keys())
            request_information.url_template = f"{{+baseurl}}/{self.normalize_url(url)}{{?{query}}}"
        else:
            request_information.url_template = f"{{+baseurl}}/{self.normalize_url(url)}"
        if not response_type:
            request_information.request_options[ResponseHandlerOption.get_key()] = ResponseHandlerOption(
                response_handler=CallableResponseHandler(response_handler)
            )
        headers = {**self.DEFAULT_HEADERS, **headers} if headers else self.DEFAULT_HEADERS
        for header_name, header_value in headers.items():
            request_information.headers.try_add(header_name=header_name, header_value=header_value)
        self.log.info("data: %s", data)
        if isinstance(data, BytesIO) or isinstance(data, bytes) or isinstance(data, str):
            request_information.content = data
        elif data:
            request_information.headers.try_add(
                header_name=RequestInformation.CONTENT_TYPE_HEADER, header_value="application/json"
            )
            request_information.content = json.dumps(data).encode("utf-8")
        return request_information

    @staticmethod
    def normalize_url(url: str) -> str | None:
        if url.startswith("/"):
            return url.replace("/", "", 1)
        return url

    @staticmethod
    def encoded_query_parameters(query_parameters) -> dict:
        if query_parameters:
            return {quote(key): value for key, value in query_parameters.items()}
        return {}

    @staticmethod
    def error_mapping() -> dict[str, ParsableFactory | None]:
        return {
            "4XX": APIError,
            "5XX": APIError,
        }
