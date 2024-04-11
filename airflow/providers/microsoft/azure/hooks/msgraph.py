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

from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlparse

import httpx
from azure.identity import ClientSecretCredential
from httpx import Timeout
from kiota_authentication_azure.azure_identity_authentication_provider import (
    AzureIdentityAuthenticationProvider,
)
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from msgraph_core import GraphClientFactory
from msgraph_core._enums import APIVersion, NationalClouds

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from kiota_abstractions.request_adapter import RequestAdapter

    from airflow.models import Connection


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

    @classmethod
    def to_msal_proxies(cls, authority: str | None, proxies: dict):
        if authority:
            no_proxies = proxies.get("no")
            if no_proxies:
                for url in no_proxies.split(","):
                    domain_name = urlparse(url).path.replace("*", "")
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
