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
from ast import literal_eval
from contextlib import suppress
from http import HTTPStatus
from io import BytesIO
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urljoin, urlparse

import httpx
from httpx import AsyncHTTPTransport, Timeout
from kiota_abstractions.api_error import APIError
from kiota_abstractions.method import Method
from kiota_abstractions.request_information import RequestInformation
from kiota_abstractions.response_handler import ResponseHandler
from kiota_abstractions.serialization import ParseNodeFactoryRegistry
from kiota_authentication_azure.azure_identity_authentication_provider import (
    AzureIdentityAuthenticationProvider,
)
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from kiota_http.middleware.options import ResponseHandlerOption
from kiota_serialization_json.json_parse_node_factory import JsonParseNodeFactory
from kiota_serialization_text.text_parse_node_factory import TextParseNodeFactory
from msgraph_core import APIVersion, GraphClientFactory
from msgraph_core._enums import NationalClouds

from airflow.exceptions import (
    AirflowBadRequest,
    AirflowConfigException,
    AirflowException,
    AirflowNotFoundException,
)
from airflow.hooks.base import BaseHook
from azure.identity import CertificateCredential, ClientSecretCredential

if TYPE_CHECKING:
    from kiota_abstractions.request_adapter import RequestAdapter
    from kiota_abstractions.request_information import QueryParams
    from kiota_abstractions.response_handler import NativeResponseType
    from kiota_abstractions.serialization import ParsableFactory
    from kiota_http.httpx_request_adapter import ResponseType

    from airflow.models import Connection
    from azure.identity._internal.client_credential_base import ClientCredentialBase


class DefaultResponseHandler(ResponseHandler):
    """DefaultResponseHandler returns JSON payload or content in bytes or response headers."""

    @staticmethod
    def get_value(response: NativeResponseType) -> Any:
        with suppress(JSONDecodeError):
            return response.json()
        content = response.content
        if not content:
            return {key: value for key, value in response.headers.items()}
        return content

    async def handle_response_async(
        self, response: NativeResponseType, error_map: dict[str, ParsableFactory | None] | None = None
    ) -> Any:
        """
        Invoke this callback method when a response is received.

        param response: The type of the native response object.
        param error_map: The error dict to use in case of a failed request.
        """
        value = self.get_value(response)
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
    :param host: The host to be used (default is "https://graph.microsoft.com").
    :param scopes: The scopes to be used (default is ["https://graph.microsoft.com/.default"]).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as "v1.0" or "beta".
    """

    DEFAULT_HEADERS = {"Accept": "application/json;q=1"}
    DEFAULT_SCOPE = "https://graph.microsoft.com/.default"
    cached_request_adapters: dict[str, tuple[APIVersion, RequestAdapter]] = {}
    conn_type: str = "msgraph"
    conn_name_attr: str = "conn_id"
    default_conn_name: str = "msgraph_default"
    hook_name: str = "Microsoft Graph API"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        timeout: float | None = None,
        proxies: dict | None = None,
        host: str = NationalClouds.Global.value,
        scopes: str | list[str] | None = None,
        api_version: APIVersion | str | None = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.timeout = timeout
        self.proxies = proxies
        self.host = host
        if isinstance(scopes, str):
            self.scopes = [scopes]
        else:
            self.scopes = scopes or [self.DEFAULT_SCOPE]
        self._api_version = self.resolve_api_version_from_value(api_version)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextAreaFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms.fields import BooleanField, StringField

        return {
            "tenant_id": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
            "api_version": StringField(
                lazy_gettext("API Version"), widget=BS3TextFieldWidget(), default=APIVersion.v1.value
            ),
            "authority": StringField(lazy_gettext("Authority"), widget=BS3TextFieldWidget()),
            "certificate_path": StringField(lazy_gettext("Certificate path"), widget=BS3TextFieldWidget()),
            "certificate_data": StringField(lazy_gettext("Certificate data"), widget=BS3TextFieldWidget()),
            "scopes": StringField(
                lazy_gettext("Scopes"),
                widget=BS3TextFieldWidget(),
                default=cls.DEFAULT_SCOPE,
            ),
            "disable_instance_discovery": BooleanField(
                lazy_gettext("Disable instance discovery"), default=False
            ),
            "allowed_hosts": StringField(lazy_gettext("Allowed hosts"), widget=BS3TextFieldWidget()),
            "proxies": StringField(lazy_gettext("Proxies"), widget=BS3TextAreaFieldWidget()),
            "verify": BooleanField(lazy_gettext("Verify"), default=True),
            "trust_env": BooleanField(lazy_gettext("Trust environment"), default=True),
            "base_url": StringField(lazy_gettext("Base URL"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["extra"],
            "relabeling": {
                "login": "Client ID",
                "password": "Client Secret",
            },
            "default_values": {
                "schema": "https",
                "host": NationalClouds.Global.value,
                "port": 443,
            },
        }

    @property
    def api_version(self) -> str | None:
        self.get_conn()  # Make sure config has been loaded through get_conn to have correct api version!
        return self._api_version

    @staticmethod
    def resolve_api_version_from_value(
        api_version: APIVersion | str, default: str | None = None
    ) -> str | None:
        if isinstance(api_version, APIVersion):
            return api_version.value
        return api_version or default

    def get_api_version(self, config: dict) -> str:
        return self._api_version or self.resolve_api_version_from_value(
            config.get("api_version"), APIVersion.v1.value
        )  # type: ignore

    def get_host(self, connection: Connection) -> str:
        if connection.schema and connection.host:
            return f"{connection.schema}://{connection.host}"
        return self.host

    @staticmethod
    def format_no_proxy_url(url: str) -> str:
        if "://" not in url:
            return f"all://{url}"
        return url

    @classmethod
    def to_httpx_proxies(cls, proxies: dict) -> dict:
        if proxies:
            proxies = proxies.copy()
            if proxies.get("http"):
                proxies["http://"] = AsyncHTTPTransport(proxy=proxies.pop("http"))
            if proxies.get("https"):
                proxies["https://"] = AsyncHTTPTransport(proxy=proxies.pop("https"))
            if proxies.get("no"):
                for url in proxies.pop("no", "").split(","):
                    proxies[cls.format_no_proxy_url(url.strip())] = None
        return proxies

    def to_msal_proxies(self, authority: str | None, proxies: dict) -> dict | None:
        self.log.debug("authority: %s", authority)
        if authority and proxies:
            no_proxies = proxies.get("no")
            self.log.debug("no_proxies: %s", no_proxies)
            if no_proxies:
                for url in no_proxies.split(","):
                    self.log.info("url: %s", url)
                    domain_name = urlparse(url).path.replace("*", "")
                    self.log.debug("domain_name: %s", domain_name)
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
            api_version = self.get_api_version(config)
            host = self.get_host(connection)
            base_url = config.get("base_url", urljoin(host, api_version))
            authority = config.get("authority")
            proxies = self.get_proxies(config)
            httpx_proxies = self.to_httpx_proxies(proxies=proxies)
            scopes = config.get("scopes", self.scopes)
            if isinstance(scopes, str):
                scopes = scopes.split(",")
            verify = config.get("verify", True)
            trust_env = config.get("trust_env", False)
            allowed_hosts = (config.get("allowed_hosts", authority) or "").split(",")

            self.log.info(
                "Creating Microsoft Graph SDK client %s for conn_id: %s",
                api_version,
                self.conn_id,
            )
            self.log.info("Host: %s", host)
            self.log.info("Base URL: %s", base_url)
            self.log.info("Client id: %s", client_id)
            self.log.info("Client secret: %s", client_secret)
            self.log.info("API version: %s", api_version)
            self.log.info("Scope: %s", scopes)
            self.log.info("Verify: %s", verify)
            self.log.info("Timeout: %s", self.timeout)
            self.log.info("Trust env: %s", trust_env)
            self.log.info("Authority: %s", authority)
            self.log.info("Allowed hosts: %s", allowed_hosts)
            self.log.info("Proxies: %s", proxies)
            self.log.info("HTTPX Proxies: %s", httpx_proxies)
            credentials = self.get_credentials(
                login=connection.login,
                password=connection.password,
                config=config,
                authority=authority,
                verify=verify,
                proxies=proxies,
            )
            http_client = GraphClientFactory.create_with_default_middleware(
                api_version=api_version,  # type: ignore
                client=httpx.AsyncClient(
                    mounts=httpx_proxies,
                    timeout=Timeout(timeout=self.timeout),
                    verify=verify,
                    trust_env=trust_env,
                ),
                host=host,  # type: ignore
            )
            auth_provider = AzureIdentityAuthenticationProvider(
                credentials=credentials,  # type: ignore
                scopes=scopes,
                allowed_hosts=allowed_hosts,
            )
            parse_node_factory = ParseNodeFactoryRegistry()
            parse_node_factory.CONTENT_TYPE_ASSOCIATED_FACTORIES["text/plain"] = TextParseNodeFactory()
            parse_node_factory.CONTENT_TYPE_ASSOCIATED_FACTORIES["application/json"] = JsonParseNodeFactory()
            request_adapter = HttpxRequestAdapter(
                authentication_provider=auth_provider,
                parse_node_factory=parse_node_factory,
                http_client=http_client,
                base_url=base_url,
            )
            self.cached_request_adapters[self.conn_id] = (api_version, request_adapter)
        self._api_version = api_version
        return request_adapter

    def get_proxies(self, config: dict) -> dict:
        proxies = self.proxies or config.get("proxies", {})
        if isinstance(proxies, str):
            # TODO: Once provider depends on Airflow 2.10 or higher code below won't be needed anymore as
            #       we could then use the get_extra_dejson method on the connection which deserializes
            #       nested json. Make sure to use connection.get_extra_dejson(nested=True) instead of
            #       connection.extra_dejson.
            with suppress(JSONDecodeError):
                proxies = json.loads(proxies)
            with suppress(Exception):
                proxies = literal_eval(proxies)
        if not isinstance(proxies, dict):
            raise AirflowConfigException(
                f"Proxies must be of type dict, got {type(proxies).__name__} instead!"
            )
        return proxies

    def get_credentials(
        self,
        login: str | None,
        password: str | None,
        config,
        authority: str | None,
        verify: bool,
        proxies: dict,
    ) -> ClientCredentialBase:
        tenant_id = config.get("tenant_id") or config.get("tenantId")
        certificate_path = config.get("certificate_path")
        certificate_data = config.get("certificate_data")
        disable_instance_discovery = config.get("disable_instance_discovery", False)
        msal_proxies = self.to_msal_proxies(authority=authority, proxies=proxies)
        self.log.info("Tenant id: %s", tenant_id)
        self.log.info("Certificate path: %s", certificate_path)
        self.log.info("Certificate data: %s", certificate_data is not None)
        self.log.info("Authority: %s", authority)
        self.log.info("Disable instance discovery: %s", disable_instance_discovery)
        self.log.info("MSAL Proxies: %s", msal_proxies)
        if certificate_path or certificate_data:
            return CertificateCredential(
                tenant_id=tenant_id,  # type: ignore
                client_id=login,  # type: ignore
                password=password,
                certificate_path=certificate_path,
                certificate_data=certificate_data.encode() if certificate_data else None,
                authority=authority,
                proxies=msal_proxies,
                disable_instance_discovery=disable_instance_discovery,
                connection_verify=verify,
            )
        return ClientSecretCredential(
            tenant_id=tenant_id,  # type: ignore
            client_id=login,  # type: ignore
            client_secret=password,  # type: ignore
            authority=authority,
            proxies=msal_proxies,
            disable_instance_discovery=disable_instance_discovery,
            connection_verify=verify,
        )

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
        path_parameters: dict[str, Any] | None = None,
        method: str = "GET",
        query_parameters: dict[str, QueryParams] | None = None,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | str | BytesIO | None = None,
    ):
        self.log.info("Executing url '%s' as '%s'", url, method)

        response = await self.get_conn().send_primitive_async(
            request_info=self.request_information(
                url=url,
                response_type=response_type,
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
                response_handler=DefaultResponseHandler()
            )
        headers = {**self.DEFAULT_HEADERS, **headers} if headers else self.DEFAULT_HEADERS
        for header_name, header_value in headers.items():
            request_information.headers.try_add(header_name=header_name, header_value=header_value)
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
