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
"""
Databricks hook.

This hook enable the submitting and running of jobs to the Databricks platform. Internally the
operators talk to the ``api/2.0/jobs/runs/submit``
`endpoint <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_.
"""

from __future__ import annotations

import base64
import copy
import json
import platform
import time
from asyncio.exceptions import TimeoutError
from functools import cached_property
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import aiohttp
import requests
from aiohttp.client_exceptions import ClientConnectorError
from requests import PreparedRequest, exceptions as requests_exceptions
from requests.auth import AuthBase, HTTPBasicAuth
from requests.exceptions import JSONDecodeError
from tenacity import (
    AsyncRetrying,
    RetryError,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from airflow import __version__
from airflow.exceptions import AirflowException, AirflowOptionalProviderFeatureException
from airflow.providers_manager import ProvidersManager

try:
    from airflow.sdk import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook as BaseHook  # type: ignore

if TYPE_CHECKING:
    from airflow.models import Connection

# https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token
AZURE_METADATA_SERVICE_TOKEN_URL = "http://169.254.169.254/metadata/identity/oauth2/token"
AZURE_METADATA_SERVICE_INSTANCE_URL = "http://169.254.169.254/metadata/instance"

TOKEN_REFRESH_LEAD_TIME = 120
AZURE_MANAGEMENT_ENDPOINT = "https://management.core.windows.net/"
DEFAULT_DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
OIDC_TOKEN_SERVICE_URL = "{}/oidc/v1/token"

DEFAULT_AZURE_CREDENTIAL_SETTING_KEY = "use_default_azure_credential"


class BaseDatabricksHook(BaseHook):
    """
    Base for interaction with Databricks.

    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    :param timeout_seconds: The amount of time in seconds the requests library
        will wait before timing-out.
    :param retry_limit: The number of times to retry the connection in case of
        service outages.
    :param retry_delay: The number of seconds to wait between retries (it
        might be a floating point number).
    :param retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param caller: The name of the operator that is calling the hook.
    """

    conn_name_attr: str = "databricks_conn_id"
    default_conn_name = "databricks_default"
    conn_type = "databricks"

    extra_parameters = [
        "token",
        "host",
        "use_azure_managed_identity",
        DEFAULT_AZURE_CREDENTIAL_SETTING_KEY,
        "azure_ad_endpoint",
        "azure_resource_id",
        "azure_tenant_id",
        "service_principal_oauth",
    ]

    def __init__(
        self,
        databricks_conn_id: str = default_conn_name,
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        retry_args: dict[Any, Any] | None = None,
        caller: str = "Unknown",
    ) -> None:
        super().__init__()
        self.databricks_conn_id = databricks_conn_id
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than or equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.oauth_tokens: dict[str, dict] = {}
        self.token_timeout_seconds = 10
        self.caller = caller
        self._metadata_cache: dict[str, Any] = {}
        self._metadata_expiry: float = 0
        self._metadata_ttl: int = 300
        self.jwt_tokens: dict[str, str] = {}

        def my_after_func(retry_state):
            self._log_request_error(retry_state.attempt_number, retry_state.outcome)

        if retry_args:
            self.retry_args = copy.copy(retry_args)
            self.retry_args["retry"] = retry_if_exception(self._retryable_error)
            self.retry_args["after"] = my_after_func
        else:
            self.retry_args = {
                "stop": stop_after_attempt(self.retry_limit),
                "wait": wait_exponential(min=self.retry_delay, max=(2**retry_limit)),
                "retry": retry_if_exception(self._retryable_error),
                "after": my_after_func,
            }

    @cached_property
    def databricks_conn(self) -> Connection:
        return self.get_connection(self.databricks_conn_id)  # type: ignore[return-value]

    def get_conn(self) -> Connection:
        return self.databricks_conn

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        return {}

    @cached_property
    def user_agent_header(self) -> dict[str, str]:
        return {"user-agent": self.user_agent_value}

    @cached_property
    def user_agent_value(self) -> str:
        manager = ProvidersManager()
        package_name = manager.hooks[BaseDatabricksHook.conn_type].package_name  # type: ignore[union-attr]
        provider = manager.providers[package_name]
        version = provider.version
        python_version = platform.python_version()
        system = platform.system().lower()
        ua_string = (
            f"databricks-airflow/{version} _/0.0.0 python/{python_version} os/{system} "
            f"airflow/{__version__} operator/{self.caller}"
        )
        return ua_string

    @cached_property
    def host(self) -> str | None:
        host = None
        if "host" in self.databricks_conn.extra_dejson:
            host = self._parse_host(self.databricks_conn.extra_dejson["host"])
        elif self.databricks_conn.host:
            host = self._parse_host(self.databricks_conn.host)
        return host

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *err):
        await self._session.close()
        self._session = None

    @staticmethod
    def _parse_host(host: str) -> str:
        """
        Parse host field data; this function is resistant to incorrect connection settings provided by users.

        For example -- when users supply ``https://xx.cloud.databricks.com`` as the
        host, we must strip out the protocol to get the host.::

            h = DatabricksHook()
            assert h._parse_host('https://xx.cloud.databricks.com') == \
                'xx.cloud.databricks.com'

        In the case where users supply the correct ``xx.cloud.databricks.com`` as the
        host, this function is a no-op.::

            assert h._parse_host('xx.cloud.databricks.com') == 'xx.cloud.databricks.com'

        """
        urlparse_host = urlsplit(host).hostname
        if urlparse_host:
            # In this case, host = https://xx.cloud.databricks.com
            return urlparse_host
        # In this case, host = xx.cloud.databricks.com
        return host

    def _get_connection_attr(self, attr_name: str) -> str:
        if not (attr := getattr(self.databricks_conn, attr_name)):
            raise ValueError(f"`{attr_name}` must be present in Connection")
        return attr

    def _get_retry_object(self) -> Retrying:
        """
        Instantiate a retry object.

        :return: instance of Retrying class
        """
        return Retrying(**self.retry_args)

    def _a_get_retry_object(self) -> AsyncRetrying:
        """
        Instantiate an async retry object.

        :return: instance of AsyncRetrying class
        """
        return AsyncRetrying(**self.retry_args)

    def _get_sp_token(self, resource: str) -> str:
        """Get Service Principal token."""
        sp_token = self.oauth_tokens.get(resource)
        if sp_token and self._is_oauth_token_valid(sp_token):
            return sp_token["access_token"]

        self.log.info("Existing Service Principal token is expired, or going to expire soon. Refreshing...")
        try:
            for attempt in self._get_retry_object():
                with attempt:
                    resp = requests.post(
                        resource,
                        auth=HTTPBasicAuth(self._get_connection_attr("login"), self.databricks_conn.password),
                        data="grant_type=client_credentials&scope=all-apis",
                        headers={
                            **self.user_agent_header,
                            "Content-Type": "application/x-www-form-urlencoded",
                        },
                        timeout=self.token_timeout_seconds,
                    )

                    resp.raise_for_status()
                    jsn = resp.json()
                    jsn["expires_on"] = int(time.time() + jsn["expires_in"])

                    self._is_oauth_token_valid(jsn)
                    self.oauth_tokens[resource] = jsn
                    break
        except RetryError:
            raise AirflowException(f"API requests to Databricks failed {self.retry_limit} times. Giving up.")
        except requests_exceptions.HTTPError as e:
            msg = f"Response: {e.response.content.decode()}, Status Code: {e.response.status_code}"
            raise AirflowException(msg)

        return jsn["access_token"]

    async def _a_get_sp_token(self, resource: str) -> str:
        """Async version of `_get_sp_token()`."""
        sp_token = self.oauth_tokens.get(resource)
        if sp_token and self._is_oauth_token_valid(sp_token):
            return sp_token["access_token"]

        self.log.info("Existing Service Principal token is expired, or going to expire soon. Refreshing...")
        try:
            async for attempt in self._a_get_retry_object():
                with attempt:
                    async with self._session.post(
                        resource,
                        auth=aiohttp.BasicAuth(
                            self._get_connection_attr("login"), self.databricks_conn.password
                        ),
                        data="grant_type=client_credentials&scope=all-apis",
                        headers={
                            **self.user_agent_header,
                            "Content-Type": "application/x-www-form-urlencoded",
                        },
                        timeout=self.token_timeout_seconds,
                    ) as resp:
                        resp.raise_for_status()
                        jsn = await resp.json()
                        jsn["expires_on"] = int(time.time() + jsn["expires_in"])

                    self._is_oauth_token_valid(jsn)
                    self.oauth_tokens[resource] = jsn
                    break
        except RetryError:
            raise AirflowException(f"API requests to Databricks failed {self.retry_limit} times. Giving up.")
        except requests_exceptions.HTTPError as e:
            msg = f"Response: {e.response.content.decode()}, Status Code: {e.response.status_code}"
            raise AirflowException(msg)

        return jsn["access_token"]

    def _get_aad_token(self, resource: str) -> str:
        """
        Get AAD token for given resource.

        Supports managed identity or service principal auth.
        :param resource: resource to issue token to
        :return: AAD token, or raise an exception
        """
        aad_token = self.oauth_tokens.get(resource)
        if aad_token and self._is_oauth_token_valid(aad_token):
            return aad_token["access_token"]

        self.log.info("Existing AAD token is expired, or going to expire soon. Refreshing...")
        try:
            from azure.identity import ClientSecretCredential, ManagedIdentityCredential

            for attempt in self._get_retry_object():
                with attempt:
                    if self.databricks_conn.extra_dejson.get("use_azure_managed_identity", False):
                        token = ManagedIdentityCredential().get_token(f"{resource}/.default")
                    else:
                        credential = ClientSecretCredential(
                            client_id=self._get_connection_attr("login"),
                            client_secret=self.databricks_conn.password,
                            tenant_id=self.databricks_conn.extra_dejson["azure_tenant_id"],
                        )
                        token = credential.get_token(f"{resource}/.default")
                    jsn = {
                        "access_token": token.token,
                        "token_type": "Bearer",
                        "expires_on": token.expires_on,
                    }
                    self._is_oauth_token_valid(jsn)
                    self.oauth_tokens[resource] = jsn
                    break
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)
        except RetryError:
            raise AirflowException(f"API requests to Azure failed {self.retry_limit} times. Giving up.")
        except requests_exceptions.HTTPError as e:
            msg = f"Response: {e.response.content.decode()}, Status Code: {e.response.status_code}"
            raise AirflowException(msg)

        return jsn["access_token"]

    async def _a_get_aad_token(self, resource: str) -> str:
        """
        Async version of `_get_aad_token()`.

        :param resource: resource to issue token to
        :return: AAD token, or raise an exception
        """
        aad_token = self.oauth_tokens.get(resource)
        if aad_token and self._is_oauth_token_valid(aad_token):
            return aad_token["access_token"]

        self.log.info("Existing AAD token is expired, or going to expire soon. Refreshing...")
        try:
            from azure.identity.aio import (
                ClientSecretCredential as AsyncClientSecretCredential,
                ManagedIdentityCredential as AsyncManagedIdentityCredential,
            )

            async for attempt in self._a_get_retry_object():
                with attempt:
                    if self.databricks_conn.extra_dejson.get("use_azure_managed_identity", False):
                        async with AsyncManagedIdentityCredential() as credential:
                            token = await credential.get_token(f"{resource}/.default")
                    else:
                        async with AsyncClientSecretCredential(
                            client_id=self._get_connection_attr("login"),
                            client_secret=self.databricks_conn.password,
                            tenant_id=self.databricks_conn.extra_dejson["azure_tenant_id"],
                        ) as credential:
                            token = await credential.get_token(f"{resource}/.default")
                    jsn = {
                        "access_token": token.token,
                        "token_type": "Bearer",
                        "expires_on": token.expires_on,
                    }
                    self._is_oauth_token_valid(jsn)
                    self.oauth_tokens[resource] = jsn
                    break
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)
        except RetryError:
            raise AirflowException(f"API requests to Azure failed {self.retry_limit} times. Giving up.")
        except aiohttp.ClientResponseError as err:
            raise AirflowException(f"Response: {err.message}, Status Code: {err.status}")

        return jsn["access_token"]

    def _get_aad_token_for_default_az_credential(self, resource: str) -> str:
        """
        Get AAD token for given resource for workload identity.

        Supports managed identity or service principal auth.
        :param resource: resource to issue token to
        :return: AAD token, or raise an exception
        """
        aad_token = self.oauth_tokens.get(resource)
        if aad_token and self._is_oauth_token_valid(aad_token):
            return aad_token["access_token"]

        self.log.info("Existing AAD token is expired, or going to expire soon. Refreshing...")
        try:
            from azure.identity import DefaultAzureCredential

            for attempt in self._get_retry_object():
                with attempt:
                    # This only works in an Azure Kubernetes Service Cluster given the following environment variables:
                    # AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_FEDERATED_TOKEN_FILE
                    #
                    # While there is a WorkloadIdentityCredential class, the below class is advised by Microsoft
                    # https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview
                    token = DefaultAzureCredential().get_token(f"{resource}/.default")

                    jsn = {
                        "access_token": token.token,
                        "token_type": "Bearer",
                        "expires_on": token.expires_on,
                    }
                    self._is_oauth_token_valid(jsn)
                    self.oauth_tokens[resource] = jsn
                    break
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)
        except RetryError:
            raise AirflowException(f"API requests to Azure failed {self.retry_limit} times. Giving up.")
        except requests_exceptions.HTTPError as e:
            msg = f"Response: {e.response.content.decode()}, Status Code: {e.response.status_code}"
            raise AirflowException(msg)

        return token.token

    async def _a_get_aad_token_for_default_az_credential(self, resource: str) -> str:
        """
        Get AAD token for given resource for workload identity.

        Supports managed identity or service principal auth.
        :param resource: resource to issue token to
        :return: AAD token, or raise an exception
        """
        aad_token = self.oauth_tokens.get(resource)
        if aad_token and self._is_oauth_token_valid(aad_token):
            return aad_token["access_token"]

        self.log.info("Existing AAD token is expired, or going to expire soon. Refreshing...")
        try:
            from azure.identity.aio import (
                DefaultAzureCredential as AsyncDefaultAzureCredential,
            )

            for attempt in self._get_retry_object():
                with attempt:
                    # This only works in an Azure Kubernetes Service Cluster given the following environment variables:
                    # AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_FEDERATED_TOKEN_FILE
                    #
                    # While there is a WorkloadIdentityCredential class, the below class is advised by Microsoft
                    # https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview
                    token = await AsyncDefaultAzureCredential().get_token(f"{resource}/.default")

                    jsn = {
                        "access_token": token.token,
                        "token_type": "Bearer",
                        "expires_on": token.expires_on,
                    }
                    self._is_oauth_token_valid(jsn)
                    self.oauth_tokens[resource] = jsn
                    break
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)
        except RetryError:
            raise AirflowException(f"API requests to Azure failed {self.retry_limit} times. Giving up.")
        except requests_exceptions.HTTPError as e:
            msg = f"Response: {e.response.content.decode()}, Status Code: {e.response.status_code}"
            raise AirflowException(msg)

        return token.token

    def _get_aad_headers(self) -> dict:
        """
        Fill AAD headers if necessary (SPN is outside of the workspace).

        :return: dictionary with filled AAD headers
        """
        headers = {}
        if "azure_resource_id" in self.databricks_conn.extra_dejson:
            mgmt_token = self._get_aad_token(AZURE_MANAGEMENT_ENDPOINT)
            headers["X-Databricks-Azure-Workspace-Resource-Id"] = self.databricks_conn.extra_dejson[
                "azure_resource_id"
            ]
            headers["X-Databricks-Azure-SP-Management-Token"] = mgmt_token
        return headers

    async def _a_get_aad_headers(self) -> dict:
        """
        Async version of `_get_aad_headers()`.

        :return: dictionary with filled AAD headers
        """
        headers = {}
        if "azure_resource_id" in self.databricks_conn.extra_dejson:
            mgmt_token = await self._a_get_aad_token(AZURE_MANAGEMENT_ENDPOINT)
            headers["X-Databricks-Azure-Workspace-Resource-Id"] = self.databricks_conn.extra_dejson[
                "azure_resource_id"
            ]
            headers["X-Databricks-Azure-SP-Management-Token"] = mgmt_token
        return headers

    @staticmethod
    def _is_oauth_token_valid(token: dict, time_key="expires_on") -> bool:
        """
        Check if an OAuth token is valid and hasn't expired yet.

        :param sp_token: dict with properties of OAuth token
        :param time_key: name of the key that holds the time of expiration
        :return: true if token is valid, false otherwise
        """
        if "access_token" not in token or token.get("token_type", "") != "Bearer" or time_key not in token:
            raise AirflowException(f"Can't get necessary data from OAuth token: {token}")

        return int(token[time_key]) > (int(time.time()) + TOKEN_REFRESH_LEAD_TIME)

    def _check_azure_metadata_service(self) -> None:
        """
        Check for Azure Metadata Service (with caching).

        https://docs.microsoft.com/en-us/azure/virtual-machines/linux/instance-metadata-service
        """
        if self._metadata_cache and time.time() < self._metadata_expiry:
            return
        try:
            for attempt in self._get_retry_object():
                with attempt:
                    response = requests.get(
                        AZURE_METADATA_SERVICE_INSTANCE_URL,
                        params={"api-version": "2021-02-01"},
                        headers={"Metadata": "true"},
                        timeout=2,
                    )
                    response.raise_for_status()
                    response_json = response.json()

                    self._validate_azure_metadata_service(response_json)
                    self._metadata_cache = response_json
                    self._metadata_expiry = time.time() + self._metadata_ttl
                    break
        except RetryError:
            raise ConnectionError(f"Failed to reach Azure Metadata Service after {self.retry_limit} retries.")
        except (requests_exceptions.RequestException, ValueError) as e:
            raise ConnectionError(f"Can't reach Azure Metadata Service: {e}")

    async def _a_check_azure_metadata_service(self):
        """Async version of `_check_azure_metadata_service()`."""
        if self._metadata_cache and time.time() < self._metadata_expiry:
            return
        try:
            async for attempt in self._a_get_retry_object():
                with attempt:
                    async with self._session.get(
                        url=AZURE_METADATA_SERVICE_INSTANCE_URL,
                        params={"api-version": "2021-02-01"},
                        headers={"Metadata": "true"},
                        timeout=2,
                    ) as resp:
                        resp.raise_for_status()
                        response_json = await resp.json()
                    self._validate_azure_metadata_service(response_json)
                    self._metadata_cache = response_json
                    self._metadata_expiry = time.time() + self._metadata_ttl
                    break
        except RetryError:
            raise ConnectionError(f"Failed to reach Azure Metadata Service after {self.retry_limit} retries.")
        except (aiohttp.ClientError, ValueError) as e:
            raise ConnectionError(f"Can't reach Azure Metadata Service: {e}")

    def _validate_azure_metadata_service(self, response_json: dict) -> None:
        if "compute" not in response_json or "azEnvironment" not in response_json["compute"]:
            raise ValueError(
                f"Was able to fetch some metadata, but it doesn't look like Azure Metadata: {response_json}"
            )

    def _get_token(self, raise_error: bool = False) -> str | None:
        if "token" in self.databricks_conn.extra_dejson:
            self.log.info(
                "Using token auth. For security reasons, please set token in Password field instead of extra"
            )
            return self.databricks_conn.extra_dejson["token"]
        if not self.databricks_conn.login and self.databricks_conn.password:
            self.log.debug("Using token auth.")
            return self.databricks_conn.password
        if "azure_tenant_id" in self.databricks_conn.extra_dejson:
            if self.databricks_conn.login == "" or self.databricks_conn.password == "":
                raise AirflowException("Azure SPN credentials aren't provided")
            self.log.debug("Using AAD Token for SPN.")
            return self._get_aad_token(DEFAULT_DATABRICKS_SCOPE)
        if self.databricks_conn.extra_dejson.get("use_azure_managed_identity", False):
            self.log.debug("Using AAD Token for managed identity.")
            self._check_azure_metadata_service()
            return self._get_aad_token(DEFAULT_DATABRICKS_SCOPE)
        if self.databricks_conn.extra_dejson.get(DEFAULT_AZURE_CREDENTIAL_SETTING_KEY, False):
            self.log.debug("Using default Azure Credential authentication.")
            return self._get_aad_token_for_default_az_credential(DEFAULT_DATABRICKS_SCOPE)
        if self.databricks_conn.extra_dejson.get("service_principal_oauth", False):
            if self.databricks_conn.login == "" or self.databricks_conn.password == "":
                raise AirflowException("Service Principal credentials aren't provided")
            self.log.debug("Using Service Principal Token.")
            return self._get_sp_token(OIDC_TOKEN_SERVICE_URL.format(self.databricks_conn.host))
        if raise_error:
            raise AirflowException("Token authentication isn't configured")

        return None

    async def _a_get_token(self, raise_error: bool = False) -> str | None:
        if "token" in self.databricks_conn.extra_dejson:
            self.log.info(
                "Using token auth. For security reasons, please set token in Password field instead of extra"
            )
            return self.databricks_conn.extra_dejson["token"]
        if not self.databricks_conn.login and self.databricks_conn.password:
            self.log.debug("Using token auth.")
            return self.databricks_conn.password
        if "azure_tenant_id" in self.databricks_conn.extra_dejson:
            if self.databricks_conn.login == "" or self.databricks_conn.password == "":
                raise AirflowException("Azure SPN credentials aren't provided")
            self.log.debug("Using AAD Token for SPN.")
            return await self._a_get_aad_token(DEFAULT_DATABRICKS_SCOPE)
        if self.databricks_conn.extra_dejson.get("use_azure_managed_identity", False):
            self.log.debug("Using AAD Token for managed identity.")
            await self._a_check_azure_metadata_service()
            return await self._a_get_aad_token(DEFAULT_DATABRICKS_SCOPE)
        if self.databricks_conn.extra_dejson.get(DEFAULT_AZURE_CREDENTIAL_SETTING_KEY, False):
            self.log.debug("Using AzureDefaultCredential for authentication.")

            return await self._a_get_aad_token_for_default_az_credential(DEFAULT_DATABRICKS_SCOPE)
        if self.databricks_conn.extra_dejson.get("service_principal_oauth", False):
            if self.databricks_conn.login == "" or self.databricks_conn.password == "":
                raise AirflowException("Service Principal credentials aren't provided")
            self.log.debug("Using Service Principal Token.")
            return await self._a_get_sp_token(OIDC_TOKEN_SERVICE_URL.format(self.databricks_conn.host))
        if raise_error:
            raise AirflowException("Token authentication isn't configured")

        return None

    def _log_request_error(self, attempt_num: int, error: str) -> None:
        self.log.error("Attempt %s API Request to Databricks failed with reason: %s", attempt_num, error)

    def _endpoint_url(self, endpoint):
        port = f":{self.databricks_conn.port}" if self.databricks_conn.port else ""
        schema = self.databricks_conn.schema or "https"
        return f"{schema}://{self.host}{port}/{endpoint}"

    def _do_api_call(
        self,
        endpoint_info: tuple[str, str],
        json: dict[str, Any] | None = None,
        wrap_http_errors: bool = True,
    ):
        """
        Perform an API call with retries.

        :param endpoint_info: Tuple of method and endpoint
        :param json: Parameters for this API call.
        :return: If the api call returns a OK status code,
            this function returns the response in JSON. Otherwise,
            we throw an AirflowException.
        """
        method, endpoint = endpoint_info

        # Automatically prepend 'api/' prefix to all endpoint paths
        full_endpoint = f"api/{endpoint}"
        url = self._endpoint_url(full_endpoint)

        aad_headers = self._get_aad_headers()
        headers = {**self.user_agent_header, **aad_headers}

        auth: AuthBase
        token = self._get_token()
        if token:
            auth = _TokenAuth(token)
        else:
            self.log.info("Using basic auth.")
            auth = HTTPBasicAuth(self._get_connection_attr("login"), self.databricks_conn.password)

        request_func: Any
        if method == "GET":
            request_func = requests.get
        elif method == "POST":
            request_func = requests.post
        elif method == "PATCH":
            request_func = requests.patch
        elif method == "DELETE":
            request_func = requests.delete
        else:
            raise AirflowException("Unexpected HTTP Method: " + method)

        try:
            for attempt in self._get_retry_object():
                with attempt:
                    self.log.debug(
                        "Initiating %s request to %s with payload: %s, headers: %s",
                        method,
                        url,
                        json,
                        headers,
                    )
                    response = request_func(
                        url,
                        json=json if method in ("POST", "PATCH") else None,
                        params=json if method == "GET" else None,
                        auth=auth,
                        headers=headers,
                        timeout=self.timeout_seconds,
                    )
                    self.log.debug("Response Status Code: %s", response.status_code)
                    self.log.debug("Response text: %s", response.text)
                    response.raise_for_status()
                    return response.json()
        except RetryError:
            raise AirflowException(f"API requests to Databricks failed {self.retry_limit} times. Giving up.")
        except requests_exceptions.HTTPError as e:
            if wrap_http_errors:
                msg = f"Response: {e.response.content.decode()}, Status Code: {e.response.status_code}"
                raise AirflowException(msg)
            raise

    async def _a_do_api_call(self, endpoint_info: tuple[str, str], json: dict[str, Any] | None = None):
        """
        Async version of `_do_api_call()`.

        :param endpoint_info: Tuple of method and endpoint
        :param json: Parameters for this API call.
        :return: If the api call returns a OK status code,
            this function returns the response in JSON. Otherwise, throw an AirflowException.
        """
        method, endpoint = endpoint_info

        full_endpoint = f"api/{endpoint}"
        url = self._endpoint_url(full_endpoint)

        aad_headers = await self._a_get_aad_headers()
        headers = {**self.user_agent_header, **aad_headers}

        auth: aiohttp.BasicAuth
        token = await self._a_get_token()
        if token:
            auth = BearerAuth(token)
        else:
            self.log.info("Using basic auth.")
            auth = aiohttp.BasicAuth(self._get_connection_attr("login"), self.databricks_conn.password)

        request_func: Any
        if method == "GET":
            request_func = self._session.get
        elif method == "POST":
            request_func = self._session.post
        elif method == "PATCH":
            request_func = self._session.patch
        else:
            raise AirflowException("Unexpected HTTP Method: " + method)
        try:
            async for attempt in self._a_get_retry_object():
                with attempt:
                    self.log.debug(
                        "Initiating %s request to %s with payload: %s, headers: %s",
                        method,
                        url,
                        json,
                        headers,
                    )
                    async with request_func(
                        url,
                        json=json,
                        auth=auth,
                        headers={**headers, **self.user_agent_header},
                        timeout=self.timeout_seconds,
                    ) as response:
                        self.log.debug("Response Status Code: %s", response.status)
                        self.log.debug("Response text: %s", response.text)
                        response.raise_for_status()
                        return await response.json()
        except RetryError:
            raise AirflowException(f"API requests to Databricks failed {self.retry_limit} times. Giving up.")
        except aiohttp.ClientResponseError as err:
            raise AirflowException(f"Response: {err.message}, Status Code: {err.status}")

    @staticmethod
    def _get_error_code(exception: BaseException) -> str:
        if isinstance(exception, requests_exceptions.HTTPError):
            try:
                jsn = exception.response.json()
                return jsn.get("error_code", "")
            except JSONDecodeError:
                pass

        return ""

    @staticmethod
    def _retryable_error(exception: BaseException) -> bool:
        if isinstance(exception, requests_exceptions.RequestException):
            if isinstance(exception, (requests_exceptions.ConnectionError, requests_exceptions.Timeout)) or (
                exception.response is not None
                and (
                    exception.response.status_code >= 500
                    or exception.response.status_code == 429
                    or (
                        exception.response.status_code == 400
                        and BaseDatabricksHook._get_error_code(exception) == "COULD_NOT_ACQUIRE_LOCK"
                    )
                )
            ):
                return True

        if isinstance(exception, aiohttp.ClientResponseError):
            if exception.status >= 500 or exception.status == 429:
                return True

        if isinstance(exception, (ClientConnectorError, TimeoutError)):
            return True

        return False


class _TokenAuth(AuthBase):
    """
    Helper class for requests Auth field.

    AuthBase requires you to implement the ``__call__``
    magic function.
    """

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers["Authorization"] = "Bearer " + self.token
        return r


class BearerAuth(aiohttp.BasicAuth):
    """aiohttp only ships BasicAuth, for Bearer auth we need a subclass of BasicAuth."""

    def __new__(cls, token: str) -> BearerAuth:
        return super().__new__(cls, token)  # type: ignore

    def __init__(self, token: str) -> None:
        self.token = token

    def encode(self) -> str:
        return f"Bearer {self.token}"
