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
import copy
import sys
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests import PreparedRequest, exceptions as requests_exceptions
from requests.auth import AuthBase, HTTPBasicAuth
from requests.exceptions import JSONDecodeError
from tenacity import RetryError, Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

USER_AGENT_HEADER = {'user-agent': f'airflow-{__version__}'}

# https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token#--get-an-azure-active-directory-access-token
# https://docs.microsoft.com/en-us/graph/deployments#app-registration-and-token-service-root-endpoints
AZURE_DEFAULT_AD_ENDPOINT = "https://login.microsoftonline.com"
AZURE_TOKEN_SERVICE_URL = "{}/{}/oauth2/token"
# https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token
AZURE_METADATA_SERVICE_TOKEN_URL = "http://169.254.169.254/metadata/identity/oauth2/token"
AZURE_METADATA_SERVICE_INSTANCE_URL = "http://169.254.169.254/metadata/instance"

TOKEN_REFRESH_LEAD_TIME = 120
AZURE_MANAGEMENT_ENDPOINT = "https://management.core.windows.net/"
DEFAULT_DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"


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
    """

    conn_name_attr = 'databricks_conn_id'
    default_conn_name = 'databricks_default'
    conn_type = 'databricks'

    extra_parameters = [
        'token',
        'host',
        'use_azure_managed_identity',
        'azure_ad_endpoint',
        'azure_resource_id',
        'azure_tenant_id',
    ]

    def __init__(
        self,
        databricks_conn_id: str = default_conn_name,
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        retry_args: Optional[Dict[Any, Any]] = None,
    ) -> None:
        super().__init__()
        self.databricks_conn_id = databricks_conn_id
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError('Retry limit must be greater than or equal to 1')
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.aad_tokens: Dict[str, dict] = {}
        self.aad_timeout_seconds = 10

        def my_after_func(retry_state):
            self._log_request_error(retry_state.attempt_number, retry_state.outcome)

        if retry_args:
            self.retry_args = copy.copy(retry_args)
            self.retry_args['retry'] = retry_if_exception(self._retryable_error)
            self.retry_args['after'] = my_after_func
        else:
            self.retry_args = dict(
                stop=stop_after_attempt(self.retry_limit),
                wait=wait_exponential(min=self.retry_delay, max=(2**retry_limit)),
                retry=retry_if_exception(self._retryable_error),
                after=my_after_func,
            )

    @cached_property
    def databricks_conn(self) -> Connection:
        return self.get_connection(self.databricks_conn_id)

    def get_conn(self) -> Connection:
        return self.databricks_conn

    @cached_property
    def host(self) -> str:
        if 'host' in self.databricks_conn.extra_dejson:
            host = self._parse_host(self.databricks_conn.extra_dejson['host'])
        else:
            host = self._parse_host(self.databricks_conn.host)

        return host

    @staticmethod
    def _parse_host(host: str) -> str:
        """
        The purpose of this function is to be robust to improper connections
        settings provided by users, specifically in the host field.

        For example -- when users supply ``https://xx.cloud.databricks.com`` as the
        host, we must strip out the protocol to get the host.::

            h = DatabricksHook()
            assert h._parse_host('https://xx.cloud.databricks.com') == \
                'xx.cloud.databricks.com'

        In the case where users supply the correct ``xx.cloud.databricks.com`` as the
        host, this function is a no-op.::

            assert h._parse_host('xx.cloud.databricks.com') == 'xx.cloud.databricks.com'

        """
        urlparse_host = urlparse(host).hostname
        if urlparse_host:
            # In this case, host = https://xx.cloud.databricks.com
            return urlparse_host
        else:
            # In this case, host = xx.cloud.databricks.com
            return host

    def _get_retry_object(self) -> Retrying:
        """
        Instantiates a retry object
        :return: instance of Retrying class
        """
        return Retrying(**self.retry_args)

    def _get_aad_token(self, resource: str) -> str:
        """
        Function to get AAD token for given resource. Supports managed identity or service principal auth
        :param resource: resource to issue token to
        :return: AAD token, or raise an exception
        """
        aad_token = self.aad_tokens.get(resource)
        if aad_token and self._is_aad_token_valid(aad_token):
            return aad_token['token']

        self.log.info('Existing AAD token is expired, or going to expire soon. Refreshing...')
        try:
            for attempt in self._get_retry_object():
                with attempt:
                    if self.databricks_conn.extra_dejson.get('use_azure_managed_identity', False):
                        params = {
                            "api-version": "2018-02-01",
                            "resource": resource,
                        }
                        resp = requests.get(
                            AZURE_METADATA_SERVICE_TOKEN_URL,
                            params=params,
                            headers={**USER_AGENT_HEADER, "Metadata": "true"},
                            timeout=self.aad_timeout_seconds,
                        )
                    else:
                        tenant_id = self.databricks_conn.extra_dejson['azure_tenant_id']
                        data = {
                            "grant_type": "client_credentials",
                            "client_id": self.databricks_conn.login,
                            "resource": resource,
                            "client_secret": self.databricks_conn.password,
                        }
                        azure_ad_endpoint = self.databricks_conn.extra_dejson.get(
                            "azure_ad_endpoint", AZURE_DEFAULT_AD_ENDPOINT
                        )
                        resp = requests.post(
                            AZURE_TOKEN_SERVICE_URL.format(azure_ad_endpoint, tenant_id),
                            data=data,
                            headers={
                                **USER_AGENT_HEADER,
                                'Content-Type': 'application/x-www-form-urlencoded',
                            },
                            timeout=self.aad_timeout_seconds,
                        )

                    resp.raise_for_status()
                    jsn = resp.json()
                    if (
                        'access_token' not in jsn
                        or jsn.get('token_type') != 'Bearer'
                        or 'expires_on' not in jsn
                    ):
                        raise AirflowException(f"Can't get necessary data from AAD token: {jsn}")

                    token = jsn['access_token']
                    self.aad_tokens[resource] = {'token': token, 'expires_on': int(jsn["expires_on"])}
                    break
        except RetryError:
            raise AirflowException(f'API requests to Azure failed {self.retry_limit} times. Giving up.')
        except requests_exceptions.HTTPError as e:
            raise AirflowException(f'Response: {e.response.content}, Status Code: {e.response.status_code}')

        return token

    def _get_aad_headers(self) -> dict:
        """
        Fills AAD headers if necessary (SPN is outside of the workspace)
        :return: dictionary with filled AAD headers
        """
        headers = {}
        if 'azure_resource_id' in self.databricks_conn.extra_dejson:
            mgmt_token = self._get_aad_token(AZURE_MANAGEMENT_ENDPOINT)
            headers['X-Databricks-Azure-Workspace-Resource-Id'] = self.databricks_conn.extra_dejson[
                'azure_resource_id'
            ]
            headers['X-Databricks-Azure-SP-Management-Token'] = mgmt_token
        return headers

    @staticmethod
    def _is_aad_token_valid(aad_token: dict) -> bool:
        """
        Utility function to check AAD token hasn't expired yet
        :param aad_token: dict with properties of AAD token
        :return: true if token is valid, false otherwise
        :rtype: bool
        """
        now = int(time.time())
        if aad_token['expires_on'] > (now + TOKEN_REFRESH_LEAD_TIME):
            return True
        return False

    @staticmethod
    def _check_azure_metadata_service() -> None:
        """
        Check for Azure Metadata Service
        https://docs.microsoft.com/en-us/azure/virtual-machines/linux/instance-metadata-service
        """
        try:
            jsn = requests.get(
                AZURE_METADATA_SERVICE_INSTANCE_URL,
                params={"api-version": "2021-02-01"},
                headers={"Metadata": "true"},
                timeout=2,
            ).json()
            if 'compute' not in jsn or 'azEnvironment' not in jsn['compute']:
                raise AirflowException(
                    f"Was able to fetch some metadata, but it doesn't look like Azure Metadata: {jsn}"
                )
        except (requests_exceptions.RequestException, ValueError) as e:
            raise AirflowException(f"Can't reach Azure Metadata Service: {e}")

    def _get_token(self, raise_error: bool = False) -> Optional[str]:
        if 'token' in self.databricks_conn.extra_dejson:
            self.log.info(
                'Using token auth. For security reasons, please set token in Password field instead of extra'
            )
            return self.databricks_conn.extra_dejson['token']
        elif not self.databricks_conn.login and self.databricks_conn.password:
            self.log.info('Using token auth.')
            return self.databricks_conn.password
        elif 'azure_tenant_id' in self.databricks_conn.extra_dejson:
            if self.databricks_conn.login == "" or self.databricks_conn.password == "":
                raise AirflowException("Azure SPN credentials aren't provided")
            self.log.info('Using AAD Token for SPN.')
            return self._get_aad_token(DEFAULT_DATABRICKS_SCOPE)
        elif self.databricks_conn.extra_dejson.get('use_azure_managed_identity', False):
            self.log.info('Using AAD Token for managed identity.')
            self._check_azure_metadata_service()
            return self._get_aad_token(DEFAULT_DATABRICKS_SCOPE)
        elif raise_error:
            raise AirflowException('Token authentication isn\'t configured')

        return None

    def _log_request_error(self, attempt_num: int, error: str) -> None:
        self.log.error('Attempt %s API Request to Databricks failed with reason: %s', attempt_num, error)

    def _do_api_call(
        self,
        endpoint_info: Tuple[str, str],
        json: Optional[Dict[str, Any]] = None,
        wrap_http_errors: bool = True,
    ):
        """
        Utility function to perform an API call with retries

        :param endpoint_info: Tuple of method and endpoint
        :param json: Parameters for this API call.
        :return: If the api call returns a OK status code,
            this function returns the response in JSON. Otherwise,
            we throw an AirflowException.
        :rtype: dict
        """
        method, endpoint = endpoint_info

        # TODO: get rid of explicit 'api/' in the endpoint specification
        url = f'https://{self.host}/{endpoint}'

        aad_headers = self._get_aad_headers()
        headers = {**USER_AGENT_HEADER.copy(), **aad_headers}

        auth: AuthBase
        token = self._get_token()
        if token:
            auth = _TokenAuth(token)
        else:
            self.log.info('Using basic auth.')
            auth = HTTPBasicAuth(self.databricks_conn.login, self.databricks_conn.password)

        request_func: Any
        if method == 'GET':
            request_func = requests.get
        elif method == 'POST':
            request_func = requests.post
        elif method == 'PATCH':
            request_func = requests.patch
        elif method == 'DELETE':
            request_func = requests.delete
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        try:
            for attempt in self._get_retry_object():
                with attempt:
                    response = request_func(
                        url,
                        json=json if method in ('POST', 'PATCH') else None,
                        params=json if method == 'GET' else None,
                        auth=auth,
                        headers=headers,
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
                    return response.json()
        except RetryError:
            raise AirflowException(f'API requests to Databricks failed {self.retry_limit} times. Giving up.')
        except requests_exceptions.HTTPError as e:
            if wrap_http_errors:
                raise AirflowException(
                    f'Response: {e.response.content}, Status Code: {e.response.status_code}'
                )
            else:
                raise e

    @staticmethod
    def _get_error_code(exception: BaseException) -> str:
        if isinstance(exception, requests_exceptions.HTTPError):
            try:
                jsn = exception.response.json()
                return jsn.get('error_code', '')
            except JSONDecodeError:
                pass

        return ""

    @staticmethod
    def _retryable_error(exception: BaseException) -> bool:
        if not isinstance(exception, requests_exceptions.RequestException):
            return False
        return isinstance(exception, (requests_exceptions.ConnectionError, requests_exceptions.Timeout)) or (
            exception.response is not None
            and (
                exception.response.status_code >= 500
                or exception.response.status_code == 429
                or (
                    exception.response.status_code == 400
                    and BaseDatabricksHook._get_error_code(exception) == 'COULD_NOT_ACQUIRE_LOCK'
                )
            )
        )


class _TokenAuth(AuthBase):
    """
    Helper class for requests Auth field. AuthBase requires you to implement the __call__
    magic function.
    """

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers['Authorization'] = 'Bearer ' + self.token
        return r
