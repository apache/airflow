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
operators talk to the
``api/2.1/jobs/run-now``
`endpoint <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow>_`
or the ``api/2.1/jobs/runs/submit``
`endpoint <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit>`_.
"""
import sys
import time
from time import sleep
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from requests import PreparedRequest, exceptions as requests_exceptions
from requests.auth import AuthBase

from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

RESTART_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/restart")
START_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/start")
TERMINATE_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/delete")

RUN_NOW_ENDPOINT = ('POST', 'api/2.1/jobs/run-now')
SUBMIT_RUN_ENDPOINT = ('POST', 'api/2.1/jobs/runs/submit')
GET_RUN_ENDPOINT = ('GET', 'api/2.1/jobs/runs/get')
CANCEL_RUN_ENDPOINT = ('POST', 'api/2.1/jobs/runs/cancel')

INSTALL_LIBS_ENDPOINT = ('POST', 'api/2.0/libraries/install')
UNINSTALL_LIBS_ENDPOINT = ('POST', 'api/2.0/libraries/uninstall')

LIST_JOBS_ENDPOINT = ('GET', 'api/2.1/jobs/list')

USER_AGENT_HEADER = {'user-agent': f'airflow-{__version__}'}

RUN_LIFE_CYCLE_STATES = ['PENDING', 'RUNNING', 'TERMINATING', 'TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']

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


class RunState:
    """Utility class for the run state concept of Databricks runs."""

    def __init__(
        self, life_cycle_state: str, result_state: str = '', state_message: str = '', *args, **kwargs
    ) -> None:
        self.life_cycle_state = life_cycle_state
        self.result_state = result_state
        self.state_message = state_message

    @property
    def is_terminal(self) -> bool:
        """True if the current state is a terminal state."""
        if self.life_cycle_state not in RUN_LIFE_CYCLE_STATES:
            raise AirflowException(
                (
                    'Unexpected life cycle state: {}: If the state has '
                    'been introduced recently, please check the Databricks user '
                    'guide for troubleshooting information'
                ).format(self.life_cycle_state)
            )
        return self.life_cycle_state in ('TERMINATED', 'SKIPPED', 'INTERNAL_ERROR')

    @property
    def is_successful(self) -> bool:
        """True if the result state is SUCCESS"""
        return self.result_state == 'SUCCESS'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RunState):
            return NotImplemented
        return (
            self.life_cycle_state == other.life_cycle_state
            and self.result_state == other.result_state
            and self.state_message == other.state_message
        )

    def __repr__(self) -> str:
        return str(self.__dict__)


class DatabricksHook(BaseHook):
    """
    Interact with Databricks.

    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    :param timeout_seconds: The amount of time in seconds the requests library
        will wait before timing-out.
    :param retry_limit: The number of times to retry the connection in case of
        service outages.
    :param retry_delay: The number of seconds to wait between retries (it
        might be a floating point number).
    """

    conn_name_attr = 'databricks_conn_id'
    default_conn_name = 'databricks_default'
    conn_type = 'databricks'
    hook_name = 'Databricks'

    def __init__(
        self,
        databricks_conn_id: str = default_conn_name,
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        super().__init__()
        self.databricks_conn_id = databricks_conn_id
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError('Retry limit must be greater than equal to 1')
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.aad_tokens: Dict[str, dict] = {}
        self.aad_timeout_seconds = 10

    @cached_property
    def databricks_conn(self) -> Connection:
        return self.get_connection(self.databricks_conn_id)

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
        attempt_num = 1
        while True:
            try:
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
                        headers={**USER_AGENT_HEADER, 'Content-Type': 'application/x-www-form-urlencoded'},
                        timeout=self.aad_timeout_seconds,
                    )

                resp.raise_for_status()
                jsn = resp.json()
                if 'access_token' not in jsn or jsn.get('token_type') != 'Bearer' or 'expires_on' not in jsn:
                    raise AirflowException(f"Can't get necessary data from AAD token: {jsn}")

                token = jsn['access_token']
                self.aad_tokens[resource] = {'token': token, 'expires_on': int(jsn["expires_on"])}

                return token
            except requests_exceptions.RequestException as e:
                if not _retryable_error(e):
                    raise AirflowException(
                        f'Response: {e.response.content}, Status Code: {e.response.status_code}'
                    )

                self._log_request_error(attempt_num, e.strerror)

            if attempt_num == self.retry_limit:
                raise AirflowException(f'API requests to Azure failed {self.retry_limit} times. Giving up.')

            attempt_num += 1
            sleep(self.retry_delay)

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

    def _do_api_call(self, endpoint_info, json):
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

        if 'host' in self.databricks_conn.extra_dejson:
            host = self._parse_host(self.databricks_conn.extra_dejson['host'])
        else:
            host = self._parse_host(self.databricks_conn.host)

        url = f'https://{host}/{endpoint}'

        aad_headers = self._get_aad_headers()
        headers = {**USER_AGENT_HEADER.copy(), **aad_headers}

        if 'token' in self.databricks_conn.extra_dejson:
            self.log.info(
                'Using token auth. For security reasons, please set token in Password field instead of extra'
            )
            auth = _TokenAuth(self.databricks_conn.extra_dejson['token'])
        elif not self.databricks_conn.login and self.databricks_conn.password:
            self.log.info('Using token auth.')
            auth = _TokenAuth(self.databricks_conn.password)
        elif 'azure_tenant_id' in self.databricks_conn.extra_dejson:
            if self.databricks_conn.login == "" or self.databricks_conn.password == "":
                raise AirflowException("Azure SPN credentials aren't provided")
            self.log.info('Using AAD Token for SPN.')
            auth = _TokenAuth(self._get_aad_token(DEFAULT_DATABRICKS_SCOPE))
        elif self.databricks_conn.extra_dejson.get('use_azure_managed_identity', False):
            self.log.info('Using AAD Token for managed identity.')
            self._check_azure_metadata_service()
            auth = _TokenAuth(self._get_aad_token(DEFAULT_DATABRICKS_SCOPE))
        else:
            self.log.info('Using basic auth.')
            auth = (self.databricks_conn.login, self.databricks_conn.password)

        if method == 'GET':
            request_func = requests.get
        elif method == 'POST':
            request_func = requests.post
        elif method == 'PATCH':
            request_func = requests.patch
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        attempt_num = 1
        while True:
            try:
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
            except requests_exceptions.RequestException as e:
                if not _retryable_error(e):
                    # In this case, the user probably made a mistake.
                    # Don't retry.
                    raise AirflowException(
                        f'Response: {e.response.content}, Status Code: {e.response.status_code}'
                    )

                self._log_request_error(attempt_num, e)

            if attempt_num == self.retry_limit:
                raise AirflowException(
                    f'API requests to Databricks failed {self.retry_limit} times. Giving up.'
                )

            attempt_num += 1
            sleep(self.retry_delay)

    def _log_request_error(self, attempt_num: int, error: str) -> None:
        self.log.error('Attempt %s API Request to Databricks failed with reason: %s', attempt_num, error)

    def run_now(self, json: dict) -> int:
        """
        Utility function to call the ``api/2.1/jobs/run-now`` endpoint.

        :param json: The data used in the body of the request to the ``run-now`` endpoint.
        :return: the run_id as an int
        :rtype: str
        """
        response = self._do_api_call(RUN_NOW_ENDPOINT, json)
        return response['run_id']

    def submit_run(self, json: dict) -> int:
        """
        Utility function to call the ``api/2.1/jobs/runs/submit`` endpoint.

        :param json: The data used in the body of the request to the ``submit`` endpoint.
        :return: the run_id as an int
        :rtype: str
        """
        response = self._do_api_call(SUBMIT_RUN_ENDPOINT, json)
        return response['run_id']

    def list_jobs(self, limit: int = 25, offset: int = 0, expand_tasks: bool = False) -> List[Dict[str, Any]]:
        """
        Lists the jobs in the Databricks Job Service.

        :param limit: The limit/batch size used to retrieve jobs.
        :param offset: The offset of the first job to return, relative to the most recently created job.
        :param expand_tasks: Whether to include task and cluster details in the response.
        :return: A list of jobs.
        """
        has_more = True
        jobs = []

        while has_more:
            json = {
                'limit': limit,
                'offset': offset,
                'expand_tasks': expand_tasks,
            }
            response = self._do_api_call(LIST_JOBS_ENDPOINT, json)
            jobs += response['jobs'] if 'jobs' in response else []
            has_more = response.get('has_more', False)
            if has_more:
                offset += len(response['jobs'])

        return jobs

    def find_job_id_by_name(self, job_name: str) -> Optional[int]:
        """
        Finds job id by its name. If there are multiple jobs with the same name, raises AirflowException.

        :param job_name: The name of the job to look up.
        :return: The job_id as an int or None if no job was found.
        """
        all_jobs = self.list_jobs()
        matching_jobs = [j for j in all_jobs if j['settings']['name'] == job_name]

        if len(matching_jobs) > 1:
            raise AirflowException(
                f"There are more than one job with name {job_name}. Please delete duplicated jobs first"
            )

        if not matching_jobs:
            return None
        else:
            return matching_jobs[0]['job_id']

    def get_run_page_url(self, run_id: int) -> str:
        """
        Retrieves run_page_url.

        :param run_id: id of the run
        :return: URL of the run page
        """
        json = {'run_id': run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        return response['run_page_url']

    def get_job_id(self, run_id: int) -> int:
        """
        Retrieves job_id from run_id.

        :param run_id: id of the run
        :return: Job id for given Databricks run
        """
        json = {'run_id': run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        return response['job_id']

    def get_run_state(self, run_id: int) -> RunState:
        """
        Retrieves run state of the run.

        Please note that any Airflow tasks that call the ``get_run_state`` method will result in
        failure unless you have enabled xcom pickling.  This can be done using the following
        environment variable: ``AIRFLOW__CORE__ENABLE_XCOM_PICKLING``

        If you do not want to enable xcom pickling, use the ``get_run_state_str`` method to get
        a string describing state, or ``get_run_state_lifecycle``, ``get_run_state_result``, or
        ``get_run_state_message`` to get individual components of the run state.

        :param run_id: id of the run
        :return: state of the run
        """
        json = {'run_id': run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        state = response['state']
        return RunState(**state)

    def get_run_state_str(self, run_id: int) -> str:
        """
        Return the string representation of RunState.

        :param run_id: id of the run
        :return: string describing run state
        """
        state = self.get_run_state(run_id)
        run_state_str = (
            f"State: {state.life_cycle_state}. Result: {state.result_state}. {state.state_message}"
        )
        return run_state_str

    def get_run_state_lifecycle(self, run_id: int) -> str:
        """
        Returns the lifecycle state of the run

        :param run_id: id of the run
        :return: string with lifecycle state
        """
        return self.get_run_state(run_id).life_cycle_state

    def get_run_state_result(self, run_id: int) -> str:
        """
        Returns the resulting state of the run

        :param run_id: id of the run
        :return: string with resulting state
        """
        return self.get_run_state(run_id).result_state

    def get_run_state_message(self, run_id: int) -> str:
        """
        Returns the state message for the run

        :param run_id: id of the run
        :return: string with state message
        """
        return self.get_run_state(run_id).state_message

    def cancel_run(self, run_id: int) -> None:
        """
        Cancels the run.

        :param run_id: id of the run
        """
        json = {'run_id': run_id}
        self._do_api_call(CANCEL_RUN_ENDPOINT, json)

    def restart_cluster(self, json: dict) -> None:
        """
        Restarts the cluster.

        :param json: json dictionary containing cluster specification.
        """
        self._do_api_call(RESTART_CLUSTER_ENDPOINT, json)

    def start_cluster(self, json: dict) -> None:
        """
        Starts the cluster.

        :param json: json dictionary containing cluster specification.
        """
        self._do_api_call(START_CLUSTER_ENDPOINT, json)

    def terminate_cluster(self, json: dict) -> None:
        """
        Terminates the cluster.

        :param json: json dictionary containing cluster specification.
        """
        self._do_api_call(TERMINATE_CLUSTER_ENDPOINT, json)

    def install(self, json: dict) -> None:
        """
        Install libraries on the cluster.

        Utility function to call the ``2.0/libraries/install`` endpoint.

        :param json: json dictionary containing cluster_id and an array of library
        """
        self._do_api_call(INSTALL_LIBS_ENDPOINT, json)

    def uninstall(self, json: dict) -> None:
        """
        Uninstall libraries on the cluster.

        Utility function to call the ``2.0/libraries/uninstall`` endpoint.

        :param json: json dictionary containing cluster_id and an array of library
        """
        self._do_api_call(UNINSTALL_LIBS_ENDPOINT, json)


def _retryable_error(exception) -> bool:
    return (
        isinstance(exception, (requests_exceptions.ConnectionError, requests_exceptions.Timeout))
        or exception.response is not None
        and exception.response.status_code >= 500
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
