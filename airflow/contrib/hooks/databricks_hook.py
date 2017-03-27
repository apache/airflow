# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import requests

from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from requests import exceptions as requests_exceptions


try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse


SUBMIT_RUN_ENDPOINT = ('POST', 'api/2.0/jobs/runs/submit')
GET_RUN_ENDPOINT = ('GET', 'api/2.0/jobs/runs/get')
USER_AGENT_HEADER = {'user-agent': 'airflow-{v}'.format(v=__version__)}


class DatabricksHook(BaseHook):
    """
    Interact with Databricks.
    """
    def __init__(
            self,
            databricks_conn_id='databricks_default',
            timeout_seconds=180,
            retry_limit=3):
        """
        :param databricks_conn_id: The name of the databricks connection to use.
        :type databricks_conn_id: string
        :param timeout_seconds: The amount of time in seconds the requests library
            will wait before timing-out.
        :type timeout_seconds: int
        :param retry_limit: The number of times to retry the connection in case of
            service outages.
        :type retry_limit: int
        """
        self.databricks_conn_id = databricks_conn_id
        self.databricks_conn = self.get_connection(databricks_conn_id)
        self.timeout_seconds = timeout_seconds
        self.retry_limit = retry_limit

    def _parse_host(self, host):
        """
        The purpose of this function is to be robust to improper connections
        settings provided by users, specifically in the host field.

        For example -- when users supply ``https://XX.cloud.databricks.com`` as the
        host, we must strip out the protocol to get the host.
            _parse_host(https://XX.cloud.databricks.com) -> XX.cloud.databricks.com.

        In the case where users supply the correct ``XX.cloud.databricks.com`` as the
        host, this function is a no-op.
            _parse_host(XX.cloud.databricks.com) -> XX.cloud.databricks.com.
        """
        urlparse_host = urlparse.urlparse(host).hostname
        if urlparse_host:
            # In this case, host = https://XX.cloud.databricks.com
            return urlparse_host
        else:
            # In this case, host = XX.cloud.databricks.com
            return host

    def _do_api_call(self, endpoint_info, api_parameters):
        """
        Utility function to perform an API call with retries
        :param endpoint_info: Tuple of method and endpoint
        :type endpoint_info: (string, string)
        :param api_parameters: Parameters for this API call.
        :type api_parameters: dict
        :return: If the api call returns a OK status code,
            this function returns the response in JSON.
        :rtype: dict
        """
        method, endpoint = endpoint_info
        url = 'https://{host}/{endpoint}'.format(
            host=self._parse_host(self.databricks_conn.host),
            endpoint=endpoint)
        auth = (self.databricks_conn.login, self.databricks_conn.password)
        if method == 'GET':
            request_func = requests.get
        elif method == 'POST':
            request_func = requests.post
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        for attempt_num in range(1, self.retry_limit+1):
            try:
                response = request_func(
                    url,
                    json=api_parameters,
                    auth=auth,
                    headers=USER_AGENT_HEADER,
                    timeout=self.timeout_seconds,
                    verify=False)
                if response.ok:
                    return response.json()
                else:
                    # In this case, the user probably made a mistake.
                    # Don't retry.
                    raise AirflowException(response.content)
            except (requests_exceptions.ConnectionError,
                    requests_exceptions.Timeout) as e:
                logging.error(('Attempt {0} API Request to Databricks failed ' +
                              'with reason: {1}').format(attempt_num, e))
        raise AirflowException(('API requests to Databricks failed {} times. ' +
                               'Giving up.').format(self.retry_limit))

    def submit_run(
            self,
            spark_jar_task=None,
            notebook_task=None,
            new_cluster=None,
            existing_cluster_id=None,
            libraries=None,
            run_name=None,
            timeout_seconds=0,
            **kwargs):
        """
        Utility function to call the ``api/2.0/jobs/runs/submit`` endpoint.
        Parameters are identical to those provided to ``DatabricksSubmitRunOperator``.

        .. seealso: DatabricksSubmitRunOperator
        """
        api_params = kwargs.copy()
        if spark_jar_task is not None:
            api_params['spark_jar_task'] = spark_jar_task
        if notebook_task is not None:
            api_params['notebook_task'] = notebook_task
        if new_cluster is not None:
            api_params['new_cluster'] = new_cluster
        if existing_cluster_id is not None:
            api_params['existing_cluster_id'] = existing_cluster_id
        if libraries is not None:
            api_params['libraries'] = libraries
        if run_name is not None:
            api_params['run_name'] = run_name
        api_params['timeout_seconds'] = timeout_seconds
        response = self._do_api_call(SUBMIT_RUN_ENDPOINT, api_params)
        return response['run_id']

    def get_run_page_url(self, run_id):
        api_params = {'run_id': run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, api_params)
        return response['run_page_url']

    def get_run_state(self, run_id):
        api_params = {'run_id': run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, api_params)
        life_cycle_state = response['state']['life_cycle_state']
        # result_state may not be in the state if not terminal
        result_state = response['state'].get('result_state', None)
        state_message = response['state']['state_message']
        return RunState(life_cycle_state, result_state, state_message)


RUN_LIFE_CYCLE_STATES = [
        'PENDING',
        'RUNNING',
        'TERMINATING',
        'TERMINATED',
        'SKIPPED',
        'INTERNAL_ERROR'
]


class RunState:
    """
    Utility class for the run state concept of Databricks runs.
    """
    def __init__(self, life_cycle_state, result_state, state_message):
        self.life_cycle_state = life_cycle_state
        self.result_state = result_state
        self.state_message = state_message

    @property
    def is_terminal(self):
        if self.life_cycle_state not in RUN_LIFE_CYCLE_STATES:
            raise AirflowException('Unexpected life cycle state: {}:'.format(
                self.life_cycle_state))
        return self.life_cycle_state == 'TERMINATED' or \
               self.life_cycle_state == 'SKIPPED' or \
               self.life_cycle_state == 'INTERNAL_ERROR'

    @property
    def is_successful(self):
        return self.result_state == 'SUCCESS'

    def __eq__(self, other):
        return self.life_cycle_state == other.life_cycle_state and \
               self.result_state == other.result_state and \
               self.state_message == other.state_message

    def __repr__(self):
        return str(self.__dict__)
