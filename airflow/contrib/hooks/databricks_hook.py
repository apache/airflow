# -*- coding: utf-8 -*-
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
#
import requests

from collections import namedtuple
from requests.exceptions import ConnectionError, Timeout
from requests.auth import AuthBase

from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

USER_AGENT_HEADER = {'user-agent': 'airflow-{v}'.format(v=__version__)}
DEFAULT_API_VERSION = '2.0'

Endpoint = namedtuple('Endpoint', ['http_method', 'path', 'method'])


class DatabricksHook(BaseHook):
    """
    Interact with Databricks.
    """

    API = {
        # API V2.0
        #   JOBS API
        # '2.0/jobs/create': Endpoint('POST', '2.0/jobs/create', ''),
        # '2.0/jobs/list': Endpoint('GET', '2.0/jobs/list', ''),
        # '2.0/jobs/delete': Endpoint('POST', '2.0/jobs/delete', ''),
        # '2.0/jobs/get': Endpoint('GET', '2.0/jobs/get', ''),
        # '2.0/jobs/reset': Endpoint('POST', '2.0/jobs/reset', ''),
        '2.0/jobs/run-now': Endpoint('POST', '2.0/jobs/run-now', ''),
        '2.0/jobs/runs/submit': Endpoint('POST', '2.0/jobs/runs/submit',
                                         'jobs_runs_submit'),
        # '2.0/jobs/runs/list': Endpoint('GET', '2.0/jobs/runs/list', ''),
        '2.0/jobs/runs/get': Endpoint('GET', '2.0/jobs/runs/get',
                                      'jobs_runs_get'),
        # '2.0/jobs/runs/export': Endpoint('GET', '2.0/jobs/runs/export', ''),
        '2.0/jobs/runs/cancel': Endpoint('POST', '2.0/jobs/runs/cancel',
                                         'jobs_runs_cancel')
        # '2.0/jobs/runs/get-output': Endpoint('GET',
        #                                      '2.0/jobs/runs/get-output', '')
    }
    # TODO: https://docs.databricks.com/api/latest/index.html
    # TODO: https://docs.databricks.com/api/latest/dbfs.html
    # TODO: https://docs.databricks.com/api/latest/groups.html
    # TODO: https://docs.databricks.com/api/latest/instance-profiles.html
    # TODO: https://docs.databricks.com/api/latest/libraries.html
    # TODO: https://docs.databricks.com/api/latest/tokens.html
    # TODO: https://docs.databricks.com/api/latest/workspace.html

    def __init__(self, databricks_conn_id='databricks_default',
                 timeout_seconds=180, retry_limit=3):
        """
        :param databricks_conn_id: The name of the databricks connection to
            use.
        :type databricks_conn_id: string
        :param timeout_seconds: The amount of time in seconds the requests
            library will wait before timing-out.
        :type timeout_seconds: int
        :param retry_limit: The number of times to retry the connection in
            case of service outages.
        :type retry_limit: int
        """
        self.databricks_conn_id = databricks_conn_id
        self.databricks_conn = self.get_connection(databricks_conn_id)
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError('Retry limit must be greater than equal to 1')
        self.retry_limit = retry_limit

    @staticmethod
    def _parse_host(host):
        """Verify connection host setting provided by the user.

        The purpose of this function is to be robust to improper connections
        settings provided by users, specifically in the host field.

        For example -- when users supply ``https://xx.cloud.databricks.com`` as
        the host, we must strip out the protocol to get the host.

        >>> host = 'https://xx.cloud.databricks.com'
        >>> h = DatabricksHook()
        >>> assert h._parse_host(host) == 'xx.cloud.databricks.com'

        In the case where users supply the correct ``xx.cloud.databricks.com``
        host, this function is a no-op.

        >>> host = 'xx.cloud.databricks.com'
        >>> assert h._parse_host(host) == 'xx.cloud.databricks.com'

        """
        return urlparse(host).hostname or host

    def _do_api_call(self, endpoint, json):
        """Perform an API call with retries.

        :param endpoint_info: Instance of Endpoint with http_method and path.
        :type endpoint_info: Endpoint
        :param json: Parameters for this API call.
        :type json: dict
        :return: If the api call returns a OK status code,
            this function returns the response in JSON. Otherwise,
            we throw an AirflowException.
        :rtype: dict
        """
        if isinstance(endpoint, str):
            endpoint = self.API[endpoint]
        url = 'https://{host}/api/{endpoint}'.format(
            host=self._parse_host(self.databricks_conn.host),
            endpoint=endpoint.path)
        self.log.info('Calling endpoint (%s) with url: %s', endpoint, url)
        if 'token' in self.databricks_conn.extra_dejson:
            self.log.info('Using token auth.')
            auth = _TokenAuth(self.databricks_conn.extra_dejson['token'])
        else:
            self.log.info('Using basic auth.')
            auth = (self.databricks_conn.login, self.databricks_conn.password)
        if endpoint.http_method == 'GET':
            request_func = requests.get
        elif endpoint.http_method == 'POST':
            request_func = requests.post
        else:
            raise AirflowException('Unexpected HTTP Method: {}'.format(
                endpoint.http_method
            ))

        for attempt in range(1, self.retry_limit + 1):
            try:
                response = request_func(
                    url,
                    json=json,
                    auth=auth,
                    headers=USER_AGENT_HEADER,
                    timeout=self.timeout_seconds)
                if response.status_code == requests.codes.ok:
                    return response.json()
                else:
                    # In this case, the user probably made a mistake.
                    # Don't retry.
                    msg = 'Response: {}, Status Code: {}'
                    raise AirflowException(msg.format(response.content,
                                                      response.status_code))
            except (ConnectionError, Timeout) as exception:
                msg = 'Attempt {} API Request to Databricks failed with'
                msg += 'reason: {}'
                self.log.error(msg.format(attempt, exception))

        msg = 'API requests to Databricks failed {} times. Giving up.'
        raise AirflowException(msg.format(self.retry_limit))

    def get_api_method(self, endpoint):
        """Retrieve the method that handle one endpoint."""
        try:
            return getattr(self, self.API[endpoint].method)
        except AttributeError:
            AirflowException('Endpoint handler %s not yet implemented.',
                             endpoint)

    def jobs_runs_submit(self, json):
        """Call the ``api/2.0/jobs/runs/submit`` endpoint.

        :param json: The data used in the body of the request to the
            ``submit`` endpoint.
        :type json: dict
        :return: A dict with the run_id as a string
        :rtype: dict
        """
        return self._do_api_call(self.API['2.0/jobs/runs/submit'], json)

    def jobs_runs_get(self, run_id):
        """Call the ``2.0/jobs/runs/get`` endpoint.

        Retrieves the metadata of a run.

        :param run_id: The id of a run you wish to get the data about.
        :type run_id: int or string
        :return: a dict with the metadata from a run (endpoint json response)
        """
        return self._do_api_call(self.API['2.0/jobs/runs/get'],
                                 {'run_id': run_id})

    def jobs_runs_cancel(self, run_id):
        """Call the ``2.0/jobs/runs/cancel`` endpoint.

        Send a CANCEL command to a specific run.

        This method does not verify if the run was actually cancelled.

        :param  run_id: The id of the RUN.
        :type run_id: int
        """
        self._do_api_call(self.API['2.0/jobs/runs/cancel'],
                          {'run_id': run_id})

    def get_run_page_url(self, run_id):
        """Get the webpage url to follow the run status.

        :param run_id: The id of the RUN.
        :type run_id: int
        :return: A URL, such as:
            https://<user>.cloud.databricks.com/#job/1234/run/2
        :rtype: string
        """
        return self.jobs_runs_get(run_id).get('run_page_url')

    def get_run_state(self, run_id):
        """Get the state of a RUN.

        :param  run_id: The id of the RUN.
        :type run_id: int
        :return: A RunState object with the run state information.
        :rtype: RunState
        """
        response = self.jobs_runs_get(run_id)
        state = response.get('state')
        life_cycle_state = state.get('life_cycle_state')
        # result_state may not be in the state if not terminal
        result_state = state.get('result_state', None)
        state_message = state.get('state_message')
        return RunState(life_cycle_state, result_state, state_message)


class RunState:
    """
    Utility class for the run state concept of Databricks runs.
    """

    STATES = [
        'PENDING',
        'RUNNING',
        'TERMINATING',
        'TERMINATED',
        'SKIPPED',
        'INTERNAL_ERROR'
    ]

    TERMINAL_STATES = ('TERMINATED', 'SKIPPED', 'INTERNAL_ERROR')

    def __init__(self, life_cycle_state, result_state, state_message):
        self.life_cycle_state = life_cycle_state
        self.result_state = result_state
        self.state_message = state_message

    @property
    def is_terminal(self):
        """Check wether the terminal is a terminal state."""
        if self.life_cycle_state not in RunState.STATES:
            msg = 'Unexpected life cycle state: {}: If the state has been '
            msg += 'introduced recently, please check the Databricks user '
            msg += 'guide for troubleshooting information'
            raise AirflowException(msg.format(self.life_cycle_state))
        return self.life_cycle_state in RunState.TERMINAL_STATES

    @property
    def is_successful(self):
        return self.result_state == 'SUCCESS'

    def __eq__(self, other):
        return self.life_cycle_state == other.life_cycle_state and \
            self.result_state == other.result_state and \
            self.state_message == other.state_message

    def __repr__(self):
        return str(self.__dict__)


class _TokenAuth(AuthBase):
    """Helper class for requests Auth field.

    AuthBase requires you to implement the __call__ magic function.
    """
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers['Authorization'] = 'Bearer ' + self.token
        return r
