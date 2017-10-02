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

import json
import unittest

from airflow import __version__
from airflow.contrib.hooks.databricks_hook import DatabricksHook, RunState, SUBMIT_RUN_ENDPOINT, _TokenAuth
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils import db
from requests import exceptions as requests_exceptions

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

TASK_ID = 'databricks-operator'
DEFAULT_CONN_ID = 'databricks_default'
NOTEBOOK_TASK = {
    'notebook_path': '/test'
}
NEW_CLUSTER = {
    'spark_version': '2.0.x-scala2.10',
    'node_type_id': 'r3.xlarge',
    'num_workers': 1
}
RUN_ID = 1
HOST = 'xx.cloud.databricks.com'
HOST_WITH_SCHEME = 'https://xx.cloud.databricks.com'
LOGIN = 'login'
PASSWORD = 'password'
TOKEN = 'token'
USER_AGENT_HEADER = {'user-agent': 'airflow-{v}'.format(v=__version__)}
RUN_PAGE_URL = 'https://XX.cloud.databricks.com/#jobs/1/runs/1'
LIFE_CYCLE_STATE = 'PENDING'
STATE_MESSAGE = 'Waiting for cluster'
GET_RUN_RESPONSE = {
    'run_page_url': RUN_PAGE_URL,
    'state': {
        'life_cycle_state': LIFE_CYCLE_STATE,
        'state_message': STATE_MESSAGE
    }
}
RESULT_STATE = None


def submit_run_endpoint(host):
    """
    Utility function to generate the submit run endpoint given the host.
    """
    return 'https://{}/api/2.0/jobs/runs/submit'.format(host)


def get_run_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return 'https://{}/api/2.0/jobs/runs/get'.format(host)

def cancel_run_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return 'https://{}/api/2.0/jobs/runs/cancel'.format(host)

class DatabricksHookTest(unittest.TestCase):
    """
    Tests for DatabricksHook.
    """
    @db.provide_session
    def setUp(self, session=None):
        conn = session.query(Connection) \
            .filter(Connection.conn_id == DEFAULT_CONN_ID) \
            .first()
        conn.host = HOST
        conn.login = LOGIN
        conn.password = PASSWORD
        session.commit()

        self.hook = DatabricksHook()

    def test_parse_host_with_proper_host(self):
        host = self.hook._parse_host(HOST)
        self.assertEquals(host, HOST)

    def test_parse_host_with_scheme(self):
        host = self.hook._parse_host(HOST_WITH_SCHEME)
        self.assertEquals(host, HOST)

    def test_init_bad_retry_limit(self):
        with self.assertRaises(AssertionError):
            DatabricksHook(retry_limit = 0)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_do_api_call_with_error_retry(self, mock_requests):
        for exception in [requests_exceptions.ConnectionError, requests_exceptions.Timeout]:
            with mock.patch.object(self.hook.log, 'error') as mock_errors:
                mock_requests.reset_mock()
                mock_requests.post.side_effect = exception()

                with self.assertRaises(AirflowException):
                    self.hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                self.assertEquals(len(mock_errors.mock_calls), self.hook.retry_limit)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_do_api_call_with_bad_status_code(self, mock_requests):
        mock_requests.codes.ok = 200
        status_code_mock = mock.PropertyMock(return_value=500)
        type(mock_requests.post.return_value).status_code = status_code_mock
        with self.assertRaises(AirflowException):
            self.hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_submit_run(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {'run_id': '1'}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        json = {
          'notebook_task': NOTEBOOK_TASK,
          'new_cluster': NEW_CLUSTER
        }
        run_id = self.hook.submit_run(json)

        self.assertEquals(run_id, '1')
        mock_requests.post.assert_called_once_with(
            submit_run_endpoint(HOST),
            json={
                'notebook_task': NOTEBOOK_TASK,
                'new_cluster': NEW_CLUSTER,
            },
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_get_run_page_url(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.get.return_value).status_code = status_code_mock

        run_page_url = self.hook.get_run_page_url(RUN_ID)

        self.assertEquals(run_page_url, RUN_PAGE_URL)
        mock_requests.get.assert_called_once_with(
            get_run_endpoint(HOST),
            json={'run_id': RUN_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_get_run_state(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.get.return_value).status_code = status_code_mock

        run_state = self.hook.get_run_state(RUN_ID)

        self.assertEquals(run_state, RunState(
            LIFE_CYCLE_STATE,
            RESULT_STATE,
            STATE_MESSAGE))
        mock_requests.get.assert_called_once_with(
            get_run_endpoint(HOST),
            json={'run_id': RUN_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_cancel_run(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = GET_RUN_RESPONSE
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        self.hook.cancel_run(RUN_ID)

        mock_requests.post.assert_called_once_with(
            cancel_run_endpoint(HOST),
            json={'run_id': RUN_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds)


class DatabricksHookTokenTest(unittest.TestCase):
    """
    Tests for DatabricksHook when auth is done with token.
    """
    @db.provide_session
    def setUp(self, session=None):
        conn = session.query(Connection) \
            .filter(Connection.conn_id == DEFAULT_CONN_ID) \
            .first()
        conn.extra = json.dumps({'token': TOKEN})
        session.commit()

        self.hook = DatabricksHook()

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_submit_run(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {'run_id': '1'}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        json = {
          'notebook_task': NOTEBOOK_TASK,
          'new_cluster': NEW_CLUSTER
        }
        run_id = self.hook.submit_run(json)

        self.assertEquals(run_id, '1')
        args = mock_requests.post.call_args
        kwargs = args[1]
        self.assertEquals(kwargs['auth'].token, TOKEN)


class RunStateTest(unittest.TestCase):
    def test_is_terminal_true(self):
        terminal_states = ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']
        for state in terminal_states:
            run_state = RunState(state, '', '')
            self.assertTrue(run_state.is_terminal)

    def test_is_terminal_false(self):
        non_terminal_states = ['PENDING', 'RUNNING', 'TERMINATING']
        for state in non_terminal_states:
            run_state = RunState(state, '', '')
            self.assertFalse(run_state.is_terminal)

    def test_is_terminal_with_nonexistent_life_cycle_state(self):
        run_state = RunState('blah', '', '')
        with self.assertRaises(AirflowException):
            run_state.is_terminal

    def test_is_successful(self):
        run_state = RunState('TERMINATED', 'SUCCESS', '')
        self.assertTrue(run_state.is_successful)
