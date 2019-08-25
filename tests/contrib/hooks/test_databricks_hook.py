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

import itertools
import json
import unittest

from requests import exceptions as requests_exceptions

from airflow import __version__
from airflow.contrib.hooks.databricks_hook import (
    DatabricksHook,
    RunState,
    SUBMIT_RUN_ENDPOINT
)
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils import db
from tests.compat import mock

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
CLUSTER_ID = 'cluster_id'
RUN_ID = 1
JOB_ID = 42
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
NOTEBOOK_PARAMS = {
    "dry-run": "true",
    "oldest-time-to-consider": "1457570074236"
}
JAR_PARAMS = ["param1", "param2"]
RESULT_STATE = None  # type: None


def run_now_endpoint(host):
    """
    Utility function to generate the run now endpoint given the host.
    """
    return 'https://{}/api/2.0/jobs/run-now'.format(host)


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


def start_cluster_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return 'https://{}/api/2.0/clusters/start'.format(host)


def restart_cluster_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return 'https://{}/api/2.0/clusters/restart'.format(host)


def terminate_cluster_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return 'https://{}/api/2.0/clusters/delete'.format(host)


def create_valid_response_mock(content):
    response = mock.MagicMock()
    response.json.return_value = content
    return response


def create_post_side_effect(exception, status_code=500):
    if exception != requests_exceptions.HTTPError:
        return exception()
    else:
        response = mock.MagicMock()
        response.status_code = status_code
        response.raise_for_status.side_effect = exception(response=response)
        return response


def setup_mock_requests(mock_requests,
                        exception,
                        status_code=500,
                        error_count=None,
                        response_content=None):
    side_effect = create_post_side_effect(exception, status_code)

    if error_count is None:
        # POST requests will fail indefinitely
        mock_requests.post.side_effect = itertools.repeat(side_effect)
    else:
        # POST requests will fail 'error_count' times, and then they will succeed (once)
        mock_requests.post.side_effect = \
            [side_effect] * error_count + [create_valid_response_mock(response_content)]


class TestDatabricksHook(unittest.TestCase):
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
        conn.extra = None
        session.commit()

        self.hook = DatabricksHook(retry_delay=0)

    def test_parse_host_with_proper_host(self):
        host = self.hook._parse_host(HOST)
        self.assertEqual(host, HOST)

    def test_parse_host_with_scheme(self):
        host = self.hook._parse_host(HOST_WITH_SCHEME)
        self.assertEqual(host, HOST)

    def test_init_bad_retry_limit(self):
        with self.assertRaises(ValueError):
            DatabricksHook(retry_limit=0)

    def test_do_api_call_retries_with_retryable_error(self):
        for exception in [requests_exceptions.ConnectionError,
                          requests_exceptions.SSLError,
                          requests_exceptions.Timeout,
                          requests_exceptions.ConnectTimeout,
                          requests_exceptions.HTTPError]:
            with mock.patch('airflow.contrib.hooks.databricks_hook.requests') as mock_requests:
                with mock.patch.object(self.hook.log, 'error') as mock_errors:
                    setup_mock_requests(mock_requests, exception)

                    with self.assertRaises(AirflowException):
                        self.hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                    self.assertEqual(mock_errors.call_count, self.hook.retry_limit)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_do_api_call_does_not_retry_with_non_retryable_error(self, mock_requests):
        setup_mock_requests(
            mock_requests, requests_exceptions.HTTPError, status_code=400
        )

        with mock.patch.object(self.hook.log, 'error') as mock_errors:
            with self.assertRaises(AirflowException):
                self.hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

            mock_errors.assert_not_called()

    def test_do_api_call_succeeds_after_retrying(self):
        for exception in [requests_exceptions.ConnectionError,
                          requests_exceptions.SSLError,
                          requests_exceptions.Timeout,
                          requests_exceptions.ConnectTimeout,
                          requests_exceptions.HTTPError]:
            with mock.patch('airflow.contrib.hooks.databricks_hook.requests') as mock_requests:
                with mock.patch.object(self.hook.log, 'error') as mock_errors:
                    setup_mock_requests(
                        mock_requests,
                        exception,
                        error_count=2,
                        response_content={'run_id': '1'}
                    )

                    response = self.hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                    self.assertEqual(mock_errors.call_count, 2)
                    self.assertEqual(response, {'run_id': '1'})

    @mock.patch('airflow.contrib.hooks.databricks_hook.sleep')
    def test_do_api_call_waits_between_retries(self, mock_sleep):
        retry_delay = 5
        self.hook = DatabricksHook(retry_delay=retry_delay)

        for exception in [requests_exceptions.ConnectionError,
                          requests_exceptions.SSLError,
                          requests_exceptions.Timeout,
                          requests_exceptions.ConnectTimeout,
                          requests_exceptions.HTTPError]:
            with mock.patch('airflow.contrib.hooks.databricks_hook.requests') as mock_requests:
                with mock.patch.object(self.hook.log, 'error'):
                    mock_sleep.reset_mock()
                    setup_mock_requests(mock_requests, exception)

                    with self.assertRaises(AirflowException):
                        self.hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                    self.assertEqual(len(mock_sleep.mock_calls), self.hook.retry_limit - 1)
                    mock_sleep.assert_called_with(retry_delay)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_submit_run(self, mock_requests):
        mock_requests.post.return_value.json.return_value = {'run_id': '1'}
        json = {
            'notebook_task': NOTEBOOK_TASK,
            'new_cluster': NEW_CLUSTER
        }
        run_id = self.hook.submit_run(json)

        self.assertEqual(run_id, '1')
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
    def test_run_now(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {'run_id': '1'}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        json = {
            'notebook_params': NOTEBOOK_PARAMS,
            'jar_params': JAR_PARAMS,
            'job_id': JOB_ID
        }
        run_id = self.hook.run_now(json)

        self.assertEqual(run_id, '1')

        mock_requests.post.assert_called_once_with(
            run_now_endpoint(HOST),
            json={
                'notebook_params': NOTEBOOK_PARAMS,
                'jar_params': JAR_PARAMS,
                'job_id': JOB_ID
            },
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_get_run_page_url(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE

        run_page_url = self.hook.get_run_page_url(RUN_ID)

        self.assertEqual(run_page_url, RUN_PAGE_URL)
        mock_requests.get.assert_called_once_with(
            get_run_endpoint(HOST),
            json={'run_id': RUN_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_get_run_state(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE

        run_state = self.hook.get_run_state(RUN_ID)

        self.assertEqual(run_state, RunState(
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
        mock_requests.post.return_value.json.return_value = GET_RUN_RESPONSE

        self.hook.cancel_run(RUN_ID)

        mock_requests.post.assert_called_once_with(
            cancel_run_endpoint(HOST),
            json={'run_id': RUN_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_start_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        self.hook.start_cluster({"cluster_id": CLUSTER_ID})

        mock_requests.post.assert_called_once_with(
            start_cluster_endpoint(HOST),
            json={'cluster_id': CLUSTER_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_restart_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        self.hook.restart_cluster({"cluster_id": CLUSTER_ID})

        mock_requests.post.assert_called_once_with(
            restart_cluster_endpoint(HOST),
            json={'cluster_id': CLUSTER_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_terminate_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        self.hook.terminate_cluster({"cluster_id": CLUSTER_ID})

        mock_requests.post.assert_called_once_with(
            terminate_cluster_endpoint(HOST),
            json={'cluster_id': CLUSTER_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds)


class TestDatabricksHookToken(unittest.TestCase):
    """
    Tests for DatabricksHook when auth is done with token.
    """

    @db.provide_session
    def setUp(self, session=None):
        conn = session.query(Connection) \
            .filter(Connection.conn_id == DEFAULT_CONN_ID) \
            .first()
        conn.extra = json.dumps({'token': TOKEN, 'host': HOST})

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

        self.assertEqual(run_id, '1')
        args = mock_requests.post.call_args
        kwargs = args[1]
        self.assertEqual(kwargs['auth'].token, TOKEN)


class TestRunState(unittest.TestCase):
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
