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
from unittest import mock

import pytest
from requests import exceptions as requests_exceptions

from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.databricks.hooks.databricks import SUBMIT_RUN_ENDPOINT, DatabricksHook, RunState
from airflow.utils.session import provide_session

TASK_ID = 'databricks-operator'
DEFAULT_CONN_ID = 'databricks_default'
NOTEBOOK_TASK = {'notebook_path': '/test'}
SPARK_PYTHON_TASK = {'python_file': 'test.py', 'parameters': ['--param', '123']}
NEW_CLUSTER = {'spark_version': '2.0.x-scala2.10', 'node_type_id': 'r3.xlarge', 'num_workers': 1}
CLUSTER_ID = 'cluster_id'
RUN_ID = 1
JOB_ID = 42
HOST = 'xx.cloud.databricks.com'
HOST_WITH_SCHEME = 'https://xx.cloud.databricks.com'
LOGIN = 'login'
PASSWORD = 'password'
TOKEN = 'token'
USER_AGENT_HEADER = {'user-agent': f'airflow-{__version__}'}
RUN_PAGE_URL = 'https://XX.cloud.databricks.com/#jobs/1/runs/1'
LIFE_CYCLE_STATE = 'PENDING'
STATE_MESSAGE = 'Waiting for cluster'
GET_RUN_RESPONSE = {
    'job_id': JOB_ID,
    'run_page_url': RUN_PAGE_URL,
    'state': {'life_cycle_state': LIFE_CYCLE_STATE, 'state_message': STATE_MESSAGE},
}
NOTEBOOK_PARAMS = {"dry-run": "true", "oldest-time-to-consider": "1457570074236"}
JAR_PARAMS = ["param1", "param2"]
RESULT_STATE = None  # type: None
LIBRARIES = [
    {"jar": "dbfs:/mnt/libraries/library.jar"},
    {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2", "exclusions": ["slf4j:slf4j"]}},
]


def run_now_endpoint(host):
    """
    Utility function to generate the run now endpoint given the host.
    """
    return f'https://{host}/api/2.0/jobs/run-now'


def submit_run_endpoint(host):
    """
    Utility function to generate the submit run endpoint given the host.
    """
    return f'https://{host}/api/2.0/jobs/runs/submit'


def get_run_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return f'https://{host}/api/2.0/jobs/runs/get'


def cancel_run_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return f'https://{host}/api/2.0/jobs/runs/cancel'


def start_cluster_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return f'https://{host}/api/2.0/clusters/start'


def restart_cluster_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return f'https://{host}/api/2.0/clusters/restart'


def terminate_cluster_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return f'https://{host}/api/2.0/clusters/delete'


def install_endpoint(host):
    """
    Utility function to generate the install endpoint given the host.
    """
    return f'https://{host}/api/2.0/libraries/install'


def uninstall_endpoint(host):
    """
    Utility function to generate the uninstall endpoint given the host.
    """
    return f'https://{host}/api/2.0/libraries/uninstall'


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


def setup_mock_requests(mock_requests, exception, status_code=500, error_count=None, response_content=None):
    side_effect = create_post_side_effect(exception, status_code)

    if error_count is None:
        # POST requests will fail indefinitely
        mock_requests.post.side_effect = itertools.repeat(side_effect)
    else:
        # POST requests will fail 'error_count' times, and then they will succeed (once)
        mock_requests.post.side_effect = [side_effect] * error_count + [
            create_valid_response_mock(response_content)
        ]


class TestDatabricksHook(unittest.TestCase):
    """
    Tests for DatabricksHook.
    """

    @provide_session
    def setUp(self, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.host = HOST
        conn.login = LOGIN
        conn.password = PASSWORD
        conn.extra = None
        session.commit()

        self.hook = DatabricksHook(retry_delay=0)

    def test_parse_host_with_proper_host(self):
        host = self.hook._parse_host(HOST)
        assert host == HOST

    def test_parse_host_with_scheme(self):
        host = self.hook._parse_host(HOST_WITH_SCHEME)
        assert host == HOST

    def test_init_bad_retry_limit(self):
        with pytest.raises(ValueError):
            DatabricksHook(retry_limit=0)

    def test_do_api_call_retries_with_retryable_error(self):
        for exception in [
            requests_exceptions.ConnectionError,
            requests_exceptions.SSLError,
            requests_exceptions.Timeout,
            requests_exceptions.ConnectTimeout,
            requests_exceptions.HTTPError,
        ]:
            with mock.patch('airflow.providers.databricks.hooks.databricks.requests') as mock_requests:
                with mock.patch.object(self.hook.log, 'error') as mock_errors:
                    setup_mock_requests(mock_requests, exception)

                    with pytest.raises(AirflowException):
                        self.hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                    assert mock_errors.call_count == self.hook.retry_limit

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_do_api_call_does_not_retry_with_non_retryable_error(self, mock_requests):
        setup_mock_requests(mock_requests, requests_exceptions.HTTPError, status_code=400)

        with mock.patch.object(self.hook.log, 'error') as mock_errors:
            with pytest.raises(AirflowException):
                self.hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

            mock_errors.assert_not_called()

    def test_do_api_call_succeeds_after_retrying(self):
        for exception in [
            requests_exceptions.ConnectionError,
            requests_exceptions.SSLError,
            requests_exceptions.Timeout,
            requests_exceptions.ConnectTimeout,
            requests_exceptions.HTTPError,
        ]:
            with mock.patch('airflow.providers.databricks.hooks.databricks.requests') as mock_requests:
                with mock.patch.object(self.hook.log, 'error') as mock_errors:
                    setup_mock_requests(
                        mock_requests, exception, error_count=2, response_content={'run_id': '1'}
                    )

                    response = self.hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                    assert mock_errors.call_count == 2
                    assert response == {'run_id': '1'}

    @mock.patch('airflow.providers.databricks.hooks.databricks.sleep')
    def test_do_api_call_waits_between_retries(self, mock_sleep):
        retry_delay = 5
        self.hook = DatabricksHook(retry_delay=retry_delay)

        for exception in [
            requests_exceptions.ConnectionError,
            requests_exceptions.SSLError,
            requests_exceptions.Timeout,
            requests_exceptions.ConnectTimeout,
            requests_exceptions.HTTPError,
        ]:
            with mock.patch('airflow.providers.databricks.hooks.databricks.requests') as mock_requests:
                with mock.patch.object(self.hook.log, 'error'):
                    mock_sleep.reset_mock()
                    setup_mock_requests(mock_requests, exception)

                    with pytest.raises(AirflowException):
                        self.hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                    assert len(mock_sleep.mock_calls) == self.hook.retry_limit - 1
                    calls = [mock.call(retry_delay), mock.call(retry_delay)]
                    mock_sleep.assert_has_calls(calls)

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_do_api_call_patch(self, mock_requests):
        mock_requests.patch.return_value.json.return_value = {'cluster_name': 'new_name'}
        data = {'cluster_name': 'new_name'}
        patched_cluster_name = self.hook._do_api_call(('PATCH', 'api/2.0/jobs/runs/submit'), data)

        assert patched_cluster_name['cluster_name'] == 'new_name'
        mock_requests.patch.assert_called_once_with(
            submit_run_endpoint(HOST),
            json={'cluster_name': 'new_name'},
            params=None,
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_submit_run(self, mock_requests):
        mock_requests.post.return_value.json.return_value = {'run_id': '1'}
        data = {'notebook_task': NOTEBOOK_TASK, 'new_cluster': NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        assert run_id == '1'
        mock_requests.post.assert_called_once_with(
            submit_run_endpoint(HOST),
            json={
                'notebook_task': NOTEBOOK_TASK,
                'new_cluster': NEW_CLUSTER,
            },
            params=None,
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_spark_python_submit_run(self, mock_requests):
        mock_requests.post.return_value.json.return_value = {'run_id': '1'}
        data = {'spark_python_task': SPARK_PYTHON_TASK, 'new_cluster': NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        assert run_id == '1'
        mock_requests.post.assert_called_once_with(
            submit_run_endpoint(HOST),
            json={
                'spark_python_task': SPARK_PYTHON_TASK,
                'new_cluster': NEW_CLUSTER,
            },
            params=None,
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_run_now(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {'run_id': '1'}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        data = {'notebook_params': NOTEBOOK_PARAMS, 'jar_params': JAR_PARAMS, 'job_id': JOB_ID}
        run_id = self.hook.run_now(data)

        assert run_id == '1'

        mock_requests.post.assert_called_once_with(
            run_now_endpoint(HOST),
            json={'notebook_params': NOTEBOOK_PARAMS, 'jar_params': JAR_PARAMS, 'job_id': JOB_ID},
            params=None,
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_get_run_page_url(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE

        run_page_url = self.hook.get_run_page_url(RUN_ID)

        assert run_page_url == RUN_PAGE_URL
        mock_requests.get.assert_called_once_with(
            get_run_endpoint(HOST),
            json=None,
            params={'run_id': RUN_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_get_job_id(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE

        job_id = self.hook.get_job_id(RUN_ID)

        assert job_id == JOB_ID
        mock_requests.get.assert_called_once_with(
            get_run_endpoint(HOST),
            json=None,
            params={'run_id': RUN_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_get_run_state(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE

        run_state = self.hook.get_run_state(RUN_ID)

        assert run_state == RunState(LIFE_CYCLE_STATE, RESULT_STATE, STATE_MESSAGE)
        mock_requests.get.assert_called_once_with(
            get_run_endpoint(HOST),
            json=None,
            params={'run_id': RUN_ID},
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_cancel_run(self, mock_requests):
        mock_requests.post.return_value.json.return_value = GET_RUN_RESPONSE

        self.hook.cancel_run(RUN_ID)

        mock_requests.post.assert_called_once_with(
            cancel_run_endpoint(HOST),
            json={'run_id': RUN_ID},
            params=None,
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_start_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        self.hook.start_cluster({"cluster_id": CLUSTER_ID})

        mock_requests.post.assert_called_once_with(
            start_cluster_endpoint(HOST),
            json={'cluster_id': CLUSTER_ID},
            params=None,
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_restart_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        self.hook.restart_cluster({"cluster_id": CLUSTER_ID})

        mock_requests.post.assert_called_once_with(
            restart_cluster_endpoint(HOST),
            json={'cluster_id': CLUSTER_ID},
            params=None,
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_terminate_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        self.hook.terminate_cluster({"cluster_id": CLUSTER_ID})

        mock_requests.post.assert_called_once_with(
            terminate_cluster_endpoint(HOST),
            json={'cluster_id': CLUSTER_ID},
            params=None,
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_install_libs_on_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        data = {'cluster_id': CLUSTER_ID, 'libraries': LIBRARIES}
        self.hook.install(data)

        mock_requests.post.assert_called_once_with(
            install_endpoint(HOST),
            json={'cluster_id': CLUSTER_ID, 'libraries': LIBRARIES},
            params=None,
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_uninstall_libs_on_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        data = {'cluster_id': CLUSTER_ID, 'libraries': LIBRARIES}
        self.hook.uninstall(data)

        mock_requests.post.assert_called_once_with(
            uninstall_endpoint(HOST),
            json={'cluster_id': CLUSTER_ID, 'libraries': LIBRARIES},
            params=None,
            auth=(LOGIN, PASSWORD),
            headers=USER_AGENT_HEADER,
            timeout=self.hook.timeout_seconds,
        )


class TestDatabricksHookToken(unittest.TestCase):
    """
    Tests for DatabricksHook when auth is done with token.
    """

    @provide_session
    def setUp(self, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.extra = json.dumps({'token': TOKEN, 'host': HOST})

        session.commit()

        self.hook = DatabricksHook()

    @mock.patch('airflow.providers.databricks.hooks.databricks.requests')
    def test_submit_run(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {'run_id': '1'}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        data = {'notebook_task': NOTEBOOK_TASK, 'new_cluster': NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        assert run_id == '1'
        args = mock_requests.post.call_args
        kwargs = args[1]
        assert kwargs['auth'].token == TOKEN


class TestDatabricksHookTokenWhenNoHostIsProvidedInExtra(TestDatabricksHookToken):
    @provide_session
    def setUp(self, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.extra = json.dumps({'token': TOKEN})

        session.commit()

        self.hook = DatabricksHook()


class TestRunState(unittest.TestCase):
    def test_is_terminal_true(self):
        terminal_states = ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']
        for state in terminal_states:
            run_state = RunState(state, '', '')
            assert run_state.is_terminal

    def test_is_terminal_false(self):
        non_terminal_states = ['PENDING', 'RUNNING', 'TERMINATING']
        for state in non_terminal_states:
            run_state = RunState(state, '', '')
            assert not run_state.is_terminal

    def test_is_terminal_with_nonexistent_life_cycle_state(self):
        run_state = RunState('blah', '', '')
        with pytest.raises(AirflowException):
            run_state.is_terminal

    def test_is_successful(self):
        run_state = RunState('TERMINATED', 'SUCCESS', '')
        assert run_state.is_successful
